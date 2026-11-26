"""Core backend for exact, evidence-backed product and sales commitments.

The service is intentionally framework-free.  It keeps JSON-serialisable state
and exposes semantic Python methods used by the HTTP adapter and tests.
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .errors import (
    AuthorizationError,
    ConflictError,
    ExchangeError,
    IdempotencyConflictError,
    NotFoundError,
    ValidationError,
)
from .timeutil import contains, iso, now_dt, overlap_window, parse_dt, resolve_at

SCHEMA_VERSION = 1

LISTING_FIELDS = {"store", "local", "pack", "barcode", "label", "from", "until"}
AMENDMENT_WRITABLE_FIELDS = {
    "store",
    "local",
    "pack",
    "barcode",
    "label",
    "from",
    "until",
}
TARGET_FIELDS = ("channel", "listing", "push")


def canonical_hash(record: dict[str, Any]) -> str:
    raw = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _deep_copy(value: Any) -> Any:
    return copy.deepcopy(value)


def _require_dict(value: Any, name: str = "record") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{name} must be a JSON object", details={"field": name})
    return value


def _require_nonempty_str(record: dict[str, Any], field: str) -> str:
    value = record.get(field)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValidationError(f"{field} is required", details={"field": field})
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Identifiers may be numeric in an external feed, but are compared as
        # exact strings; never infer equality from formatting/similarity.
        return str(value)
    if not isinstance(value, str):
        raise ValidationError(f"{field} must be a string or scalar identifier", details={"field": field})
    return value


def _optional_str(record: dict[str, Any], field: str) -> str | None:
    value = record.get(field)
    if value is None:
        return None
    return _require_nonempty_str(record, field)


def _positive_int(record: dict[str, Any], field: str, *, required: bool = True) -> int | None:
    if field not in record or record.get(field) is None:
        if required:
            raise ValidationError(f"{field} is required", details={"field": field})
        return None
    value = record[field]
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValidationError(f"{field} must be a positive integer", details={"field": field, "value": value})
    return value


def _material_package(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item.get("store"), item.get("local"), item.get("pack") or item.get("pieces"))


class ExchangeService:
    """Stateful exchange service.

    A single service is safe for concurrent use.  Persistence is optional via
    :meth:`save_state` / :meth:`load_state`; the HTTP server can autosave.
    """

    def __init__(self, state: dict[str, Any] | None = None) -> None:
        self._lock = threading.RLock()
        self.state: dict[str, Any] = state or self._empty_state()
        self._gateways: dict[str, Any] = {}
        if state is not None:
            self._validate_state(self.state)

    # ------------------------------------------------------------------ state
    @staticmethod
    def _empty_state() -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "seq": 0,
            "stores": [],
            "catalog": [],
            "listing_revisions": [],
            "amendment_batches": {},
            "amendment_order": [],
            "receipt_events": [],
            "receipt_arrivals": [],
            "receipt_hashes": {},
            "pauses": [],
            "claims": [],
            "decisions": [],
            "orders": {},
            "client_requests": {},
            "raw_hashes": {},
            "outbound_publications": [],
        }

    @staticmethod
    def _validate_state(state: dict[str, Any]) -> None:
        if not isinstance(state, dict):
            raise ValidationError("state must be an object")
        state.setdefault("schema_version", SCHEMA_VERSION)
        for key, default in ExchangeService._empty_state().items():
            state.setdefault(key, _deep_copy(default))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return _deep_copy(self.state)

    @classmethod
    def from_snapshot(cls, state: dict[str, Any]) -> "ExchangeService":
        return cls(_deep_copy(state))

    def save_state(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        with self._lock:
            payload = json.dumps(self.state, ensure_ascii=False, indent=2, sort_keys=False)
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(target)

    @classmethod
    def load_state(cls, path: str | Path) -> "ExchangeService":
        target = Path(path)
        if not target.exists() or target.stat().st_size == 0:
            service = cls()
            service.save_state(target)
            return service
        data = json.loads(target.read_text(encoding="utf-8"))
        return cls(data)

    def _next_id(self, prefix: str) -> str:
        self.state["seq"] += 1
        return f"{prefix}-{self.state['seq']}"

    def _remember_raw(self, kind: str, record: dict[str, Any]) -> tuple[str, bool]:
        digest = canonical_hash(record)
        key = f"{kind}:{digest}"
        if key in self.state["raw_hashes"]:
            return digest, True
        self.state["raw_hashes"][key] = True
        return digest, False

    # ------------------------------------------------------------- ingestion
    def ingest_ndjson(
        self,
        text: str,
        kind: str | None = None,
        *,
        default_kind: str | None = None,
    ) -> list[dict[str, Any]]:
        """Ingest NDJSON.

        A line may include ``kind``; otherwise ``default_kind``/``kind`` is
        used.  The sample files are named after their kind, so the CLI supplies
        it.
        """
        fallback_kind = default_kind or kind
        results: list[dict[str, Any]] = []
        with self._lock:
            for line_number, line in enumerate(text.splitlines(), start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    record_kind = record.pop("kind", fallback_kind) if isinstance(record, dict) else fallback_kind
                    if not record_kind:
                        raise ValidationError("NDJSON record requires kind or default_kind")
                    result = self.ingest_record(record_kind, record)
                    result["line_number"] = line_number
                    results.append(result)
                except ExchangeError as exc:
                    results.append(
                        {
                            "ok": False,
                            "line_number": line_number,
                            "error": exc.to_dict()["error"],
                        }
                    )
                    raise
        return results

    def ingest_file(self, path: str | Path, kind: str | None = None) -> list[dict[str, Any]]:
        p = Path(path)
        return self.ingest_ndjson(p.read_text(encoding="utf-8"), default_kind=kind or p.stem)

    def ingest_samples(self, directory: str | Path) -> dict[str, list[dict[str, Any]]]:
        """Load the six sample kinds in a business-friendly order."""
        directory = Path(directory)
        order = ["stores", "catalog", "listings", "amendments", "channel_receipts", "purchases"]
        output: dict[str, list[dict[str, Any]]] = {}
        for kind in order:
            path = directory / f"{kind}.ndjson"
            if path.exists():
                output[kind] = self.ingest_file(path, kind)
        return output

    # Common integration aliases.
    load_samples = ingest_samples
    load_file = ingest_file
    load_ndjson = ingest_ndjson

    def ingest(self, kind: str, records: dict[str, Any] | list[dict[str, Any]]) -> dict[str, Any] | list[dict[str, Any]]:
        """Ingest one record or a list of records of the same kind."""
        if isinstance(records, list):
            with self._lock:
                return [self.ingest_record(kind, record) for record in records]
        return self.ingest_record(kind, records)

    def ingest_record(self, kind: str, record: dict[str, Any]) -> dict[str, Any]:
        record = _require_dict(record)
        aliases = {
            "store": "stores",
            "stores": "stores",
            "operator": "stores",
            "catalog": "catalog",
            "item": "catalog",
            "listings": "listings",
            "listing": "listings",
            "amendments": "amendments",
            "amendment": "amendments",
            "channel_receipts": "channel_receipts",
            "receipts": "channel_receipts",
            "receipt": "channel_receipts",
            "purchases": "purchases",
            "purchase": "purchases",
            "order": "purchases",
        }
        normalized_kind = aliases.get(kind, kind)
        with self._lock:
            if normalized_kind == "stores":
                return self._ingest_store(record)
            if normalized_kind == "catalog":
                return self._ingest_catalog(record)
            if normalized_kind == "listings":
                return self._ingest_listing(record)
            if normalized_kind == "amendments":
                return self._ingest_amendment(record)
            if normalized_kind == "channel_receipts":
                result = self.ingest_receipt(record)
                return {"ok": True, "kind": normalized_kind, **result}
            if normalized_kind == "purchases":
                result = self.ingest_purchase(record)
                return {"ok": True, "kind": normalized_kind, **result}
            raise ValidationError(f"unknown record kind: {kind}", details={"kind": kind})

    # ------------------------------------------------------- operator tenure
    def _ingest_store(self, record: dict[str, Any]) -> dict[str, Any]:
        store = _require_nonempty_str(record, "store")
        operator = _require_nonempty_str(record, "operator")
        start = parse_dt(record.get("from"), field="from")
        until = parse_dt(record.get("until"), field="until", required=False)
        if until is not None and until <= start:  # type: ignore[operator]
            raise ValidationError("until must be after from", details={"store": store})
        digest, duplicate = self._remember_raw("stores", record)
        natural = next(
            (
                row
                for row in self.state["stores"]
                if row["store"] == store and row["operator"] == operator and row["from"] == record["from"]
            ),
            None,
        )
        if natural and canonical_hash(natural.get("raw", {})) != digest:
            raise IdempotencyConflictError(
                "store tenure with same business key has different payload",
                details={"store": store, "operator": operator, "from": record["from"]},
            )
        if duplicate or natural:
            return {"ok": True, "duplicate": True, "id": natural.get("evidence_id") if natural else digest, "kind": "stores"}
        envelope = {
            "evidence_id": self._next_id("op-tenure"),
            "evidence_hash": digest,
            "store": store,
            "operator": operator,
            "from": record["from"],
            "until": record.get("until"),
            "raw": _deep_copy(record),
        }
        self.state["stores"].append(envelope)
        self._reconcile_unresolved_orders(store=store)
        return {"ok": True, "duplicate": False, "id": envelope["evidence_id"], "record": _deep_copy(envelope)}

    def tenures(self, store: str, at: Any = None) -> list[dict[str, Any]]:
        at_dt = resolve_at(at)
        with self._lock:
            rows = [row for row in self.state["stores"] if row["store"] == store]
            result = []
            for row in rows:
                start = parse_dt(row["from"], field="from")
                until = parse_dt(row["until"], field="until", required=False)
                if contains(start, until, at_dt):
                    item = _deep_copy(row)
                    item["active_at"] = iso(at_dt)
                    result.append(item)
            return result

    def operator_at(self, store: str, at: Any = None) -> dict[str, Any] | None:
        active = self.tenures(store, at)
        if len(active) > 1:
            raise ConflictError(
                "overlapping operator tenures; responsible operator is ambiguous",
                code="operator_conflict",
                details={"store": store, "tenures": active},
            )
        return active[0] if active else None

    def _require_operator(self, operator_id: str | None, store: str, at: datetime) -> dict[str, Any]:
        if not operator_id:
            raise AuthorizationError("operator identity is required", details={"store": store})
        active = self.tenures(store, at)
        matches = [row for row in active if row["operator"] == operator_id]
        if not matches:
            current = None
            try:
                current = self.operator_at(store, at)
            except ConflictError:
                pass
            raise AuthorizationError(
                f"operator {operator_id} is not responsible for store {store} at {iso(at)}",
                details={
                    "store": store,
                    "operator": operator_id,
                    "at": iso(at),
                    "responsible_operators": [row["operator"] for row in active],
                    "current": current,
                },
            )
        return matches[0]

    def _require_operators_for_stores(
        self, operator_id: str | None, stores: Iterable[str], at: datetime
    ) -> list[dict[str, Any]]:
        return [self._require_operator(operator_id, store, at) for store in sorted(set(stores)) if store]

    # -------------------------------------------------------- catalog/report
    def _ingest_catalog(self, record: dict[str, Any]) -> dict[str, Any]:
        store = _require_nonempty_str(record, "store")
        local = _require_nonempty_str(record, "local")
        barcode = _require_nonempty_str(record, "barcode")
        pieces = _positive_int(record, "pieces")
        start = parse_dt(record.get("from"), field="from")
        until = parse_dt(record.get("until"), field="until", required=False)
        if until is not None and until <= start:  # type: ignore[operator]
            raise ValidationError("until must be after from", details={"store": store, "local": local})
        digest, duplicate = self._remember_raw("catalog", record)
        if duplicate:
            existing = next((x for x in self.state["catalog"] if x["evidence_hash"] == digest), None)
            return {"ok": True, "duplicate": True, "id": existing["evidence_id"] if existing else digest}
        envelope = {
            "evidence_id": self._next_id("catalog"),
            "evidence_hash": digest,
            "store": store,
            "local": local,
            "barcode": barcode,
            "label": record.get("label"),
            "pieces": pieces,
            "from": record["from"],
            "until": record.get("until"),
            "raw": _deep_copy(record),
        }
        self.state["catalog"].append(envelope)
        return {"ok": True, "duplicate": False, "id": envelope["evidence_id"], "record": _deep_copy(envelope)}

    def catalog_records(
        self,
        store: str,
        at: Any = None,
        *,
        local: str | None = None,
        barcode: str | None = None,
    ) -> list[dict[str, Any]]:
        at_dt = resolve_at(at)
        with self._lock:
            rows = []
            for row in self.state["catalog"]:
                if row["store"] != store:
                    continue
                if local is not None and row["local"] != local:
                    continue
                if barcode is not None and row["barcode"] != str(barcode):
                    continue
                start = parse_dt(row["from"], field="from")
                until = parse_dt(row["until"], field="until", required=False)
                if contains(start, until, at_dt):
                    item = _deep_copy(row)
                    item["active_at"] = iso(at_dt)
                    rows.append(item)
            return rows

    def resolve_catalog(
        self,
        store: str,
        at: Any = None,
        *,
        local: str | None = None,
        barcode: str | None = None,
    ) -> dict[str, Any]:
        """Resolve exact store identity at an instant.

        Barcode/name similarity is never used.  More than one materially
        different active record is ambiguous and must be adjudicated.
        """
        at_dt = resolve_at(at)
        rows = self.catalog_records(store, at_dt, local=local, barcode=barcode)
        distinct_specs: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            key = (row["local"], row["pieces"], row["barcode"])
            distinct_specs.setdefault(key, row)
        if not rows:
            status = "not_found"
            item = None
        elif len(distinct_specs) == 1:
            status = "exact"
            item = rows[0]
        else:
            status = "ambiguous"
            item = None
        return {
            "status": status,
            "store": store,
            "at": iso(at_dt),
            "filters": {"local": local, "barcode": barcode},
            "item": _deep_copy(item),
            "records": _deep_copy(rows),
            "reason": None if status == "exact" else "no_active_catalog_record" if not rows else "multiple_material_specs",
        }

    # -------------------------------------------------------------- listings
    def _ingest_listing(self, record: dict[str, Any]) -> dict[str, Any]:
        channel = _require_nonempty_str(record, "channel")
        listing = _require_nonempty_str(record, "listing")
        push = _require_nonempty_str(record, "push")
        local = _require_nonempty_str(record, "local")
        revision = _positive_int(record, "revision")
        pack = _positive_int(record, "pack", required=False)
        start = parse_dt(record.get("from"), field="from", required="from" in record)
        until = parse_dt(record.get("until"), field="until", required=False)
        if start is not None and until is not None and until <= start:
            raise ValidationError("until must be after from", details={"channel": channel, "listing": listing})
        source = _optional_str(record, "source") or record.get("source") or "default"
        if not isinstance(source, str):
            source = str(source)
        digest, duplicate = self._remember_raw("listings", record)
        biz_key = (source, channel, listing, revision)
        same_rev = next(
            (
                row
                for row in self.state["listing_revisions"]
                if (row.get("source", "default"), row["channel"], row["listing"], row["revision"]) == biz_key
            ),
            None,
        )
        if same_rev and same_rev["evidence_hash"] != digest:
            raise IdempotencyConflictError(
                "listing revision was already supplied with different payload",
                details={"channel": channel, "listing": listing, "revision": revision, "source": source},
            )
        if duplicate or same_rev:
            return {
                "ok": True,
                "duplicate": True,
                "id": same_rev["evidence_id"] if same_rev else digest,
                "kind": "listings",
            }
        envelope = {
            "evidence_id": self._next_id("listing-rev"),
            "evidence_hash": digest,
            "source": source,
            "channel": channel,
            "listing": listing,
            "push": push,
            "store": record.get("store"),
            "local": local,
            "pack": pack,
            "barcode": record.get("barcode"),
            "label": record.get("label"),
            "revision": revision,
            "from": record.get("from"),
            "until": record.get("until"),
            "ingest_seq": self.state["seq"],
            "raw": _deep_copy(record),
        }
        if envelope["store"] is not None:
            envelope["store"] = _require_nonempty_str(record, "store")
        self.state["listing_revisions"].append(envelope)
        self._reconcile_matching_orders(channel, listing, push)
        return {"ok": True, "duplicate": False, "id": envelope["evidence_id"], "record": _deep_copy(envelope)}

    def _ingest_amendment(self, record: dict[str, Any]) -> dict[str, Any]:
        batch = _require_nonempty_str(record, "batch")
        operator = _require_nonempty_str(record, "operator")
        changes = record.get("changes", [])
        clears = record.get("clear", [])
        if not isinstance(changes, list) or not isinstance(clears, list):
            raise ValidationError("changes and clear must be arrays", details={"batch": batch})
        for index, change in enumerate(changes):
            if not isinstance(change, dict):
                raise ValidationError("each change must be an object", details={"batch": batch, "index": index})
            for field in TARGET_FIELDS:
                _require_nonempty_str(change, field)
            if "from" in change:
                parse_dt(change.get("from"), field="change.from", required=False)
            if "until" in change:
                parse_dt(change.get("until"), field="change.until", required=False)
            if "pack" in change and change.get("pack") is not None:
                _positive_int(change, "pack", required=False)
        for index, clear in enumerate(clears):
            if not isinstance(clear, dict):
                raise ValidationError("each clear entry must be an object", details={"batch": batch, "index": index})
            for field in TARGET_FIELDS:
                _require_nonempty_str(clear, field)
            _require_nonempty_str(clear, "field")
        digest, duplicate_hash = self._remember_raw("amendments", record)
        existing = self.state["amendment_batches"].get(batch)
        if existing:
            if existing["evidence_hash"] != digest:
                raise IdempotencyConflictError(
                    "amendment batch id was already used with a different payload",
                    details={"batch": batch},
                )
            return {"ok": True, "duplicate": True, "id": existing["evidence_id"], "kind": "amendments"}
        envelope = {
            "evidence_id": self._next_id("amendment"),
            "evidence_hash": digest,
            "batch": batch,
            "operator": operator,
            "changes": _deep_copy(changes),
            "clear": _deep_copy(clears),
            "received_at": record.get("received_at"),
            "raw": _deep_copy(record),
        }
        self.state["amendment_batches"][batch] = envelope
        self.state["amendment_order"].append(batch)
        # Preserve the impact this whole batch had at its point in the ordered
        # correction stream.  A later explicit clear may remove the live
        # conflict, but the submitted batch still needs its audit/report record.
        envelope["impact"] = self._batch_impact(envelope)
        return {"ok": True, "duplicate": False, "id": envelope["evidence_id"], "record": _deep_copy(envelope)}

    def submit_amendment(self, record: dict[str, Any], *, at: Any = None) -> dict[str, Any]:
        """Ingest one whole correction batch and return affected conflicts."""
        with self._lock:
            result = self._ingest_amendment(record)
            if result.get("duplicate"):
                batch = self.state["amendment_batches"][record["batch"]]
            else:
                batch = result["record"]
            targets = self._batch_targets(batch)
            affected = []
            for target in targets:
                conflicts = [
                    c
                    for c in self.list_conflicts(at=None)
                    if c["type"] == "channel_listing_overlap"
                    and c["channel"] == target.get("channel")
                    and c["listing"] == target.get("listing")
                ]
                affected.append({"target": target, "conflicts": conflicts})
            unaffected = self._unaffected_sellable(targets, at)
            return {
                "ok": True,
                "duplicate": result.get("duplicate", False),
                "batch": batch["batch"],
                "targets": targets,
                "affected": affected,
                "batch_impact": batch.get("impact"),
                "unaffected_sellable": unaffected,
                "all_conflicts": self.list_conflicts(at=None),
            }

    def _batch_impact(self, batch: dict[str, Any]) -> dict[str, Any]:
        targets = self._batch_targets(batch)
        scopes = {(t["channel"], t["listing"]) for t in targets}
        conflicts = [
            c
            for c in self.list_conflicts(at=None)
            if c.get("type") == "channel_listing_overlap"
            and (c.get("channel"), c.get("listing")) in scopes
        ]
        return {
            "targets": targets,
            "conflict_ids": [c["conflict_id"] for c in conflicts],
            "affected_conflicts": conflicts,
            "affected_channels": [{"channel": ch, "listing": lst} for ch, lst in sorted(scopes)],
        }

    def _batch_targets(self, batch: dict[str, Any]) -> list[dict[str, Any]]:
        targets: dict[tuple[str, str, str], dict[str, Any]] = {}
        for change in batch.get("changes", []):
            if isinstance(change, dict) and all(k in change for k in TARGET_FIELDS):
                key = (str(change["channel"]), str(change["listing"]), str(change["push"]))
                targets[key] = {k: str(change[k]) for k in TARGET_FIELDS}
        for clear in batch.get("clear", []):
            if isinstance(clear, dict) and all(k in clear for k in TARGET_FIELDS):
                key = (str(clear["channel"]), str(clear["listing"]), str(clear["push"]))
                targets[key] = {k: str(clear[k]) for k in TARGET_FIELDS}
        return list(targets.values())

    def _unaffected_sellable(self, affected_targets: list[dict[str, Any]], at: Any) -> list[dict[str, Any]]:
        affected_scopes = {(t["channel"], t["listing"]) for t in affected_targets}
        preview = self.preview_channel(at=at)
        result = []
        for item in preview["sellable_now"] + preview["future_reservable"]:
            if (item["channel"], item["listing"]) not in affected_scopes:
                result.append({"channel": item["channel"], "listing": item["listing"], "push": item["push"], "status": item["status"]})
        return result

    # ------------------------------------------------- derived associations
    def _listing_base_for_key(self, key: tuple[str, str, str]) -> dict[str, Any] | None:
        channel, listing, push = key
        rows = [
            row
            for row in self.state["listing_revisions"]
            if row["channel"] == channel and row["listing"] == listing and row["push"] == push
        ]
        if not rows:
            return None
        # Revision is ordered only inside (source, channel, listing).  In the
        # ordinary single-source feed max revision is the current report.
        rows.sort(key=lambda r: (r.get("revision") or 0, r.get("ingest_seq") or 0))
        return _deep_copy(rows[-1])

    def derived_associations(self) -> dict[tuple[str, str, str], dict[str, Any]]:
        """Build current associations from listings then ordered amendments."""
        assoc: dict[tuple[str, str, str], dict[str, Any]] = {}

        def ensure(key: tuple[str, str, str]) -> dict[str, Any]:
            if key not in assoc:
                base = self._listing_base_for_key(key)
                if base:
                    item = {
                        "channel": key[0],
                        "listing": key[1],
                        "push": key[2],
                        "store": base.get("store"),
                        "local": base.get("local"),
                        "pack": base.get("pack"),
                        "barcode": base.get("barcode"),
                        "label": base.get("label"),
                        "from": base.get("from"),
                        "until": base.get("until"),
                        "sources": [base["evidence_id"]],
                        "amendment_batches": [],
                        "field_sources": {
                            field: base["evidence_id"]
                            for field in ("store", "local", "pack", "barcode", "label", "from", "until")
                            if base.get(field) is not None
                        },
                        "revision": base.get("revision"),
                        "source": base.get("source"),
                    }
                else:
                    item = {
                        "channel": key[0],
                        "listing": key[1],
                        "push": key[2],
                        "store": None,
                        "local": None,
                        "pack": None,
                        "barcode": None,
                        "label": None,
                        "from": None,
                        "until": None,
                        "sources": [],
                        "amendment_batches": [],
                        "field_sources": {},
                        "revision": None,
                        "source": None,
                    }
                assoc[key] = item
            return assoc[key]

        with self._lock:
            # Ensure all listing keys, including keys whose push differs from
            # the newest revision of a channel+listing.
            for row in self.state["listing_revisions"]:
                ensure((row["channel"], row["listing"], row["push"]))

            for batch_id in self.state["amendment_order"]:
                batch = self.state["amendment_batches"][batch_id]
                for change in batch.get("changes", []):
                    if not isinstance(change, dict) or not all(k in change for k in TARGET_FIELDS):
                        continue
                    key = tuple(str(change[k]) for k in TARGET_FIELDS)  # type: ignore[assignment]
                    item = ensure(key)
                    item["amendment_batches"].append(batch_id) if batch_id not in item["amendment_batches"] else None
                    item["sources"].append(batch["evidence_id"]) if batch["evidence_id"] not in item["sources"] else None
                    for field, value in change.items():
                        if field in TARGET_FIELDS:
                            continue
                        if field not in AMENDMENT_WRITABLE_FIELDS:
                            continue
                        item[field] = value
                        item["field_sources"][field] = batch_id
                for clear in batch.get("clear", []):
                    if not isinstance(clear, dict) or not all(k in clear for k in TARGET_FIELDS):
                        continue
                    field = clear.get("field")
                    key = tuple(str(clear[k]) for k in TARGET_FIELDS)  # type: ignore[assignment]
                    item = ensure(key)
                    item["amendment_batches"].append(batch_id) if batch_id not in item["amendment_batches"] else None
                    if field in AMENDMENT_WRITABLE_FIELDS:
                        # Explicit clear ends/removes that field.  Missing
                        # fields in changes never reach this branch.
                        item[field] = None
                        item.setdefault("cleared_fields", [])
                        if field not in item["cleared_fields"]:
                            item["cleared_fields"].append(field)
                        item["field_sources"][field] = f"{batch_id}:clear"
        return assoc

    def list_candidates(self, *, channel: str | None = None, listing: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            items = list(self.derived_associations().values())
        if channel:
            items = [x for x in items if x["channel"] == channel]
        if listing:
            items = [x for x in items if x["listing"] == listing]
        for item in items:
            item["record_kind"] = "candidate_association"
            item["curation_visibility"] = "candidate_or_confirmed_association"
        return _deep_copy(sorted(items, key=lambda x: (x["channel"], x["listing"], x["push"])))

    # ------------------------------------------------------------- receipts
    def ingest_receipt(self, record: dict[str, Any]) -> dict[str, Any]:
        channel = _require_nonempty_str(record, "channel")
        listing = _require_nonempty_str(record, "listing")
        push = _require_nonempty_str(record, "push")
        state = _require_nonempty_str(record, "state")
        digest = canonical_hash(record)
        with self._lock:
            received_at = record.get("received_at") or iso(now_dt())
            known_id = self.state["receipt_hashes"].get(digest)
            if known_id:
                event = next(e for e in self.state["receipt_events"] if e["receipt_id"] == known_id)
                event["arrival_count"] += 1
                event["last_received_at"] = received_at
                arrival = {
                    "arrival_id": self._next_id("receipt-arrival"),
                    "receipt_id": known_id,
                    "duplicate": True,
                    "channel": channel,
                    "listing": listing,
                    "push": push,
                    "state": state,
                    "received_at": received_at,
                    "raw": _deep_copy(record),
                }
                self.state["receipt_arrivals"].append(arrival)
                return {"duplicate": True, "receipt": _deep_copy(event), "arrival": _deep_copy(arrival)}
            receipt_id = self._next_id("receipt")
            event = {
                "receipt_id": receipt_id,
                "receipt_hash": digest,
                "channel": channel,
                "listing": listing,
                "push": push,
                "state": state,
                "received_at": received_at,
                "last_received_at": received_at,
                "channel_event_at": record.get("at") or record.get("event_at"),
                "arrival_count": 1,
                "raw": _deep_copy(record),
                "record_kind": "channel_publication",
            }
            self.state["receipt_events"].append(event)
            self.state["receipt_hashes"][digest] = receipt_id
            arrival = {
                "arrival_id": self._next_id("receipt-arrival"),
                "receipt_id": receipt_id,
                "duplicate": False,
                "channel": channel,
                "listing": listing,
                "push": push,
                "state": state,
                "received_at": received_at,
                "raw": _deep_copy(record),
            }
            self.state["receipt_arrivals"].append(arrival)
            # A receipt is evidence only.  It deliberately does not create an
            # association, resolve a conflict, or resume a pause.
            return {"duplicate": False, "receipt": _deep_copy(event), "arrival": _deep_copy(arrival)}

    def publications(self, *, channel: str | None = None, listing: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            events = _deep_copy(self.state["receipt_events"])
        if channel:
            events = [e for e in events if e["channel"] == channel]
        if listing:
            events = [e for e in events if e["listing"] == listing]
        for event in events:
            event["record_kind"] = "channel_publication"
        return events

    def receipt_arrivals(self, *, channel: str | None = None, listing: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = _deep_copy(self.state["receipt_arrivals"])
        if channel:
            rows = [r for r in rows if r["channel"] == channel]
        if listing:
            rows = [r for r in rows if r["listing"] == listing]
        return rows

    def latest_receipt(self, channel: str, listing: str, push: str) -> dict[str, Any] | None:
        events = [
            e
            for e in self.state["receipt_events"]
            if e["channel"] == channel and e["listing"] == listing and e["push"] == push
        ]
        return _deep_copy(events[-1]) if events else None

    # ----------------------------------------------------- channel writes
    def register_gateway(self, name: str, gateway: Any) -> None:
        """Register a repeatable channel adapter (live objects are not persisted)."""
        with self._lock:
            self._gateways[name] = gateway

    def publish_candidate(
        self,
        *,
        channel: str,
        listing: str,
        push: str | None = None,
        operator: str | None = None,
        at: Any = None,
        gateway_name: str | None = None,
        gateway: Any | None = None,
        allow_future: bool = True,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            associations = self.derived_associations()
            if push is None:
                matching = [a for a in associations.values() if a["channel"] == channel and a["listing"] == listing]
                if len(matching) == 1:
                    push = matching[0]["push"]
                else:
                    raise ValidationError(
                        "push is required when a channel listing has multiple publication versions",
                        details={"channel": channel, "listing": listing, "pushes": [a["push"] for a in matching]},
                    )
            key = (channel, listing, push)
            assoc = associations.get(key)
            if not assoc:
                raise NotFoundError(
                    "candidate association not found for exact channel/listing/push",
                    details={"channel": channel, "listing": listing, "push": push},
                )
            if assoc.get("store"):
                self._require_operator(operator, assoc["store"], at_dt)
            evaluated = self._evaluate_association(_deep_copy(assoc), at_dt)
            if evaluated["status"] == "under_review" or evaluated["status"] == "paused":
                raise ConflictError(
                    "refusing to publish a conflicted or paused candidate",
                    code="publication_blocked_by_review",
                    details={"preview": evaluated},
                )
            if evaluated["phase"] == "future" and not allow_future:
                raise ConflictError("candidate is only reservable in the future", details={"preview": evaluated})

            payload = {
                "channel": channel,
                "listing": listing,
                "push": push,
                "store": assoc.get("store"),
                "local": assoc.get("local"),
                "pack": assoc.get("pack"),
                "from": assoc.get("from"),
                "until": assoc.get("until"),
            }
            existing = next(
                (p for p in self.state["outbound_publications"] if (p["channel"], p["listing"], p["push"]) == key),
                None,
            )
            if existing:
                if canonical_hash(existing["payload"]) != canonical_hash(payload):
                    raise IdempotencyConflictError("push was already published with a different payload")
                return {"duplicate": True, "publication": _deep_copy(existing)}

            adapter = gateway or (self._gateways.get(gateway_name) if gateway_name else None)
            response: dict[str, Any] | None = None
            status = "requested"
            if adapter is not None and hasattr(adapter, "publish_listing"):
                response = adapter.publish_listing(_deep_copy(payload))
                status = str(response.get("status", "acknowledged")) if isinstance(response, dict) else "acknowledged"
            publication = {
                "publication_id": self._next_id("publication"),
                "record_kind": "outbound_channel_publication",
                "channel": channel,
                "listing": listing,
                "push": push,
                "status": status,
                "requested_by": operator,
                "requested_at": iso(at_dt),
                "payload": payload,
                "gateway": gateway_name or (adapter.__class__.__name__ if adapter else None),
                "response": response,
                "receipt_policy": "A later channel receipt remains separate evidence and never rewrites the candidate.",
            }
            self.state["outbound_publications"].append(publication)
            return {"duplicate": False, "publication": _deep_copy(publication)}

    def outbound_publications(self, *, channel: str | None = None, listing: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = _deep_copy(self.state["outbound_publications"])
        if channel:
            rows = [r for r in rows if r["channel"] == channel]
        if listing:
            rows = [r for r in rows if r["listing"] == listing]
        return rows

    def pull_channel(self, channel: str, *, gateway_name: str = "memory") -> dict[str, Any]:
        """Read listings through a registered repeatable channel adapter."""
        with self._lock:
            gateway = self._gateways.get(gateway_name)
            if gateway is None or not hasattr(gateway, "fetch_listings"):
                raise NotFoundError("channel gateway is not registered", details={"gateway": gateway_name})
            records = gateway.fetch_listings(channel)
            results = [self.ingest_record("listings", record) for record in records]
            return {"channel": channel, "results": results}


    # ------------------------------------------------------- sales switches
    def pause_sales(
        self,
        *,
        operator: str,
        channel: str,
        listing: str,
        push: str | None = None,
        reason: str = "",
        at: Any = None,
        until: Any = None,
        store: str | None = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        until_dt = parse_dt(until, field="until", required=False)
        if until_dt is not None and until_dt <= at_dt:
            raise ValidationError("pause until must be after its start")
        with self._lock:
            stores = {store} if store else self._scope_stores(channel, listing, push)
            self._require_operators_for_stores(operator, stores, at_dt)
            existing = self._active_pause(channel, listing, push, at_dt)
            if existing:
                return {"duplicate": True, "pause": _deep_copy(existing)}
            pause = {
                "pause_id": self._next_id("pause"),
                "scope": {"channel": channel, "listing": listing, "push": push},
                "store_ids": sorted(stores),
                "operator": operator,
                "reason": reason,
                "starts_at": iso(at_dt),
                "until": iso(until_dt),
                "status": "active",
                "created_at": iso(at_dt),
                "resumed_at": None,
                "resume_operator": None,
                "resume_note": None,
                "events": [{"type": "paused", "operator": operator, "at": iso(at_dt), "reason": reason}],
            }
            self.state["pauses"].append(pause)
            return {"duplicate": False, "pause": _deep_copy(pause)}

    def resume_sales(
        self,
        *,
        operator: str,
        pause_id: str | None = None,
        channel: str | None = None,
        listing: str | None = None,
        push: str | None = None,
        reason: str = "",
        at: Any = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            pause = None
            if pause_id:
                pause = next((p for p in self.state["pauses"] if p["pause_id"] == pause_id), None)
                if not pause:
                    raise NotFoundError(f"pause not found: {pause_id}", details={"pause_id": pause_id})
            elif channel and listing:
                pause = self._active_pause(channel, listing, push, at_dt)
                if not pause:
                    raise NotFoundError("active pause not found", details={"channel": channel, "listing": listing})
            else:
                raise ValidationError("pause_id or channel+listing is required")
            self._require_operators_for_stores(operator, pause.get("store_ids", []), at_dt)
            if pause["status"] != "active":
                raise ConflictError("pause is already ended", details={"pause_id": pause["pause_id"]})
            pause["status"] = "resumed"
            pause["resumed_at"] = iso(at_dt)
            pause["until"] = iso(at_dt)
            pause["resume_operator"] = operator
            pause["resume_note"] = reason
            pause["events"].append({"type": "resumed", "operator": operator, "at": iso(at_dt), "reason": reason})
            return {"duplicate": False, "pause": _deep_copy(pause)}

    def _scope_stores(self, channel: str, listing: str, push: str | None) -> set[str]:
        assocs = self.derived_associations()
        stores = set()
        for (ch, lst, psh), item in assocs.items():
            if ch == channel and lst == listing and (push is None or psh == push) and item.get("store"):
                stores.add(item["store"])
        if not stores:
            raise ValidationError(
                "cannot authorise pause because no store is linked to channel listing",
                details={"channel": channel, "listing": listing, "push": push},
            )
        return stores

    def _active_pause(
        self, channel: str, listing: str, push: str | None, at_dt: datetime
    ) -> dict[str, Any] | None:
        for pause in self.state["pauses"]:
            scope = pause["scope"]
            if scope["channel"] != channel or scope["listing"] != listing:
                continue
            scope_push = scope.get("push")
            if scope_push is not None and scope_push != push:
                # A push-specific pause does not match another push or a
                # listing-wide lookup without an explicit push.
                continue
            # scope_push is None: listing-wide pause applies to every push.
            start = parse_dt(pause["starts_at"], field="starts_at")
            until = parse_dt(pause.get("until"), field="until", required=False)
            if contains(start, until, at_dt):
                return pause
        return None

    def pause_for_association(self, assoc: dict[str, Any], at_dt: datetime) -> dict[str, Any] | None:
        with self._lock:
            return _deep_copy(
                self._active_pause(assoc["channel"], assoc["listing"], assoc["push"], at_dt)
            )

    # -------------------------------------------------------- conflicts etc
    def list_conflicts(self, *, at: Any = None) -> list[dict[str, Any]]:
        """List material overlapping packages and catalog identifier conflicts."""
        with self._lock:
            channel_conflicts = self._channel_conflicts_unlocked()
            catalog_conflicts = self._catalog_conflicts_unlocked()
            conflicts = channel_conflicts + catalog_conflicts
            for conflict in conflicts:
                conflict["claims"] = [
                    _deep_copy(c) for c in self.state["claims"] if c.get("conflict_id") == conflict["conflict_id"]
                ]
                decisions = [
                    _deep_copy(d) for d in self.state["decisions"] if d.get("conflict_id") == conflict["conflict_id"]
                ]
                conflict["decisions"] = decisions
                active_decision = self._decision_covers_conflict(decisions, conflict, at)
                conflict["active_decision"] = active_decision
                if active_decision:
                    conflict["status"] = "resolved_confirmed"
                elif conflict["claims"] and conflict["claims"][-1].get("status") == "claimed":
                    conflict["status"] = "claimed"
                else:
                    conflict["status"] = "open"
            return _deep_copy(conflicts)

    def _decision_covers_conflict(
        self, decisions: list[dict[str, Any]], conflict: dict[str, Any], at: Any
    ) -> dict[str, Any] | None:
        if not decisions:
            return None
        # Latest explicit decision wins for the material state.  History is
        # still fully present in ``decisions``.
        decision = decisions[-1]
        if at is None or not decision.get("window", {}).get("from"):
            return decision
        at_dt = resolve_at(at)
        start = parse_dt(decision["window"]["from"], field="decision.from")
        until = parse_dt(decision["window"].get("until"), field="decision.until", required=False)
        return decision if contains(start, until, at_dt) else None

    def _channel_conflicts_unlocked(self) -> list[dict[str, Any]]:
        assocs = self.derived_associations()
        scopes: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for item in assocs.values():
            if not item.get("from") or not item.get("local") or not item.get("store"):
                continue
            scopes.setdefault((item["channel"], item["listing"]), []).append(item)

        conflicts: list[dict[str, Any]] = []
        for (channel, listing), items in sorted(scopes.items()):
            windows: list[dict[str, Any]] = []
            candidates: dict[str, dict[str, Any]] = {}
            for i, a in enumerate(items):
                a0 = parse_dt(a["from"], field="from")
                a1 = parse_dt(a.get("until"), field="until", required=False)
                for b in items[i + 1 :]:
                    if not b.get("from") or not b.get("local") or not b.get("store"):
                        continue
                    b0 = parse_dt(b["from"], field="from")
                    b1 = parse_dt(b.get("until"), field="until", required=False)
                    ow = overlap_window(a0, a1, b0, b1)
                    if not ow:
                        continue
                    if _material_package(a) == _material_package(b):
                        continue
                    w_start, w_until = ow
                    windows.append({"from": iso(w_start), "until": iso(w_until)})
                    for cand in (a, b):
                        candidates[cand["push"]] = self._candidate_summary(cand, w_start)
            if not windows:
                continue
            stores = sorted({c["store"] for c in candidates.values() if c.get("store")})
            conflicts.append(
                {
                    "conflict_id": f"channel-listing:{channel}:{listing}",
                    "type": "channel_listing_overlap",
                    "channel": channel,
                    "listing": listing,
                    "channels": [{"channel": channel}],
                    "store_ids": stores,
                    "affected_goods": list(candidates.values()),
                    "candidates": list(candidates.values()),
                    "windows": windows,
                    "status": "open",
                }
            )
        return conflicts

    def _candidate_summary(self, assoc: dict[str, Any], at_dt: datetime | None = None) -> dict[str, Any]:
        barcode = assoc.get("barcode")
        catalog = None
        if assoc.get("store") and assoc.get("local") and at_dt is not None:
            resolved = self.resolve_catalog(assoc["store"], at_dt, local=assoc["local"])
            if resolved["status"] == "exact" and resolved["item"]:
                barcode = barcode or resolved["item"].get("barcode")
                catalog = {
                    "evidence_id": resolved["item"]["evidence_id"],
                    "pieces": resolved["item"]["pieces"],
                    "barcode": resolved["item"]["barcode"],
                    "label": resolved["item"].get("label"),
                }
        return {
            "channel": assoc["channel"],
            "listing": assoc["listing"],
            "push": assoc["push"],
            "store": assoc.get("store"),
            "local": assoc.get("local"),
            "pack": assoc.get("pack"),
            "barcode": barcode,
            "label": assoc.get("label"),
            "from": assoc.get("from"),
            "until": assoc.get("until"),
            "catalog_at_window_start": catalog,
            "amendment_batches": assoc.get("amendment_batches", []),
        }

    def _catalog_conflicts_unlocked(self) -> list[dict[str, Any]]:
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in self.state["catalog"]:
            groups.setdefault((row["store"], row["barcode"]), []).append(row)
        conflicts = []
        for (store, barcode), rows in sorted(groups.items()):
            windows = []
            goods = {}
            for i, a in enumerate(rows):
                a0 = parse_dt(a["from"], field="from")
                a1 = parse_dt(a.get("until"), field="until", required=False)
                for b in rows[i + 1 :]:
                    b0 = parse_dt(b["from"], field="from")
                    b1 = parse_dt(b.get("until"), field="until", required=False)
                    ow = overlap_window(a0, a1, b0, b1)
                    if not ow:
                        continue
                    if (a["local"], a["pieces"], a["barcode"]) == (b["local"], b["pieces"], b["barcode"]):
                        continue
                    start, until = ow
                    windows.append({"from": iso(start), "until": iso(until)})
                    for row in (a, b):
                        goods[row["evidence_id"]] = {
                            "evidence_id": row["evidence_id"],
                            "store": row["store"],
                            "local": row["local"],
                            "pieces": row["pieces"],
                            "barcode": row["barcode"],
                            "label": row.get("label"),
                            "from": row["from"],
                            "until": row.get("until"),
                        }
            if windows:
                conflicts.append(
                    {
                        "conflict_id": f"store-barcode:{store}:{barcode}",
                        "type": "store_barcode_overlap",
                        "store": store,
                        "barcode": barcode,
                        "store_ids": [store],
                        "channels": [],
                        "windows": windows,
                        "affected_goods": list(goods.values()),
                        "candidates": list(goods.values()),
                        "status": "open",
                    }
                )
        return conflicts

    def claim_conflict(
        self,
        conflict_id: str,
        *,
        operator: str,
        evidence_ids: list[str] | None = None,
        note: str = "",
        at: Any = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            conflict = next((c for c in self.list_conflicts() if c["conflict_id"] == conflict_id), None)
            if not conflict:
                raise NotFoundError(f"conflict not found: {conflict_id}", details={"conflict_id": conflict_id})
            self._require_operators_for_stores(operator, conflict.get("store_ids", []), at_dt)
            claim = {
                "claim_id": self._next_id("claim"),
                "conflict_id": conflict_id,
                "operator": operator,
                "status": "claimed",
                "evidence_ids": evidence_ids or [],
                "note": note,
                "at": iso(at_dt),
            }
            self.state["claims"].append(claim)
            return {"claim": _deep_copy(claim), "conflict": self.get_conflict(conflict_id, at=at_dt)}

    def get_conflict(self, conflict_id: str, *, at: Any = None) -> dict[str, Any]:
        for conflict in self.list_conflicts(at=at):
            if conflict["conflict_id"] == conflict_id:
                return conflict
        raise NotFoundError(f"conflict not found: {conflict_id}", details={"conflict_id": conflict_id})

    def confirm_association(
        self,
        conflict_id: str,
        *,
        operator: str,
        push: str | None = None,
        chosen: dict[str, Any] | None = None,
        window_from: Any = None,
        window_until: Any = None,
        note: str = "",
        evidence_ids: list[str] | None = None,
        at: Any = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            conflict = self.get_conflict(conflict_id)
            self._require_operators_for_stores(operator, conflict.get("store_ids", []), at_dt)
            choice: dict[str, Any]
            if chosen:
                choice = _deep_copy(chosen)
            elif push:
                candidate = next((c for c in conflict["candidates"] if c.get("push") == push), None)
                if not candidate:
                    raise ValidationError(
                        "push is not a candidate of this conflict",
                        details={"conflict_id": conflict_id, "push": push},
                    )
                choice = _deep_copy(candidate)
            else:
                raise ValidationError("push or chosen association is required")
            choice_start = parse_dt(choice.get("from"), field="choice.from", required=False)
            choice_until = parse_dt(choice.get("until"), field="choice.until", required=False)
            default_start = choice_start or at_dt
            default_until = choice_until
            conflict_windows = conflict.get("windows") or []
            if conflict_windows and window_from is None:
                overlap_starts = [parse_dt(w["from"], field="window.from") for w in conflict_windows]
                overlap_ends = [
                    parse_dt(w["until"], field="window.until", required=False)
                    for w in conflict_windows
                    if w.get("until")
                ]
                first_overlap_start = min(overlap_starts)
                if choice_start is not None:
                    default_start = max(choice_start, first_overlap_start)
                else:
                    default_start = first_overlap_start
                if overlap_ends:
                    overlap_end = min(overlap_ends)
                    default_until = min(x for x in (choice_until, overlap_end) if x is not None) if (choice_until or overlap_end) else None
            start = parse_dt(window_from, field="window_from", required=False) or default_start
            if window_until is not None:
                until = parse_dt(window_until, field="window_until", required=False)
            else:
                until = default_until
            # New operators must not backfill facts before their tenure begins.
            for store in conflict.get("store_ids", []):
                tenure = self._require_operator(operator, store, at_dt)
                tenure_start = parse_dt(tenure["from"], field="operator.from")
                if start < tenure_start:
                    raise AuthorizationError(
                        "cannot confirm an association before the operator's tenure starts",
                        code="backdated_operator_action",
                        details={"store": store, "tenure_from": iso(tenure_start), "requested_from": iso(start)},
                    )
            decision = {
                "decision_id": self._next_id("decision"),
                "conflict_id": conflict_id,
                "operator": operator,
                "chosen": choice,
                "window": {"from": iso(start), "until": iso(until)},
                "note": note,
                "evidence_ids": evidence_ids or [],
                "at": iso(at_dt),
            }
            self.state["decisions"].append(decision)
            return {"decision": _deep_copy(decision), "conflict": self.get_conflict(conflict_id, at=at_dt)}

    # -------------------------------------------------------------- preview
    def preview_channel(
        self,
        channel: str | None = None,
        listing: str | None = None,
        at: Any = None,
        *,
        when: Any = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(when if when is not None else at)
        with self._lock:
            associations = list(self.derived_associations().values())
            if channel:
                associations = [a for a in associations if a["channel"] == channel]
            if listing:
                associations = [a for a in associations if a["listing"] == listing]
            items = [self._evaluate_association(_deep_copy(a), at_dt) for a in associations]
        order = ["sellable_now", "reservable_future", "under_review", "paused", "unavailable", "ended"]
        buckets: dict[str, list[dict[str, Any]]] = {name: [] for name in order}
        for item in sorted(items, key=lambda x: (x["channel"], x["listing"], x.get("from") or "", x["push"])):
            buckets[item["status"]].append(item)
        return {
            "at": iso(at_dt),
            "channel": channel,
            "listing": listing,
            "items": items,
            "buckets": buckets,
            # Explicit aliases make the three product-facing states easy to
            # consume without hiding paused/unavailable states.
            "sellable_now": buckets["sellable_now"],
            "now_sellable": buckets["sellable_now"],
            "future_reservable": buckets["reservable_future"],
            "future": buckets["reservable_future"],
            "under_verification": buckets["under_review"],
            "under_review": buckets["under_review"],
            "checking": buckets["under_review"],
            "paused": buckets["paused"],
            "unavailable": buckets["unavailable"],
            "ended": buckets["ended"],
            "now_sellable_count": len(buckets["sellable_now"]),
            "future_count": len(buckets["reservable_future"]),
            "checking_count": len(buckets["under_review"]),
        }

    def _decision_at(
        self, channel: str, listing: str, assoc: dict[str, Any], at_dt: datetime
    ) -> tuple[dict[str, Any] | None, str | None]:
        scope_id = f"channel-listing:{channel}:{listing}"
        decisions = [d for d in self.state["decisions"] if d["conflict_id"] == scope_id]
        for decision in reversed(decisions):
            window = decision.get("window") or {}
            start = parse_dt(window.get("from") or decision["at"], field="decision.from")
            until = parse_dt(window.get("until"), field="decision.until", required=False)
            if not contains(start, until, at_dt):
                continue
            chosen = decision.get("chosen") or {}
            if chosen.get("push") == assoc["push"] or (
                chosen.get("local") == assoc.get("local")
                and chosen.get("store") == assoc.get("store")
                and chosen.get("pack") == assoc.get("pack")
            ):
                return _deep_copy(decision), "chosen"
            return _deep_copy(decision), "excluded"
        return None, None

    def _evaluate_association(self, assoc: dict[str, Any], at_dt: datetime) -> dict[str, Any]:
        reasons: list[str] = []
        blockers: list[str] = []
        start = parse_dt(assoc.get("from"), field="from", required=False)
        until = parse_dt(assoc.get("until"), field="until", required=False)
        if start is None:
            phase = "incomplete"
            blockers.append("missing_effective_from")
        elif at_dt < start:
            phase = "future"
        elif contains(start, until, at_dt):
            phase = "current"
        else:
            phase = "past"

        pause = self.pause_for_association(assoc, at_dt)
        decision_time = start if phase == "future" and start is not None else at_dt
        decision, decision_role = self._decision_at(
            assoc["channel"], assoc["listing"], assoc, decision_time
        )

        # Raw overlap check.  A confirmed decision can adjudicate it.
        overlap_conflicts = []
        for conflict in self._channel_conflicts_unlocked():
            if conflict["channel"] != assoc["channel"] or conflict["listing"] != assoc["listing"]:
                continue
            if not any(c.get("push") == assoc["push"] for c in conflict["candidates"]):
                continue
            relevant = True
            if start is not None:
                # Any overlap in this association's own window matters for a
                # future reservation; at query instant only current overlap.
                if phase == "current":
                    relevant = any(
                        contains(parse_dt(w["from"], field="w.from"), parse_dt(w.get("until"), field="w.until", required=False), at_dt)
                        for w in conflict["windows"]
                    )
                elif phase == "past":
                    relevant = False
            if relevant:
                overlap_conflicts.append(conflict)

        if decision_role == "excluded":
            reasons.append("excluded_by_operator_decision")
            return self._preview_item(
                assoc,
                at_dt,
                phase,
                "unavailable",
                reasons,
                blockers,
                pause=None,
                decision=decision,
                conflicts=overlap_conflicts,
                catalog=None,
                receipt=None,
            )

        if overlap_conflicts and decision_role != "chosen":
            blockers.append("multiple_effective_packages")

        if assoc.get("cleared_fields") and "local" in assoc["cleared_fields"]:
            reasons.append("local_relationship_explicitly_cleared")
            return self._preview_item(
                assoc, at_dt, phase, "unavailable", reasons, blockers, pause, decision, overlap_conflicts, None, None
            )

        required = ("store", "local", "from", "pack")
        missing = [field for field in required if assoc.get(field) in (None, "")]
        if missing:
            blockers.append("incomplete_association:" + ",".join(missing))

        # Validate the exact store identity at the time it would be used.
        catalog = None
        if not missing and assoc.get("store") and assoc.get("local") and start is not None:
            check_time = start if phase == "future" else at_dt if phase == "current" else start
            resolved = self.resolve_catalog(assoc["store"], check_time, local=assoc["local"])
            if resolved["status"] == "exact":
                catalog = resolved["item"]
                if assoc.get("pack") is not None and catalog.get("pieces") != assoc.get("pack"):
                    blockers.append(
                        f"pack_pieces_mismatch:listing={assoc['pack']},catalog={catalog.get('pieces')}"
                    )
            elif resolved["status"] == "ambiguous":
                blockers.append("catalog_identity_ambiguous")
            else:
                blockers.append("catalog_identity_not_effective")

        receipt = self.latest_receipt(assoc["channel"], assoc["listing"], assoc["push"])
        late_order_after_until = False
        if phase == "past" and until is not None:
            for order in self.state["orders"].values():
                if (
                    order.get("channel") == assoc["channel"]
                    and order.get("listing") == assoc["listing"]
                    and order.get("push") == assoc["push"]
                ):
                    ordered_at = parse_dt(order.get("ordered_at"), field="ordered_at", required=False)
                    if ordered_at is not None and ordered_at >= until:
                        late_order_after_until = True
                        break

        if pause:
            status = "paused"
            reasons.append("sales_paused_by_responsible_operator")
        elif phase == "past":
            visible_receipt_after_window = bool(receipt and receipt.get("state") == "visible")
            if visible_receipt_after_window or late_order_after_until:
                # The nominal window is over, but the channel may still display
                # the old push or a late order may prove the customer saw it.
                # Do not declare it normally ended/sellable; reconcile the
                # stale channel state while preserving exact order semantics.
                status = "under_review"
                reasons.append("stale_channel_publication_after_effective_until")
                if visible_receipt_after_window:
                    reasons.append("visible_receipt_still_present")
                if late_order_after_until:
                    reasons.append("late_purchase_references_old_push")
            else:
                status = "ended"
                reasons.append("outside_half_open_effective_window")
        elif blockers:
            status = "under_review"
            reasons.extend(blockers)
        elif phase == "future":
            status = "reservable_future"
            reasons.append("effective_in_future")
        elif phase == "current":
            if not receipt:
                status = "under_review"
                reasons.append("channel_publication_receipt_missing")
            elif receipt.get("state") != "visible":
                if receipt.get("state") in ("paused", "hidden", "removed", "withdrawn"):
                    status = "unavailable"
                    reasons.append(f"channel_receipt_state:{receipt.get('state')}")
                else:
                    status = "under_review"
                    reasons.append(f"channel_receipt_state:{receipt.get('state')}")
            else:
                status = "sellable_now"
                reasons.append("exact_association_catalog_and_visible_publication")
        else:
            status = "under_review"

        return self._preview_item(
            assoc, at_dt, phase, status, reasons, blockers, pause, decision, overlap_conflicts, catalog, receipt
        )

    def _preview_item(
        self,
        assoc: dict[str, Any],
        at_dt: datetime,
        phase: str,
        status: str,
        reasons: list[str],
        blockers: list[str],
        pause: dict[str, Any] | None,
        decision: dict[str, Any] | None,
        conflicts: list[dict[str, Any]],
        catalog: dict[str, Any] | None,
        receipt: dict[str, Any] | None,
    ) -> dict[str, Any]:
        sellable = status == "sellable_now"
        view_state = {
            "sellable_now": "now_sellable",
            "reservable_future": "future_reservable",
            "under_review": "checking",
        }.get(status, status)
        return {
            "record_kind": "channel_listing_preview",
            "status": status,
            "state": status,
            "view_state": view_state,
            "preview_state": view_state,
            "phase": phase,
            "sellable": sellable,
            "salable": sellable,
            "reservable": status == "reservable_future",
            "needs_review": status == "under_review",
            "at": iso(at_dt),
            "channel": assoc["channel"],
            "listing": assoc["listing"],
            "push": assoc["push"],
            "effective": {"from": assoc.get("from"), "until": assoc.get("until")},
            "commitment": {
                "store": assoc.get("store"),
                "local": assoc.get("local"),
                "pack": assoc.get("pack"),
                "barcode": (catalog or {}).get("barcode") or assoc.get("barcode"),
                "label": (catalog or {}).get("label") or assoc.get("label"),
                "pieces": (catalog or {}).get("pieces"),
            },
            "association": {
                "record_kind": "candidate_association",
                "store": assoc.get("store"),
                "local": assoc.get("local"),
                "pack": assoc.get("pack"),
                "revision": assoc.get("revision"),
                "source": assoc.get("source"),
                "amendment_batches": assoc.get("amendment_batches", []),
                "field_sources": assoc.get("field_sources", {}),
                "cleared_fields": assoc.get("cleared_fields", []),
            },
            "publication": receipt
            and {
                "record_kind": "channel_publication",
                "receipt_id": receipt["receipt_id"],
                "state": receipt["state"],
                "received_at": receipt["received_at"],
                "arrival_count": receipt.get("arrival_count", 1),
            }
            or None,
            "pause": pause and {"pause_id": pause["pause_id"], "scope": pause["scope"], "reason": pause.get("reason")},
            "decision": decision and {"decision_id": decision["decision_id"], "chosen": decision.get("chosen")},
            "conflicts": [
                {"conflict_id": c["conflict_id"], "type": c["type"], "windows": c["windows"]} for c in conflicts
            ],
            "catalog_evidence": catalog
            and {
                "evidence_id": catalog["evidence_id"],
                "pieces": catalog["pieces"],
                "barcode": catalog["barcode"],
                "label": catalog.get("label"),
            }
            or None,
            "reason_codes": reasons,
            "blockers": blockers,
        }

    # -------------------------------------------------------------- explain
    def explain(
        self,
        *,
        store: str | None = None,
        barcode: str | int | None = None,
        local: str | None = None,
        channel: str | None = None,
        listing: str | None = None,
        push: str | None = None,
        at: Any = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            result: dict[str, Any] = {
                "query": {
                    "store": store,
                    "barcode": None if barcode is None else str(barcode),
                    "local": local,
                    "channel": channel,
                    "listing": listing,
                    "push": push,
                    "at": iso(at_dt),
                },
                "identity_policy": "exact store/local/channel/listing/push identifiers; no name or barcode similarity inference",
            }
            if store:
                result["operator"] = self.operator_at(store, at_dt)
                result["catalog_resolution"] = self.resolve_catalog(
                    store, at_dt, local=local, barcode=None if barcode is None else str(barcode)
                )
            if channel and listing:
                preview = self.preview_channel(channel, listing=listing, at=at_dt)
                result["channel_preview"] = preview
                if push:
                    assoc = self.derived_associations().get((channel, listing, push))
                    result["channel_commitment"] = _deep_copy(assoc)
                    matching = [i for i in preview["items"] if i["push"] == push]
                    result["push_preview"] = matching[0] if matching else None
                active = [i for i in preview["items"] if i["phase"] == "current"]
                sellable = [i for i in active if i["status"] == "sellable_now"]
                chosen = active[0] if len(active) == 1 else sellable[0] if len(sellable) == 1 else None
                if push and result.get("push_preview"):
                    chosen = result["push_preview"]
                result["sold_as"] = self._sold_as_from_preview(chosen) if chosen and chosen.get("commitment", {}).get("local") else None
            elif store and (barcode is not None or local is not None):
                resolution = result["catalog_resolution"]
                result["sold_as"] = self._sold_as_from_catalog(resolution.get("item")) if resolution["status"] == "exact" else None
            else:
                result["sold_as"] = None

            result["answer"] = self._human_answer(result)
            return _deep_copy(result)

    def _sold_as_from_catalog(self, item: dict[str, Any] | None) -> dict[str, Any] | None:
        if not item:
            return None
        return {
            "kind": "store_catalog_package",
            "store": item["store"],
            "local": item["local"],
            "barcode": item["barcode"],
            "label": item.get("label"),
            "pieces_per_sales_unit": item["pieces"],
            "source_evidence_id": item["evidence_id"],
        }

    def _sold_as_from_preview(self, item: dict[str, Any] | None) -> dict[str, Any] | None:
        if not item:
            return None
        c = item.get("commitment") or {}
        return {
            "kind": "channel_commitment",
            "channel": item["channel"],
            "listing": item["listing"],
            "push": item["push"],
            "store": c.get("store"),
            "local": c.get("local"),
            "barcode": c.get("barcode"),
            "label": c.get("label"),
            "promised_pack_pieces": c.get("pack"),
            "catalog_pieces": c.get("pieces"),
            "preview_status": item["status"],
            "source": item.get("association", {}).get("field_sources"),
        }

    def _human_answer(self, result: dict[str, Any]) -> str:
        sold = result.get("sold_as")
        q = result["query"]
        if sold and sold.get("kind") == "channel_commitment":
            return (
                f"在 {q['at']}，渠道 {sold['channel']} 商品 {sold['listing']} 发布号 {sold['push']} "
                f"对应门店 {sold['store']} 的身份 {sold['local']}（条码 {sold.get('barcode')}），"
                f"外部承诺包装为 {sold['promised_pack_pieces']} 件/份；预览状态 {sold['preview_status']}。"
                "条码和商品名仅作证据，不用于相似推断。"
            )
        if sold and sold.get("kind") == "store_catalog_package":
            return (
                f"在 {q['at']}，门店 {sold['store']} 条码 {sold['barcode']} 唯一对应身份 {sold['local']}，"
                f"{sold['pieces_per_sales_unit']} 件/销售单位。"
            )
        catalog = result.get("catalog_resolution")
        if catalog and catalog["status"] == "ambiguous":
            return "同一门店和时间存在两种以上有效包装；已列为核对冲突，须由负责运营人员确认或暂停，不能按名称/条码相似性自动选择。"
        preview = result.get("channel_preview")
        if preview and preview.get("checking_count"):
            return "该渠道商品存在正在核对的关联或发布证据；当前不能放心更新为可售。"
        return "现有精确标识不足以唯一解释该时间点的商品；需要补充证据或由负责人确认。"

    # ======================================================================
    # Orders, accepted commitments, substitution and delivery
    # ======================================================================
    def ingest_purchase(self, record: dict[str, Any], *, received_at: Any = None) -> dict[str, Any]:
        record = _require_dict(record, "purchase")
        order_id = _require_nonempty_str(record, "order")
        customer = _require_nonempty_str(record, "customer")
        channel = _require_nonempty_str(record, "channel")
        listing = _require_nonempty_str(record, "listing")
        push = _require_nonempty_str(record, "push")
        units = _positive_int(record, "units")
        ordered_at = parse_dt(record.get("ordered_at"), field="ordered_at")
        digest = canonical_hash(record)
        received_dt = parse_dt(received_at, field="received_at", required=False) or now_dt()

        with self._lock:
            existing = self.state["orders"].get(order_id)
            if existing:
                immutable_fields = ("customer", "channel", "listing", "push", "units", "ordered_at")
                changed = [f for f in immutable_fields if existing.get(f) != record.get(f)]
                if changed:
                    raise IdempotencyConflictError(
                        "order id already exists with different accepted commitment",
                        details={"order": order_id, "changed_fields": changed},
                    )
                existing["duplicate_delivery_count"] = existing.get("duplicate_delivery_count", 0) + 1
                return {"duplicate": True, "order": _deep_copy(existing)}

            order = self._build_order_snapshot(
                record=record,
                digest=digest,
                order_id=order_id,
                customer=customer,
                channel=channel,
                listing=listing,
                push=push,
                units=units,  # type: ignore[arg-type]
                ordered_at=ordered_at,
                received_dt=received_dt,
            )
            self.state["orders"][order_id] = order
            return {"duplicate": False, "order": _deep_copy(order)}

    def _build_order_snapshot(
        self,
        *,
        record: dict[str, Any],
        digest: str,
        order_id: str,
        customer: str,
        channel: str,
        listing: str,
        push: str,
        units: int,
        ordered_at: datetime,
        received_dt: datetime,
    ) -> dict[str, Any]:
        associations = self.derived_associations()
        assoc = associations.get((channel, listing, push))
        raw_listing = self._listing_base_for_key((channel, listing, push))

        # The push is the exact promise the customer saw.  Prefer the corrected
        # association when complete, but retain the original channel listing and
        # never resolve by current barcode/name similarity.
        source_values = raw_listing or {}
        chosen: dict[str, Any] = {
            "store": source_values.get("store"),
            "local": source_values.get("local"),
            "pack": source_values.get("pack"),
            "barcode": source_values.get("barcode"),
            "label": source_values.get("label"),
            "source": "listing_revision",
            "listing_evidence_id": source_values.get("evidence_id"),
            "revision": source_values.get("revision"),
        }
        association_snapshot = None
        if assoc:
            association_snapshot = {
                k: _deep_copy(assoc.get(k))
                for k in (
                    "store",
                    "local",
                    "pack",
                    "barcode",
                    "label",
                    "from",
                    "until",
                    "sources",
                    "amendment_batches",
                    "field_sources",
                    "cleared_fields",
                    "revision",
                )
            }
            if assoc.get("local") and assoc.get("store") and assoc.get("pack") is not None:
                chosen.update(
                    {
                        "store": assoc.get("store"),
                        "local": assoc.get("local"),
                        "pack": assoc.get("pack"),
                        "barcode": assoc.get("barcode") or chosen.get("barcode"),
                        "label": assoc.get("label") or chosen.get("label"),
                        "source": "listing_plus_amendments",
                    }
                )

        store = chosen.get("store") or record.get("store")
        if store is not None:
            store = str(store)
        if store and not chosen.get("store"):
            chosen["store"] = store

        warnings: list[str] = []
        commitment: dict[str, Any] | None = None
        status = "accepted"
        fulfillment_status = "pending"
        managing_operator = None
        if store:
            tenure = self.operator_at(store, ordered_at)
            if tenure:
                managing_operator = tenure["operator"]
            else:
                warnings.append("operator_tenure_missing_at_order_time")

        if store and chosen.get("local") and chosen.get("pack") is not None:
            pack = int(chosen["pack"])
            commitment = {
                "channel": channel,
                "listing": listing,
                "push": push,
                "store": store,
                "local": str(chosen["local"]),
                "barcode": None if chosen.get("barcode") is None else str(chosen["barcode"]),
                "label": chosen.get("label"),
                "promised_pack_pieces": pack,
                "units": units,
                "total_pieces": units * pack,
                "quantity_meaning": f"{units} 份外部包装 × {pack} 件/份 = {units * pack} 件",
                "source": chosen.get("source"),
                "listing_evidence_id": chosen.get("listing_evidence_id"),
                "revision": chosen.get("revision"),
                "association": association_snapshot,
                "raw_listing": raw_listing and {
                    "evidence_id": raw_listing.get("evidence_id"),
                    "local": raw_listing.get("local"),
                    "pack": raw_listing.get("pack"),
                    "revision": raw_listing.get("revision"),
                    "from": raw_listing.get("from"),
                    "until": raw_listing.get("until"),
                },
                "accepted_at": iso(received_dt),
                "ordered_at": iso(ordered_at),
            }
            catalog_then = self.resolve_catalog(store, ordered_at, local=commitment["local"])
            if catalog_then["status"] == "exact" and catalog_then["item"]:
                item = catalog_then["item"]
                commitment["catalog_at_order_time"] = {
                    "evidence_id": item["evidence_id"],
                    "barcode": item["barcode"],
                    "pieces": item["pieces"],
                    "label": item.get("label"),
                }
                if commitment["barcode"] is None:
                    commitment["barcode"] = item["barcode"]
                if item["pieces"] != pack:
                    warnings.append(f"pack_catalog_mismatch:promised={pack},catalog={item['pieces']}")
            else:
                warnings.append(f"catalog_at_order_time_{catalog_then['status']}")
                if commitment.get("barcode") is None:
                    identity_rows = [
                        row
                        for row in self.state["catalog"]
                        if row["store"] == store and row["local"] == commitment["local"]
                    ]
                    if identity_rows:
                        identity_row = _deep_copy(identity_rows[-1])
                        commitment["barcode"] = identity_row["barcode"]
                        commitment["catalog_identity_evidence"] = {
                            "evidence_id": identity_row["evidence_id"],
                            "barcode": identity_row["barcode"],
                            "pieces": identity_row["pieces"],
                            "label": identity_row.get("label"),
                            "from": identity_row["from"],
                            "until": identity_row.get("until"),
                        }

            # Explain current barcode mapping so same-barcode/new-spec traps
            # are visible immediately, without changing the accepted snapshot.
            if commitment.get("barcode"):
                current = self.resolve_catalog(store, received_dt, barcode=commitment["barcode"])
                if current["status"] == "exact" and current["item"]:
                    current_local = current["item"]["local"]
                    if current_local != commitment["local"] or current["item"]["pieces"] != pack:
                        warnings.append("barcode_now_maps_to_different_spec")
                        commitment["current_barcode_resolution"] = {
                            "local": current_local,
                            "pieces": current["item"]["pieces"],
                            "evidence_id": current["item"]["evidence_id"],
                        }
                elif current["status"] == "ambiguous":
                    warnings.append("barcode_currently_ambiguous")

            if raw_listing and raw_listing.get("until"):
                until = parse_dt(raw_listing["until"], field="listing.until", required=False)
                if until and ordered_at >= until:
                    warnings.append("stale_channel_push_ordered_after_nominal_window")
            receipt = self.latest_receipt(channel, listing, push)
            if not receipt:
                warnings.append("publication_receipt_not_yet_available")
            elif receipt.get("state") != "visible":
                warnings.append(f"publication_receipt_state_{receipt.get('state')}")
        else:
            status = "unresolved_commitment"
            fulfillment_status = "unresolved"
            warnings.append("exact_channel_push_commitment_not_available")

        order = {
            "order": order_id,
            "customer": customer,
            "channel": channel,
            "listing": listing,
            "push": push,
            "units": units,
            "ordered_at": record["ordered_at"],
            "received_at": iso(received_dt),
            "raw_hash": digest,
            "status": status,
            "fulfillment_status": fulfillment_status,
            "store": store if commitment else None,
            "managing_operator_id": managing_operator,
            "accepted_commitment": commitment,
            "warnings": warnings,
            "agreed_resolution": None,
            "proposals": [],
            "picking": None,
            "delivery": None,
            "mismatch_reports": [],
            "events": [
                {
                    "event_id": self._next_id("order-event"),
                    "type": "purchase_ingested",
                    "at": iso(received_dt),
                    "ordered_at": record["ordered_at"],
                    "status": status,
                    "fulfillment_status": fulfillment_status,
                }
            ],
            "duplicate_delivery_count": 0,
        }
        return order

    def get_order(self, order_id: str) -> dict[str, Any]:
        with self._lock:
            order = self.state["orders"].get(order_id)
            if not order:
                raise NotFoundError(f"order not found: {order_id}", details={"order": order_id})
            return _deep_copy(order)

    def list_orders(
        self,
        *,
        store: str | None = None,
        operator: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            orders = list(self.state["orders"].values())
        if store:
            orders = [o for o in orders if o.get("store") == store]
        if operator:
            orders = [o for o in orders if o.get("managing_operator_id") == operator]
        if status:
            orders = [o for o in orders if o.get("status") == status or o.get("fulfillment_status") == status]
        return _deep_copy(sorted(orders, key=lambda o: o["order"]))

    def _reconcile_matching_orders(self, channel: str, listing: str, push: str) -> None:
        for order in self.state["orders"].values():
            if (
                order.get("status") == "unresolved_commitment"
                and order.get("channel") == channel
                and order.get("listing") == listing
                and order.get("push") == push
            ):
                self._reconcile_order_object(order)

    def _reconcile_unresolved_orders(self, *, store: str | None = None) -> None:
        for order in self.state["orders"].values():
            if order.get("status") != "unresolved_commitment":
                continue
            if store is not None and order.get("store") not in (None, store):
                continue
            self._reconcile_order_object(order)

    def reconcile_order(self, order_id: str) -> dict[str, Any]:
        """Re-run exact source resolution after late evidence arrives."""
        with self._lock:
            order = self._mutable_order(order_id)
            self._reconcile_order_object(order)
            return _deep_copy(order)

    def _reconcile_order_object(self, order: dict[str, Any]) -> None:
        record = {
            "order": order["order"],
            "customer": order["customer"],
            "channel": order["channel"],
            "listing": order["listing"],
            "push": order["push"],
            "units": order["units"],
            "ordered_at": order["ordered_at"],
        }
        received_dt = parse_dt(order.get("received_at"), field="received_at", required=False) or now_dt()
        fresh = self._build_order_snapshot(
            record=record,
            digest=order.get("raw_hash", canonical_hash(record)),
            order_id=order["order"],
            customer=order["customer"],
            channel=order["channel"],
            listing=order["listing"],
            push=order["push"],
            units=order["units"],
            ordered_at=parse_dt(order["ordered_at"], field="ordered_at"),
            received_dt=received_dt,
        )
        if not fresh.get("accepted_commitment"):
            return
        order["status"] = fresh["status"]
        order["fulfillment_status"] = fresh["fulfillment_status"]
        order["store"] = fresh["store"]
        order["managing_operator_id"] = fresh["managing_operator_id"]
        order["accepted_commitment"] = fresh["accepted_commitment"]
        order["warnings"] = fresh["warnings"]
        self._append_order_event(
            order,
            "commitment_reconciled_from_late_evidence",
            source=fresh["accepted_commitment"].get("source"),
        )

    def trace_order(self, order_id: str) -> dict[str, Any]:
        order = self.get_order(order_id)
        commitment = order.get("accepted_commitment") or {}
        source_explanation = None
        if commitment:
            source_explanation = self.explain(
                store=commitment.get("store"),
                channel=order["channel"],
                listing=order["listing"],
                push=order["push"],
                at=order["ordered_at"],
            )
        return {
            "order_id": order_id,
            "accepted_order": order,
            "immutable_accepted_commitment": commitment,
            "quantity_meaning_at_acceptance": commitment.get("quantity_meaning"),
            "managing_operator_id": order.get("managing_operator_id"),
            "source_chain_at_ordered_at": source_explanation,
            "current_agreed_resolution": order.get("agreed_resolution"),
            "timeline": order.get("events", []),
            "trace_policy": "The accepted snapshot is never overwritten by later barcode, catalog, amendment or operator changes.",
        }

    def _mutable_order(self, order_id: str) -> dict[str, Any]:
        order = self.state["orders"].get(order_id)
        if not order:
            raise NotFoundError(f"order not found: {order_id}", details={"order": order_id})
        return order

    def _require_order_operator(self, order: dict[str, Any], operator: str | None) -> str:
        expected = order.get("managing_operator_id")
        if not operator:
            raise AuthorizationError("operator identity is required for order operations")
        if not expected:
            raise AuthorizationError(
                "order has no unambiguous responsible operator; adjudicate operator tenure first",
                details={"order": order["order"]},
            )
        if operator != expected:
            raise AuthorizationError(
                "only the operator responsible when the order was accepted may dispose of the original order",
                code="order_operator_transition_forbidden",
                details={"order": order["order"], "expected_operator": expected, "provided_operator": operator},
            )
        return expected

    def _append_order_event(self, order: dict[str, Any], event_type: str, **fields: Any) -> dict[str, Any]:
        event = {"event_id": self._next_id("order-event"), "type": event_type, "at": iso(now_dt()), **fields}
        order.setdefault("events", []).append(event)
        return event

    def _required_fulfillment_spec(self, order: dict[str, Any]) -> dict[str, Any]:
        resolution = order.get("agreed_resolution")
        if resolution and resolution.get("type") == "replacement":
            return {
                "source": "customer_confirmed_replacement",
                "local": resolution["local"],
                "pack": resolution["pack"],
                "units": resolution["units"],
                "proposal_id": resolution["proposal_id"],
            }
        commitment = order.get("accepted_commitment")
        if not commitment:
            raise ConflictError("order commitment is unresolved", code="unresolved_commitment")
        return {
            "source": "accepted_commitment",
            "local": commitment["local"],
            "pack": commitment["promised_pack_pieces"],
            "units": commitment["units"],
        }

    def record_picking(
        self,
        order_id: str,
        *,
        operator: str,
        local: str | None = None,
        pack: int | None = None,
        units: int | None = None,
        barcode: str | int | None = None,
        at: Any = None,
        evidence_ids: list[str] | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            order = self._mutable_order(order_id)
            self._require_order_operator(order, operator)
            if order["status"] == "cancelled":
                raise ConflictError("cancelled order cannot be picked", details={"order": order_id})
            if order.get("picking"):
                raise ConflictError(
                    "picking fact already recorded and is append-only; report a mismatch/correction instead",
                    code="picking_fact_immutable",
                    details={"order": order_id, "picking": order["picking"]},
                )
            required = self._required_fulfillment_spec(order)
            actual_local = local
            actual_pack = pack
            barcode_str = None if barcode is None else str(barcode)

            # A scan alone is resolved through exact store+barcode+time.  It is
            # deliberately compared to the accepted local, not used to rewrite
            # the order to the current barcode owner.
            if actual_local is None and barcode_str is not None:
                scan = self.resolve_catalog(order["store"], at_dt, barcode=barcode_str)
                if scan["status"] != "exact" or not scan.get("item"):
                    raise ValidationError(
                        "scanned barcode does not uniquely identify a current store package",
                        code="barcode_not_unique_or_known",
                        details={"scan": scan},
                    )
                actual_local = scan["item"]["local"]
                actual_pack = actual_pack or scan["item"]["pieces"]

            if not actual_local:
                raise ValidationError("local or barcode is required to record picking")
            if actual_pack is None:
                if actual_local == required["local"]:
                    actual_pack = required["pack"]
                else:
                    raise ValidationError("pack is required when picking a different local")
            if units is None:
                units = required["units"]
            if not isinstance(units, int) or units <= 0 or pack is not None and (not isinstance(pack, int) or pack <= 0):
                raise ValidationError("pack and units must be positive integers")

            same_barcode_trap = False
            required_barcode = (order.get("accepted_commitment") or {}).get("barcode")
            if actual_local != required["local"] and barcode_str and required_barcode == barcode_str:
                same_barcode_trap = True
            elif actual_local != required["local"] and required_barcode:
                current = self.resolve_catalog(order["store"], at_dt, local=actual_local)
                item = current.get("item") if current["status"] == "exact" else None
                if item and item.get("barcode") == required_barcode:
                    same_barcode_trap = True

            if actual_local != required["local"] or int(actual_pack) != int(required["pack"]):
                raise ConflictError(
                    "picked package does not match the exact promised specification; create an explicit replacement confirmed by the customer",
                    code="same_barcode_different_spec" if same_barcode_trap else "fulfillment_spec_mismatch",
                    details={
                        "order": order_id,
                        "required": required,
                        "actual": {"local": actual_local, "pack": actual_pack, "units": units, "barcode": barcode_str},
                        "same_barcode_new_spec": same_barcode_trap,
                        "allowed_action": "propose_replacement_or_cancellation",
                    },
                )
            if units != required["units"]:
                raise ConflictError(
                    "picked unit count does not match the confirmed quantity",
                    code="quantity_mismatch",
                    details={"required_units": required["units"], "actual_units": units},
                )

            picking = {
                "picking_id": self._next_id("picking"),
                "recorded_at": iso(at_dt),
                "operator": operator,
                "store": order["store"],
                "local": actual_local,
                "pack": int(actual_pack),
                "units": units,
                "total_pieces": units * int(actual_pack),
                "barcode": barcode_str or required_barcode,
                "source_spec": required["source"],
                "evidence_ids": evidence_ids or [],
                "note": note,
            }
            order["picking"] = picking
            order["fulfillment_status"] = "picked"
            self._append_order_event(order, "picking_recorded", picking=_deep_copy(picking))
            return {"ok": True, "order": _deep_copy(order)}

    def record_delivery(
        self,
        order_id: str,
        *,
        operator: str,
        at: Any = None,
        note: str = "",
        evidence_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            order = self._mutable_order(order_id)
            self._require_order_operator(order, operator)
            if order["status"] == "cancelled":
                raise ConflictError("cancelled order cannot be delivered", details={"order": order_id})
            if not order.get("picking"):
                raise ConflictError("record exact-spec picking before delivery", code="picking_required")
            if order.get("delivery"):
                raise ConflictError("delivery fact already recorded", code="delivery_immutable")
            delivery = {
                "delivery_id": self._next_id("delivery"),
                "delivered_at": iso(at_dt),
                "operator": operator,
                "note": note,
                "evidence_ids": evidence_ids or [],
            }
            order["delivery"] = delivery
            order["fulfillment_status"] = "delivered"
            self._append_order_event(order, "delivery_recorded", delivery=_deep_copy(delivery))
            return {"ok": True, "order": _deep_copy(order)}

    def fulfill_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
        """Convenience method for tests/integrations: pick and deliver exact spec."""
        with self._lock:
            delivered_at = kwargs.pop("delivered_at", None)
            pick = self.record_picking(order_id, **kwargs)
            delivered = self.record_delivery(
                order_id,
                operator=kwargs["operator"],
                at=delivered_at or kwargs.get("at"),
            )
            return delivered

    def propose_replacement(
        self,
        order_id: str,
        *,
        operator: str,
        local: str | None = None,
        pack: int | None = None,
        units: int | None = None,
        barcode: str | int | None = None,
        reason: str = "",
        evidence_ids: list[str] | None = None,
        at: Any = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            order = self._mutable_order(order_id)
            self._require_order_operator(order, operator)
            if order["status"] == "cancelled":
                raise ConflictError("cancelled order cannot be replaced")
            if any(p["status"] == "pending" for p in order.get("proposals", [])):
                raise ConflictError("another proposal is pending customer confirmation", code="proposal_pending")
            if not local or pack is None:
                raise ValidationError(
                    "replacement requires exact local and pack; barcode or name similarity is not enough",
                    code="exact_replacement_spec_required",
                )
            if not isinstance(pack, int) or pack <= 0:
                raise ValidationError("replacement pack must be a positive integer")
            units = units or (order.get("accepted_commitment") or {}).get("units") or order.get("units")
            if not isinstance(units, int) or units <= 0:
                raise ValidationError("replacement units must be a positive integer")
            required = self._required_fulfillment_spec(order)
            if local == required["local"] and pack == required["pack"]:
                raise ConflictError("proposed replacement is the same specification; fulfill it directly")

            catalog_check = self.resolve_catalog(order["store"], at_dt, local=local)
            proposal = {
                "proposal_id": self._next_id("proposal"),
                "type": "replacement",
                "status": "pending",
                "proposed_by": operator,
                "proposed_at": iso(at_dt),
                "local": local,
                "pack": pack,
                "units": units,
                "total_pieces": units * pack,
                "barcode": None if barcode is None else str(barcode),
                "reason": reason,
                "evidence_ids": evidence_ids or [],
                "catalog_check": {
                    "status": catalog_check["status"],
                    "current_barcode": (catalog_check.get("item") or {}).get("barcode"),
                    "current_pieces": (catalog_check.get("item") or {}).get("pieces"),
                },
                "customer_decision": None,
            }
            order.setdefault("proposals", []).append(proposal)
            order["fulfillment_status"] = "proposal_pending"
            self._append_order_event(order, "replacement_proposed", proposal=_deep_copy(proposal))
            return {"ok": True, "proposal": _deep_copy(proposal), "order": _deep_copy(order)}

    def request_cancellation(
        self,
        order_id: str,
        *,
        operator: str,
        reason: str = "",
        evidence_ids: list[str] | None = None,
        at: Any = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            order = self._mutable_order(order_id)
            self._require_order_operator(order, operator)
            if order["status"] == "cancelled":
                raise ConflictError("order is already cancelled")
            if any(p["status"] == "pending" for p in order.get("proposals", [])):
                raise ConflictError("another proposal is pending customer confirmation", code="proposal_pending")
            proposal = {
                "proposal_id": self._next_id("proposal"),
                "type": "cancellation",
                "status": "pending",
                "proposed_by": operator,
                "proposed_at": iso(at_dt),
                "reason": reason,
                "evidence_ids": evidence_ids or [],
                "customer_decision": None,
            }
            order.setdefault("proposals", []).append(proposal)
            order["fulfillment_status"] = "proposal_pending"
            self._append_order_event(order, "cancellation_proposed", proposal=_deep_copy(proposal))
            return {"ok": True, "proposal": _deep_copy(proposal), "order": _deep_copy(order)}

    def customer_decide(
        self,
        order_id: str,
        proposal_id: str,
        *,
        customer: str,
        approve: bool,
        note: str = "",
        at: Any = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            order = self._mutable_order(order_id)
            if customer != order.get("customer"):
                raise AuthorizationError(
                    "only the customer on the order may confirm this proposal",
                    code="customer_identity_mismatch",
                    details={"order": order_id},
                )
            proposal = next((p for p in order.get("proposals", []) if p["proposal_id"] == proposal_id), None)
            if not proposal:
                raise NotFoundError("proposal not found", details={"proposal_id": proposal_id})
            if proposal["status"] != "pending":
                raise ConflictError("proposal has already been decided", details={"proposal": proposal})
            proposal["status"] = "approved" if approve else "declined"
            proposal["customer_decision"] = {
                "customer": customer,
                "approved": approve,
                "note": note,
                "at": iso(at_dt),
            }
            if approve and proposal["type"] == "replacement":
                order["agreed_resolution"] = {
                    "type": "replacement",
                    "proposal_id": proposal["proposal_id"],
                    "local": proposal["local"],
                    "pack": proposal["pack"],
                    "units": proposal["units"],
                    "total_pieces": proposal["total_pieces"],
                    "agreed_at": iso(at_dt),
                }
                order["fulfillment_status"] = "ready_for_picking"
                self._append_order_event(order, "replacement_confirmed", resolution=_deep_copy(order["agreed_resolution"]))
            elif approve and proposal["type"] == "cancellation":
                order["status"] = "cancelled"
                order["fulfillment_status"] = "cancelled"
                order["cancelled_at"] = iso(at_dt)
                self._append_order_event(order, "cancellation_confirmed", proposal_id=proposal_id)
            else:
                order["fulfillment_status"] = "pending"
                self._append_order_event(order, "proposal_declined", proposal_id=proposal_id)
            return {"ok": True, "order": _deep_copy(order)}

    # Friendly aliases for integration code.
    respond_proposal = customer_decide

    def report_spec_mismatch(
        self,
        order_id: str,
        *,
        operator: str | None = None,
        customer: str | None = None,
        actual_local: str | None = None,
        actual_pack: int | None = None,
        barcode: str | int | None = None,
        description: str = "",
        proposal_id: str | None = None,
        at: Any = None,
    ) -> dict[str, Any]:
        at_dt = resolve_at(at)
        with self._lock:
            order = self._mutable_order(order_id)
            if operator:
                self._require_order_operator(order, operator)
            elif customer:
                if customer != order.get("customer"):
                    raise AuthorizationError("customer identity mismatch", code="customer_identity_mismatch")
            else:
                raise AuthorizationError("operator or customer identity is required")
            approved_handling = None
            if proposal_id:
                proposal = next((p for p in order.get("proposals", []) if p["proposal_id"] == proposal_id), None)
                if not proposal:
                    raise NotFoundError("proposal not found", details={"proposal_id": proposal_id})
                if proposal.get("status") != "approved":
                    raise ConflictError("handling proposal is not customer-approved", details={"proposal": proposal})
                approved_handling = {
                    "proposal_id": proposal_id,
                    "type": proposal["type"],
                    "decision": proposal.get("customer_decision"),
                }
            report = {
                "report_id": self._next_id("mismatch"),
                "reported_at": iso(at_dt),
                "reported_by_operator": operator,
                "reported_by_customer": customer if not operator else None,
                "actual": {
                    "local": actual_local,
                    "pack": actual_pack,
                    "barcode": None if barcode is None else str(barcode),
                },
                "description": description,
                "customer_approved_handling": approved_handling,
            }
            order.setdefault("mismatch_reports", []).append(report)
            self._append_order_event(order, "spec_mismatch_reported", report=_deep_copy(report))
            return {"ok": True, "report": _deep_copy(report), "order": _deep_copy(order)}

    # Semantic aliases used by different integration vocabularies.
    explain_product = explain
    query_item = explain
    resolve_product = explain
    preview = preview_channel
    accept_purchase = ingest_purchase
    add_receipt = ingest_receipt
    record_receipt = ingest_receipt
    pause = pause_sales
    resume = resume_sales

    def accept_order(self, record: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        """Accept an order and return the immutable order aggregate directly."""
        return self.ingest_purchase(record, **kwargs)["order"]

    create_order = accept_order
    record_order = accept_order




