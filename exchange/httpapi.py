"""JSON HTTP adapter for the exchange service using the standard library."""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import ExchangeError
from .service import ExchangeService

ORDER_RE = re.compile(r"^/v1/orders/([^/]+)(?:/(trace|fulfillment|delivery|replacements|cancellation-requests|customer-decision|mismatch-reports))?/?$")
CONFLICT_RE = re.compile(r"^/v1/conflicts/([^/]+)/(claim|confirm)/?$")


def _query_one(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    return values[0] if values else None


def build_handler(service: ExchangeService, state_path: str | Path | None = None):
    class ExchangeHTTPRequestHandler(BaseHTTPRequestHandler):
        server_version = "ExchangeBackend/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # quieter tests
            if getattr(self.server, "log_requests", False):
                super().log_message(fmt, *args)

        # ------------------------------------------------------ responses
        def _send_json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _ok(self, payload: Any, status: int = 200) -> None:
            self._send_json(status, payload)

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            if not raw:
                return {}
            try:
                data = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ExchangeError("request body must be valid JSON", code="invalid_json", http_status=400) from exc
            if data is None:
                return {}
            if not isinstance(data, dict):
                raise ExchangeError("JSON request body must be an object", code="invalid_body", http_status=400)
            return data

        def _persist(self) -> None:
            if state_path:
                service.save_state(state_path)

        def _actor(self, body: dict[str, Any]) -> str | None:
            return body.get("operator") or self.headers.get("X-Operator-Id")

        def _customer(self, body: dict[str, Any]) -> str | None:
            return body.get("customer") or self.headers.get("X-Customer-Id")

        # --------------------------------------------------------- routing
        def do_GET(self) -> None:  # noqa: N802
            try:
                parsed = urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"
                query = parse_qs(parsed.query)
                q = lambda key: _query_one(query, key)  # noqa: E731

                if path == "/health":
                    self._ok({"ok": True, "service": "exchange-backend"})
                    return
                if path == "/":
                    self._ok(self._index())
                    return
                if path == "/v1/explain":
                    self._ok(
                        service.explain(
                            store=q("store"),
                            barcode=q("barcode"),
                            local=q("local"),
                            channel=q("channel"),
                            listing=q("listing"),
                            push=q("push"),
                            at=q("at"),
                        )
                    )
                    return
                if path == "/v1/preview":
                    self._ok(service.preview_channel(q("channel"), listing=q("listing"), at=q("at")))
                    return
                if path == "/v1/candidates":
                    self._ok({"items": service.list_candidates(channel=q("channel"), listing=q("listing"))})
                    return
                if path == "/v1/publications":
                    self._ok({"items": service.publications(channel=q("channel"), listing=q("listing"))})
                    return
                if path == "/v1/outbound-publications":
                    self._ok({"items": service.outbound_publications(channel=q("channel"), listing=q("listing"))})
                    return
                if path == "/v1/receipt-arrivals":
                    self._ok({"items": service.receipt_arrivals(channel=q("channel"), listing=q("listing"))})
                    return
                if path == "/v1/conflicts":
                    self._ok({"items": service.list_conflicts(at=q("at"))})
                    return
                if path == "/v1/orders":
                    self._ok({"items": service.list_orders(store=q("store"), operator=q("operator"), status=q("status"))})
                    return

                match = ORDER_RE.match(path)
                if match and self.command == "GET":
                    order_id, sub = match.groups()
                    if sub == "trace":
                        self._ok(service.trace_order(order_id))
                    elif sub is None:
                        self._ok(service.get_order(order_id))
                    else:
                        raise ExchangeError("method not allowed", code="method_not_allowed", http_status=405)
                    return

                raise ExchangeError("not found", code="route_not_found", http_status=404, details={"path": path})
            except ExchangeError as exc:
                self._send_json(exc.http_status, exc.to_dict())
            except Exception as exc:  # pragma: no cover - defensive adapter boundary
                self._send_json(500, {"error": {"code": "internal_error", "message": str(exc)}})

        def do_POST(self) -> None:  # noqa: N802,C901
            try:
                parsed = urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"
                query = parse_qs(parsed.query)
                q = lambda key: _query_one(query, key)  # noqa: E731
                body = self._read_json()

                if path == "/v1/ingest":
                    kind = body.get("kind") or q("kind")
                    if not kind:
                        raise ExchangeError("kind is required")
                    record = body.get("record") if "record" in body else body
                    result = service.ingest_record(kind, record)
                    self._persist()
                    self._ok(result, 201 if not result.get("duplicate") else 200)
                    return

                if path.startswith("/v1/ingest/"):
                    kind = path.rsplit("/", 1)[-1]
                    if isinstance(body.get("records"), list):
                        results = [service.ingest_record(kind, row) for row in body["records"]]
                    else:
                        results = [service.ingest_record(kind, body)]
                    self._persist()
                    self._ok({"results": results}, 201)
                    return

                if path == "/v1/amendments":
                    result = service.submit_amendment(body, at=body.get("at"))
                    self._persist()
                    self._ok(result, 201 if not result.get("duplicate") else 200)
                    return
                if path == "/v1/receipts" or path == "/v1/channel-receipts":
                    result = service.ingest_receipt(body)
                    self._persist()
                    self._ok(result, 201 if not result.get("duplicate") else 200)
                    return
                if path == "/v1/publications":
                    result = service.publish_candidate(
                        channel=body["channel"],
                        listing=body["listing"],
                        push=body.get("push"),
                        operator=self._actor(body),
                        at=body.get("at"),
                        gateway_name=body.get("gateway"),
                        allow_future=bool(body.get("allow_future", True)),
                    )
                    self._persist()
                    self._ok(result, 201 if not result.get("duplicate") else 200)
                    return
                if path == "/v1/purchases":
                    result = service.ingest_purchase(body, received_at=body.get("received_at"))
                    self._persist()
                    self._ok(result, 201 if not result.get("duplicate") else 200)
                    return
                if path == "/v1/pauses":
                    result = service.pause_sales(
                        operator=self._actor(body),
                        channel=body["channel"],
                        listing=body["listing"],
                        push=body.get("push"),
                        reason=body.get("reason", ""),
                        at=body.get("at"),
                        until=body.get("until"),
                        store=body.get("store"),
                    )
                    self._persist()
                    self._ok(result, 201)
                    return
                if path == "/v1/resumptions" or path == "/v1/sales/resume":
                    result = service.resume_sales(
                        operator=self._actor(body),
                        pause_id=body.get("pause_id"),
                        channel=body.get("channel"),
                        listing=body.get("listing"),
                        push=body.get("push"),
                        reason=body.get("reason", ""),
                        at=body.get("at"),
                    )
                    self._persist()
                    self._ok(result)
                    return

                conflict_match = CONFLICT_RE.match(path)
                if conflict_match:
                    conflict_id, action = conflict_match.groups()
                    if action == "claim":
                        result = service.claim_conflict(
                            conflict_id,
                            operator=self._actor(body),
                            evidence_ids=body.get("evidence_ids"),
                            note=body.get("note", ""),
                            at=body.get("at"),
                        )
                    else:
                        result = service.confirm_association(
                            conflict_id,
                            operator=self._actor(body),
                            push=body.get("push"),
                            chosen=body.get("chosen"),
                            window_from=body.get("window_from"),
                            window_until=body.get("window_until"),
                            note=body.get("note", ""),
                            evidence_ids=body.get("evidence_ids"),
                            at=body.get("at"),
                        )
                    self._persist()
                    self._ok(result)
                    return

                order_match = ORDER_RE.match(path)
                if order_match:
                    order_id, sub = order_match.groups()
                    actor = self._actor(body)
                    if sub == "fulfillment":
                        result = service.record_picking(
                            order_id,
                            operator=actor,
                            local=body.get("local"),
                            pack=body.get("pack"),
                            units=body.get("units"),
                            barcode=body.get("barcode"),
                            at=body.get("at"),
                            evidence_ids=body.get("evidence_ids"),
                            note=body.get("note", ""),
                        )
                    elif sub == "delivery":
                        result = service.record_delivery(
                            order_id,
                            operator=actor,
                            at=body.get("at"),
                            note=body.get("note", ""),
                            evidence_ids=body.get("evidence_ids"),
                        )
                    elif sub == "replacements":
                        result = service.propose_replacement(
                            order_id,
                            operator=actor,
                            local=body.get("local"),
                            pack=body.get("pack"),
                            units=body.get("units"),
                            barcode=body.get("barcode"),
                            reason=body.get("reason", ""),
                            evidence_ids=body.get("evidence_ids"),
                            at=body.get("at"),
                        )
                    elif sub == "cancellation-requests":
                        result = service.request_cancellation(
                            order_id,
                            operator=actor,
                            reason=body.get("reason", ""),
                            evidence_ids=body.get("evidence_ids"),
                            at=body.get("at"),
                        )
                    elif sub == "customer-decision":
                        result = service.customer_decide(
                            order_id,
                            body["proposal_id"],
                            customer=self._customer(body),
                            approve=bool(body["approve"]),
                            note=body.get("note", ""),
                            at=body.get("at"),
                        )
                    elif sub == "mismatch-reports":
                        result = service.report_spec_mismatch(
                            order_id,
                            operator=actor,
                            customer=self._customer(body),
                            actual_local=body.get("actual_local"),
                            actual_pack=body.get("actual_pack"),
                            barcode=body.get("barcode"),
                            description=body.get("description", ""),
                            proposal_id=body.get("proposal_id"),
                            at=body.get("at"),
                        )
                    else:
                        raise ExchangeError("use POST /v1/purchases to accept an order", code="method_not_allowed", http_status=405)
                    self._persist()
                    self._ok(result, 201 if sub in {"replacements", "cancellation-requests", "mismatch-reports"} else 200)
                    return

                raise ExchangeError("not found", code="route_not_found", http_status=404, details={"path": path})
            except ExchangeError as exc:
                self._send_json(exc.http_status, exc.to_dict())
            except KeyError as exc:
                self._send_json(400, {"error": {"code": "missing_field", "message": f"missing field: {exc}"}})
            except Exception as exc:  # pragma: no cover
                self._send_json(500, {"error": {"code": "internal_error", "message": str(exc)}})

        def _index(self) -> dict[str, Any]:
            return {
                "name": "multi-channel specification and promise backend",
                "endpoints": [
                    "GET /health",
                    "GET /v1/explain",
                    "GET /v1/preview",
                    "GET /v1/candidates",
                    "GET /v1/publications",
                    "GET /v1/conflicts",
                    "POST /v1/ingest/{stores|catalog|listings|amendments|channel_receipts|purchases}",
                    "POST /v1/amendments",
                    "POST /v1/receipts",
                    "POST /v1/purchases",
                    "POST /v1/pauses",
                    "POST /v1/resumptions",
                    "POST /v1/conflicts/{id}/claim",
                    "POST /v1/conflicts/{id}/confirm",
                    "GET /v1/orders",
                    "GET /v1/orders/{id}",
                    "GET /v1/orders/{id}/trace",
                    "POST /v1/orders/{id}/fulfillment",
                    "POST /v1/orders/{id}/delivery",
                    "POST /v1/orders/{id}/replacements",
                    "POST /v1/orders/{id}/cancellation-requests",
                    "POST /v1/orders/{id}/customer-decision",
                    "POST /v1/orders/{id}/mismatch-reports",
                ],
            }

    return ExchangeHTTPRequestHandler


def create_server(
    host: str = "127.0.0.1",
    port: int = 8080,
    *,
    service: ExchangeService | None = None,
    state_path: str | Path | None = None,
) -> ThreadingHTTPServer:
    if state_path:
        service = service or ExchangeService.load_state(state_path)
    service = service or ExchangeService()
    handler = build_handler(service, state_path)
    server = ThreadingHTTPServer((host, port), handler)
    server.service = service  # type: ignore[attr-defined]
    return server
