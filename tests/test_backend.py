"""Behavior tests for exact commitments, conflicts, operator transition and API."""
from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.client import RemoteDisconnected
from pathlib import Path

from exchange import AuthorizationError, ConflictError, ExchangeService, ValidationError
from exchange.httpapi import create_server

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def sample_rows(name: str):
    return [json.loads(line) for line in (SAMPLES / f"{name}.ndjson").read_text().splitlines() if line.strip()]


class ExchangeBehaviorTest(unittest.TestCase):
    def setUp(self):
        self.svc = ExchangeService()

    def load(self, *kinds):
        for kind in kinds:
            self.svc.ingest_file(SAMPLES / f"{kind}.ndjson", kind)

    def test_barcode_is_time_and_store_scoped_but_never_similarity(self):
        self.load("stores", "catalog")
        old_time = "2026-11-27T18:50:00+08:00"
        boundary = "2026-11-27T20:00:00+08:00"

        old = self.svc.resolve_catalog("s1", old_time, barcode="S119")
        self.assertEqual(old["status"], "exact")
        self.assertEqual(old["item"]["local"], "case-old")
        self.assertEqual(old["item"]["pieces"], 3)

        # Half-open boundary: old until == new from means new owns boundary.
        new = self.svc.resolve_catalog("s1", boundary, barcode="S119")
        self.assertEqual(new["item"]["local"], "case-new")
        self.assertEqual(new["item"]["pieces"], 2)

        other_store = self.svc.resolve_catalog("s2", old_time, barcode="S119")
        self.assertEqual(other_store["item"]["local"], "case-other")
        self.assertEqual(other_store["item"]["pieces"], 1)

        missing = self.svc.resolve_catalog("s1", "2026-10-01T00:00:00+08:00", barcode="S119")
        self.assertEqual(missing["status"], "not_found")

    def test_amendment_batch_reports_overlap_and_keeps_other_channel_selling(self):
        self.load("stores", "catalog", "listings", "channel_receipts")
        batch_11 = sample_rows("amendments")[0]
        result = self.svc.submit_amendment(batch_11, at="2026-11-27T19:35:00+08:00")

        self.assertEqual(len(result["all_conflicts"]), 1)
        conflict = result["all_conflicts"][0]
        self.assertEqual(conflict["conflict_id"], "channel-listing:ch-a:A7")
        self.assertEqual(conflict["store_ids"], ["s1"])
        self.assertEqual(conflict["windows"][0]["from"], "2026-11-27T19:30:00+08:00")
        packages = {(g["push"], g["local"], g["pack"]) for g in conflict["affected_goods"]}
        self.assertIn(("tx-a1", "case-old", 3), packages)
        self.assertIn(("tx-a2", "case-new", 2), packages)

        preview = self.svc.preview_channel("ch-a", at="2026-11-27T19:45:00+08:00")
        self.assertEqual({i["push"]: i["status"] for i in preview["items"]}, {
            "tx-a1": "under_review",
            "tx-a2": "under_review",
        })
        ch_b = self.svc.preview_channel("ch-b", at="2026-11-27T19:45:00+08:00")
        self.assertEqual(ch_b["now_sellable_count"], 1)
        self.assertEqual(ch_b["sellable_now"][0]["commitment"]["local"], "case-other")

    def test_missing_fields_do_not_change_but_explicit_clear_ends_relation(self):
        self.load("stores", "catalog", "listings", "channel_receipts")
        rows = sample_rows("amendments")
        self.svc.submit_amendment(rows[0])
        self.svc.ingest_record("amendments", rows[1])
        historical_impact = self.svc.state["amendment_batches"]["m-11"]["impact"]
        self.assertEqual(historical_impact["conflict_ids"], ["channel-listing:ch-a:A7"])
        assoc = next(a for a in self.svc.list_candidates(channel="ch-a", listing="A7") if a["push"] == "tx-a2")
        self.assertEqual(assoc["from"], "2026-11-27T19:30:00+08:00")
        self.assertIsNone(assoc["local"])
        self.assertIn("local", assoc["cleared_fields"])
        preview = self.svc.preview_channel("ch-a", at="2026-11-27T19:45:00+08:00")
        by_push = {i["push"]: i["status"] for i in preview["items"]}
        self.assertEqual(by_push["tx-a2"], "unavailable")
        self.assertEqual(by_push["tx-a1"], "sellable_now")

    def test_one_material_can_move_alias_window_without_changing_identity(self):
        self.load("listings")
        self.svc.submit_amendment(
            {
                "batch": "move-window",
                "operator": "org-c",
                "changes": [
                    {
                        "channel": "ch-b",
                        "listing": "A7",
                        "push": "tx-b1",
                        "until": "2026-12-01T00:00:00+08:00",
                    }
                ],
                "clear": [],
            }
        )
        assoc = next(a for a in self.svc.list_candidates(channel="ch-b", listing="A7") if a["push"] == "tx-b1")
        self.assertEqual(assoc["local"], "case-other")
        self.assertEqual(assoc["from"], "2026-11-26T10:00:00+08:00")
        self.assertEqual(assoc["until"], "2026-12-01T00:00:00+08:00")
        self.assertEqual(assoc["field_sources"]["until"], "move-window")

    def test_future_reservation_and_current_sale_are_distinct(self):
        self.load("stores", "catalog", "listings", "channel_receipts")
        preview = self.svc.preview_channel("ch-a", at="2026-11-27T12:00:00+08:00")
        by_push = {i["push"]: i["status"] for i in preview["items"]}
        self.assertEqual(by_push["tx-a1"], "sellable_now")
        self.assertEqual(by_push["tx-a2"], "reservable_future")

    def test_duplicate_receipt_cannot_resume_paused_channel_or_other_channel(self):
        self.load("stores", "catalog", "listings", "amendments", "channel_receipts")
        pause = self.svc.pause_sales(
            operator="org-b", channel="ch-a", listing="A7", at="2026-11-27T19:35:00+08:00"
        )
        self.assertFalse(pause["duplicate"])

        repeated = self.svc.ingest_receipt(
            {"channel": "ch-a", "listing": "A7", "push": "tx-a1", "state": "visible"}
        )
        self.assertTrue(repeated["duplicate"])
        self.assertGreaterEqual(repeated["receipt"]["arrival_count"], 2)

        preview_a = self.svc.preview_channel("ch-a", at="2026-11-27T19:45:00+08:00")
        self.assertTrue(all(i["status"] in {"paused", "unavailable"} for i in preview_a["items"]))
        self.assertEqual(preview_a["sellable_now"], [])

        preview_b = self.svc.preview_channel("ch-b", at="2026-11-27T19:45:00+08:00")
        self.assertEqual(preview_b["now_sellable_count"], 1)

        # Receipts never create an association.
        before = len(self.svc.list_candidates())
        self.svc.ingest_receipt({"channel": "ch-z", "listing": "Z9", "push": "p1", "state": "visible"})
        self.assertEqual(len(self.svc.list_candidates()), before)

    def test_push_specific_pause_does_not_stop_another_push(self):
        self.load("stores", "catalog", "listings", "channel_receipts")
        self.svc.pause_sales(
            operator="org-a",
            channel="ch-a",
            listing="A7",
            push="tx-a1",
            at="2026-11-27T12:00:00+08:00",
        )
        preview = self.svc.preview_channel("ch-a", at="2026-11-27T12:30:00+08:00")
        by_push = {i["push"]: i["status"] for i in preview["items"]}
        self.assertEqual(by_push["tx-a1"], "paused")
        self.assertEqual(by_push["tx-a2"], "reservable_future")

    def test_claim_confirm_and_backdated_authorization(self):
        self.load("stores", "catalog", "listings", "channel_receipts")
        self.svc.submit_amendment(sample_rows("amendments")[0])
        with self.assertRaises(AuthorizationError):
            self.svc.claim_conflict(
                "channel-listing:ch-a:A7", operator="org-c", at="2026-11-27T19:40:00+08:00"
            )
        claim = self.svc.claim_conflict(
            "channel-listing:ch-a:A7", operator="org-b", at="2026-11-27T19:40:00+08:00"
        )
        self.assertEqual(claim["conflict"]["status"], "claimed")
        confirmed = self.svc.confirm_association(
            "channel-listing:ch-a:A7",
            operator="org-b",
            push="tx-a1",
            at="2026-11-27T19:41:00+08:00",
        )
        self.assertEqual(confirmed["decision"]["chosen"]["local"], "case-old")
        preview = self.svc.preview_channel("ch-a", at="2026-11-27T19:45:00+08:00")
        by_push = {i["push"]: i["status"] for i in preview["items"]}
        self.assertEqual(by_push["tx-a1"], "sellable_now")
        self.assertEqual(by_push["tx-a2"], "unavailable")

        with self.assertRaises(AuthorizationError) as ctx:
            self.svc.confirm_association(
                "channel-listing:ch-a:A7",
                operator="org-b",
                push="tx-a1",
                window_from="2026-11-02T09:00:00+08:00",
                at="2026-11-27T19:41:00+08:00",
            )
        self.assertEqual(ctx.exception.code, "backdated_operator_action")

    def test_orders_keep_accepted_quantity_store_and_source(self):
        self.svc.ingest_samples(SAMPLES)
        o17 = self.svc.get_order("order-17")
        o18 = self.svc.get_order("order-18")
        o21 = self.svc.get_order("order-21")

        self.assertEqual(o17["managing_operator_id"], "org-a")
        self.assertEqual(o17["accepted_commitment"]["local"], "case-old")
        self.assertEqual(o17["accepted_commitment"]["total_pieces"], 6)
        self.assertEqual(o18["managing_operator_id"], "org-b")
        self.assertEqual(o18["store"], "s1")
        self.assertEqual(o18["accepted_commitment"]["local"], "case-old")
        self.assertEqual(o18["accepted_commitment"]["promised_pack_pieces"], 3)
        self.assertEqual(o18["accepted_commitment"]["total_pieces"], 6)
        self.assertTrue(any("stale_channel_push" in w for w in o18["warnings"]))
        stale_preview = self.svc.preview_channel("ch-a", at="2026-11-27T20:09:00+08:00")
        stale_item = next(i for i in stale_preview["items"] if i["push"] == "tx-a1")
        self.assertEqual(stale_item["status"], "under_review")
        self.assertIn("stale_channel_publication_after_effective_until", stale_item["reason_codes"])
        self.assertEqual(o21["store"], "s2")
        self.assertEqual(o21["accepted_commitment"]["total_pieces"], 2)

        trace = self.svc.trace_order("order-18")
        self.assertEqual(trace["immutable_accepted_commitment"]["push"], "tx-a1")
        self.assertIn("source_chain_at_ordered_at", trace)

        # Repeated channel delivery is idempotent.
        before = len(self.svc.state["orders"])
        results = self.svc.ingest_file(SAMPLES / "purchases.ndjson", "purchases")
        self.assertTrue(all(r["duplicate"] for r in results))
        self.assertEqual(len(self.svc.state["orders"]), before)

    def test_same_barcode_new_spec_cannot_be_silent_substitution(self):
        self.svc.ingest_samples(SAMPLES)
        with self.assertRaises(ConflictError) as ctx:
            self.svc.record_picking(
                "order-18", operator="org-b", barcode="S119", at="2026-11-27T20:15:00+08:00"
            )
        self.assertEqual(ctx.exception.code, "same_barcode_different_spec")
        self.assertTrue(ctx.exception.details["same_barcode_new_spec"])
        self.assertEqual(ctx.exception.details["required"]["local"], "case-old")
        self.assertEqual(ctx.exception.details["actual"]["local"], "case-new")

        with self.assertRaises(AuthorizationError):
            self.svc.fulfill_order(
                "order-17", operator="org-b", local="case-old", pack=3, at="2026-11-27T18:55:00+08:00"
            )
        delivered = self.svc.fulfill_order(
            "order-17", operator="org-a", local="case-old", pack=3, at="2026-11-27T18:55:00+08:00"
        )
        self.assertEqual(delivered["order"]["fulfillment_status"], "delivered")

    def test_explicit_replacement_requires_customer_and_preserves_original(self):
        self.svc.ingest_samples(SAMPLES)
        with self.assertRaises(ValidationError):
            self.svc.propose_replacement("order-18", operator="org-b", barcode="S119")
        proposal = self.svc.propose_replacement(
            "order-18",
            operator="org-b",
            local="case-new",
            pack=2,
            units=2,
            reason="old 3-pack unavailable",
            at="2026-11-27T20:15:00+08:00",
        )["proposal"]
        with self.assertRaises(AuthorizationError):
            self.svc.customer_decide(
                "order-18", proposal["proposal_id"], customer="viewer-999", approve=True
            )
        decided = self.svc.customer_decide(
            "order-18", proposal["proposal_id"], customer="viewer-2", approve=True
        )
        self.assertEqual(decided["order"]["agreed_resolution"]["local"], "case-new")
        self.assertEqual(decided["order"]["accepted_commitment"]["local"], "case-old")
        delivered = self.svc.fulfill_order(
            "order-18", operator="org-b", barcode="S119", at="2026-11-27T20:20:00+08:00"
        )
        self.assertEqual(delivered["order"]["picking"]["source_spec"], "customer_confirmed_replacement")
        self.assertEqual(delivered["order"]["picking"]["total_pieces"], 4)

    def test_cancellation_and_mismatch_report_flow(self):
        self.svc.ingest_samples(SAMPLES)
        proposal = self.svc.request_cancellation("order-21", operator="org-c", reason="stock lost")["proposal"]
        self.svc.customer_decide("order-21", proposal["proposal_id"], customer="viewer-2", approve=True)
        order = self.svc.get_order("order-21")
        self.assertEqual(order["status"], "cancelled")
        with self.assertRaises(ConflictError):
            self.svc.record_picking("order-21", operator="org-c", local="case-other", pack=1)

        report = self.svc.report_spec_mismatch(
            "order-18",
            customer="viewer-2",
            actual_local="case-new",
            actual_pack=2,
            barcode="S119",
            description="received 2-pack instead of promised 3-pack",
        )
        self.assertEqual(report["report"]["actual"]["local"], "case-new")

    def test_late_purchase_evidence_reconciles(self):
        purchase = sample_rows("purchases")[1]
        created = self.svc.ingest_purchase(purchase)["order"]
        self.assertEqual(created["status"], "unresolved_commitment")
        self.load("stores", "catalog", "listings")
        reconciled = self.svc.get_order("order-18")
        self.assertEqual(reconciled["status"], "accepted")
        self.assertEqual(reconciled["accepted_commitment"]["local"], "case-old")
        self.assertTrue(any(e["type"] == "commitment_reconciled_from_late_evidence" for e in reconciled["events"]))

    def test_snapshot_persistence_round_trip(self):
        self.svc.ingest_samples(SAMPLES)
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "state.json"
            self.svc.save_state(path)
            restored = ExchangeService.load_state(path)
            self.assertEqual(restored.get_order("order-17")["accepted_commitment"]["total_pieces"], 6)
            repeated = restored.ingest_record("purchases", sample_rows("purchases")[0])
            self.assertTrue(repeated["duplicate"])


class HTTPAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.svc = ExchangeService()
        cls.svc.ingest_samples(SAMPLES)
        cls.server = create_server("127.0.0.1", 0, service=cls.svc)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def get(self, path):
        with urllib.request.urlopen(self.base + path, timeout=5) as response:
            return response.status, json.loads(response.read().decode())

    def post(self, path, payload, operator=None, customer=None):
        headers = {"Content-Type": "application/json"}
        if operator:
            headers["X-Operator-Id"] = operator
        if customer:
            headers["X-Customer-Id"] = customer
        req = urllib.request.Request(
            self.base + path,
            data=json.dumps(payload).encode(),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status, json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def test_health_explain_preview_and_order_routes(self):
        status, health = self.get("/health")
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])

        status, explain = self.get(
            "/v1/explain?store=s1&barcode=S119&at=2026-11-27T18:50:00%2B08:00"
        )
        self.assertEqual(status, 200)
        self.assertEqual(explain["sold_as"]["local"], "case-old")

        status, order = self.get("/v1/orders/order-18/trace")
        self.assertEqual(status, 200)
        self.assertEqual(order["immutable_accepted_commitment"]["push"], "tx-a1")

        status, result = self.post(
            "/v1/pauses",
            {"channel": "ch-b", "listing": "A7", "at": "2026-11-27T20:10:00+08:00", "reason": "api test"},
            operator="org-c",
        )
        self.assertEqual(status, 201)
        status, forbidden = self.post(
            "/v1/orders/order-21/cancellation-requests",
            {"reason": "x"},
            operator="org-a",
        )
        self.assertEqual(status, 403)
        self.assertEqual(forbidden["error"]["code"], "order_operator_transition_forbidden")


if __name__ == "__main__":
    unittest.main()
