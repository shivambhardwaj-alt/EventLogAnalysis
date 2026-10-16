"""v2 production features: anomaly detection, alerts (+ webhook), schema drift,
run lock, schema migration, right to be forgotten, dashboard API."""
import json
import sqlite3
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

from funnel.alerts import Alert, WebhookNotifier, default_notifiers, record_and_deliver
from funnel.db import connect
from funnel.lock import PipelineLockedError, RunLock
from funnel.privacy import erase_user
from funnel.server import Api, ApiError, make_server
from tests.helpers import PipelineTestCase, ev

DAY = 86400


def day_sessions(day: int, n: int, cart_rate: float, prefix: str, app_version: str = "2.3.1") -> list[dict]:
    """n sessions on `day`; the first n*cart_rate of them add to cart."""
    out = []
    for i in range(n):
        u, t = f"{prefix}{day}_{i}", day * DAY + i * 5
        out.append(ev(f"{u}-v", "view_product", t, user=u, app_version=app_version))
        if i < round(n * cart_rate):
            out.append(ev(f"{u}-c", "add_to_cart", t + 60, user=u, app_version=app_version))
    return out


class TestAnomalyDetection(PipelineTestCase):
    def test_regression_is_flagged_from_its_first_day_and_stays_flagged(self):
        events = []
        for d, rate in enumerate([0.70, 0.72, 0.69, 0.71, 0.30, 0.32]):
            events += day_sessions(d, 120, rate, "u")
        self.write(events)
        s = self.run_pipeline(self.cfg.replace(funnel_steps=("view_product", "add_to_cart")))
        days = sorted({a["session_date"] for a in s.anomalies})
        self.assertEqual(days, ["2026-03-06", "2026-03-07"])                       # days 5 and 6
        self.assertTrue(all(a["step_name"] == "add_to_cart" for a in s.anomalies))
        # baseline excludes anomalous days: both days compared with the same healthy baseline
        self.assertEqual({a["baseline_sessions"] for a in s.anomalies}, {480})
        self.assertEqual(s.drop.anomaly_started_on, "2026-03-06")
        self.assertEqual([a.kind for a in s.new_alerts], ["funnel_conversion_drop"])

    def test_noise_and_small_samples_are_not_flagged(self):
        events = []
        for d, rate in enumerate([0.70, 0.66, 0.73, 0.68, 0.71]):       
            events += day_sessions(d, 120, rate, "u")
        events += day_sessions(5, 20, 0.10, "small")                        
        self.write(events)
        cfg = self.cfg.replace(funnel_steps=("view_product", "add_to_cart"))
        self.assertEqual(self.run_pipeline(cfg).anomalies, [])


class TestAlerts(PipelineTestCase):
    def test_alerts_are_deduplicated_by_key(self):
        con = connect(self.cfg.warehouse_path)
        from funnel.db import apply_schema
        apply_schema(con)
        sent = []

        class Capture:
            name = "capture"

            def send(self, alert):
                sent.append(alert.key)

        a = Alert("drift:x", "warning", "schema_drift", "x appeared")
        self.assertEqual(len(record_and_deliver(con, "r1", [a], [Capture()])), 1)
        self.assertEqual(len(record_and_deliver(con, "r2", [a], [Capture()])), 0)
        self.assertEqual(sent, ["drift:x"])
        con.close()

    def test_webhook_payload_and_failure_is_not_fatal(self):
        received = []

        class Hook(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
                self.send_response(200)
                self.end_headers()

            def log_message(self, *a):
                pass

        srv = HTTPServer(("127.0.0.1", 0), Hook)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            url = f"http://127.0.0.1:{srv.server_port}/T0KEN-secret"
            WebhookNotifier(url).send(Alert("k", "critical", "funnel_conversion_drop", "payment fell"))
            self.assertIn("payment fell", received[0]["text"])
            self.assertNotIn("T0KEN", repr(WebhookNotifier(url)))            # URL (a secret) never printed
        finally:
            srv.shutdown()
            srv.server_close()
        self.assertEqual([n.name for n in default_notifiers({"ALERT_WEBHOOK_URL": url})], ["log", "webhook"])
        self.assertEqual([n.name for n in default_notifiers({})], ["log"])

        # unreachable webhook: run still succeeds, alert stored with delivered='log'
        self.write([ev(str(i), "search", i) for i in range(3)] + ["not json"] * 7)
        s = self.run_pipeline(notifiers=default_notifiers({"ALERT_WEBHOOK_URL": "http://127.0.0.1:9/x"}))
        self.assertEqual(s.status, "success")
        self.assertEqual(self.one("SELECT delivered FROM alerts WHERE kind='high_quarantine_rate'"), "log")


class TestSchemaDrift(PipelineTestCase):
    def test_undeclared_fields_are_reported_once_with_first_file(self):
        self.write([ev("a", "search", 0)], name="d1.jsonl")
        self.run_pipeline()
        self.write([ev("b", "search", 10, sdk="ios-9", properties={"product_id": "p", "coupon": "X"}),
                    ev("c", "search", 20, sdk="ios-9")], name="d2.jsonl")
        s = self.run_pipeline()
        drift = {d["field"]: (d["rows"], d["first_seen_file"]) for d in s.schema_drift}
        self.assertEqual(drift, {"sdk": (2, "d2.jsonl"), "properties.coupon": (1, "d2.jsonl")})
        self.assertEqual(sorted(a.key for a in s.new_alerts), ["drift:properties.coupon", "drift:sdk"])
        # rows with extra fields are still accepted - drift is reported, not rejected
        self.assertEqual(self.one("SELECT COUNT(*) FROM clean_events"), 3)


class TestRunLock(PipelineTestCase):
    def test_second_writer_is_refused_and_stale_lock_is_taken_over(self):
        path = self.cfg.warehouse_path.with_suffix(".lock")
        with RunLock(path, timeout_seconds=60):
            with self.assertRaises(PipelineLockedError):
                self.run_pipeline()
        self.run_pipeline()                                            # free again
        path.write_text(json.dumps({"pid": 1, "since": time.time() - 99999}))
        RunLock(path, timeout_seconds=60).acquire().release()          # stale -> taken over
        self.assertFalse(path.exists())


class TestSchemaMigration(PipelineTestCase):
    def test_old_schema_version_is_rebuilt_from_raw(self):
        self.write([ev("a", "view_product", 0), ev("b", "add_to_cart", 30)])
        self.run_pipeline()
        con = sqlite3.connect(self.cfg.warehouse_path)
        con.execute("UPDATE pipeline_state SET value='1' WHERE key='schema_version'")
        con.commit()
        con.close()
        s = self.run_pipeline()
        self.assertEqual((s.mode, s.full_refresh_reason), ("full_refresh", "warehouse schema upgraded"))
        self.assertEqual(self.one("SELECT COUNT(*) FROM clean_events"), 2)


class TestForgetUser(PipelineTestCase):
    def test_user_is_removed_everywhere_and_stays_removed(self):
        self.write([ev("a1", "view_product", 0, user="alice"), ev("a2", "add_to_cart", 30, user="alice"),
                    ev("a1", "view_product", 0, user="alice"),                   # duplicate row
                    json.dumps(ev("a3", "teleport", 40, user="alice")),          # quarantined row
                    ev("b1", "view_product", 0, user="bob")])
        self.run_pipeline()
        r = erase_user(self.cfg, "alice")
        self.assertEqual((r.raw_rows, r.sessions), (4, 1))
        for table in ("stg_events", "clean_events"):
            self.assertEqual(self.one(f"SELECT COUNT(*) FROM {table} WHERE user_id = 'alice'"), 0, table)
        self.assertEqual(self.one("SELECT COUNT(*) FROM quarantine_events WHERE raw_payload LIKE '%alice%'"), 0)
        self.assertEqual(self.one("SELECT COUNT(*) FROM duplicate_events"), 0)
        self.assertEqual(self.one("SELECT COUNT(*) FROM raw_events WHERE raw_payload LIKE '%alice%'"), 0)
        self.assertEqual(self.one("SELECT COUNT(*) FROM sessions WHERE user_id='alice'"), 0)
        self.assertEqual(self.one("SELECT sessions_reached FROM funnel_metrics WHERE step_index=1"), 1)  # bob only
        self.assertNotIn("alice", self.one("SELECT user_hash FROM erased_users"))
        # a late file with alice's events is filtered at ingestion
        self.write([ev("a9", "purchase", 999, user="alice"), ev("b9", "add_to_cart", 60, user="bob")])
        s = self.run_pipeline()
        self.assertEqual(s.ingest.erased_rows_dropped, 1)
        self.assertEqual(self.one("SELECT COUNT(*) FROM raw_events WHERE raw_payload LIKE '%alice%'"), 0)
        self.assertTrue(s.quality["all_checks_passed"])


class TestDashboardApi(PipelineTestCase):
    def setUp(self):
        super().setUp()
        events = []
        for i in range(40):
            dev = "mobile" if i % 2 else "desktop"
            events += [ev(f"{i}v", "view_product", i * 10, user=f"u{i}", device=dev)]
            if dev == "desktop":
                events += [ev(f"{i}c", "add_to_cart", i * 10 + 5, user=f"u{i}", device=dev)]
        self.write(events)
        self.run_pipeline(write_report=True)
        self.api = Api(self.cfg)

    def test_filtered_funnel_is_computed_live(self):
        all_ = self.api.funnel({})
        mob = self.api.funnel({"device": "mobile"})
        self.assertEqual(all_["steps"][1]["sessions_reached"], 20)
        self.assertEqual(mob["steps"][0]["sessions_reached"], 20)
        self.assertEqual(mob["steps"][1]["sessions_reached"], 0)
        self.assertEqual(self.api.funnel({"device": "' OR 1=1 --"})["sessions_in_filter"], 0)  # injection-safe
        with self.assertRaises(ApiError):
            self.api.funnel({"date_from": "yesterday"})

    def test_user_endpoint_and_errors(self):
        u = self.api.user("u0")
        self.assertEqual([e["event_name"] for e in u["events"]], ["view_product", "add_to_cart"])
        with self.assertRaises(ApiError) as cm:
            self.api.user("nobody")
        self.assertEqual(cm.exception.status, 404)

    def test_http_server_end_to_end(self):
        srv = make_server(self.cfg, port=0)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{srv.server_port}"
        try:
            for path in ("/api/health", "/api/dimensions", "/api/daily", "/api/runs", "/api/alerts",
                         "/api/segments", "/api/explanation", "/api/funnel?device=desktop"):
                with urllib.request.urlopen(base + path) as r:
                    self.assertEqual(r.status, 200, path)
                    json.loads(r.read())
            with urllib.request.urlopen(base + "/") as r:
                self.assertIn(b"live dashboard", r.read())
            with self.assertRaises(urllib.error.HTTPError) as cm:
                urllib.request.urlopen(base + "/api/nope")
            self.assertEqual(cm.exception.code, 404)
        finally:
            srv.shutdown()
            srv.server_close()


if __name__ == "__main__":
    unittest.main()
