"""End to end on generated data: the pipeline must find exactly what the
generator planted, and incremental loading must equal a full rebuild."""
import json
from datetime import datetime, timezone

from funnel.generator import GeneratorSettings, generate
from tests.helpers import PipelineTestCase

TABLES = {
    "clean_events": "SELECT event_id, user_id, event_name, event_ts, device, country, channel, product_id, price, "
                    "is_near_duplicate, session_id FROM clean_events ORDER BY event_id",
    "sessions": "SELECT * FROM sessions ORDER BY session_id",
    "funnel_metrics": "SELECT * FROM funnel_metrics ORDER BY step_index",
    "funnel_segments": "SELECT * FROM funnel_segments ORDER BY dimension, segment_value, step_index",
    "quarantine": "SELECT reason, raw_payload FROM quarantine_events ORDER BY raw_payload",
    "daily_funnel": "SELECT * FROM daily_funnel ORDER BY session_date, step_index",
    "anomalies": "SELECT * FROM funnel_anomalies ORDER BY session_date, step_index",
    "schema_drift": "SELECT field, rows, first_seen_file FROM schema_drift ORDER BY field",
}


class TestEndToEnd(PipelineTestCase):
    def setUp(self):
        super().setUp()
        self.cfg = self.cfg.replace(min_sample_size=30)
        start = datetime(2026, 9, 1, tzinfo=timezone.utc)
        self.truth = generate(self.incoming, GeneratorSettings(users=1500, days=7, seed=7, start=start))
        self.files = sorted(p.name for p in self.incoming.iterdir())

    def snapshot(self):
        return {name: [tuple(r) for r in self.q(sql)] for name, sql in TABLES.items()}

    def test_finds_every_planted_issue_and_the_planted_drop(self):
        s = self.run_pipeline(write_report=True)
        inj = self.truth["injected"]

        quarantined = {r[0]: r[1] for r in self.q("SELECT reason, COUNT(*) FROM quarantine_events GROUP BY reason")}
        expected = {k.split(":", 1)[1]: v for k, v in inj.items() if k.startswith("quarantine:")}
        self.assertEqual(quarantined, expected)
        self.assertEqual(s.exact_duplicates, inj["exact_duplicates"])
        self.assertEqual(s.conflicting_duplicates, inj["conflicting_duplicates"])
        self.assertEqual(self.one("SELECT COUNT(*) FROM clean_events WHERE is_near_duplicate=1"), inj["near_duplicates"])

        self.assertTrue(s.quality["all_checks_passed"], s.quality["checks"])
        self.assertEqual((s.drop.from_step, s.drop.to_step), ("begin_checkout", "add_payment_info"))
        self.assertEqual((s.drop.segments[0].dimension, s.drop.segments[0].value), ("app_version", "2.4.0"))
        self.assertIn("2.4.0", s.explanation.text)

        # the release regression is detected as an anomaly starting exactly on the release day
        release = self.truth["planted_anomaly"]["release_date_utc"]
        self.assertEqual(s.drop.anomaly_started_on, release)
        flagged = {(a["step_name"], a["session_date"]) for a in s.anomalies}
        self.assertIn(("add_payment_info", release), flagged)
        self.assertTrue(all(step == "add_payment_info" and day >= release for step, day in flagged), flagged)

        # schema drift: exactly the undeclared fields the generator added, first seen in the last file
        drift = {d["field"]: d["first_seen_file"] for d in s.schema_drift}
        self.assertEqual(sorted(drift), self.truth["schema_drift"]["fields"])
        self.assertEqual(set(drift.values()), {self.truth["schema_drift"]["file"]})
        kinds = sorted(a.kind for a in s.new_alerts)
        self.assertEqual(kinds, ["funnel_conversion_drop", "schema_drift", "schema_drift"])

        report = json.loads((self.cfg.output_dir / "report.json").read_text())
        self.assertEqual(report["largest_drop"]["to_step"], "add_payment_info")
        self.assertTrue((self.cfg.output_dir / "report.html").stat().st_size > 5000)

    def test_rerun_is_a_no_op(self):
        self.run_pipeline()
        before = self.snapshot()
        raw_before = self.one("SELECT COUNT(*) FROM raw_events")
        s = self.run_pipeline()
        self.assertEqual(s.status, "no_new_data")
        self.assertEqual(s.new_alerts, [])                       # same problems are not alerted twice
        self.assertEqual(self.one("SELECT COUNT(*) FROM raw_events"), raw_before)
        self.assertEqual(self.snapshot(), before)

    def test_day_by_day_incremental_equals_full_rebuild(self):
        # deliver one file per run, like a daily job, into a fresh warehouse
        staged = self.tmp / "staged"
        staged.mkdir()
        for name in self.files:
            (self.incoming / name).rename(staged / name)
        for name in self.files:
            (staged / name).rename(self.incoming / name)
            s = self.run_pipeline()
            self.assertEqual(s.status, "success")
            self.assertTrue(s.quality["all_checks_passed"], s.quality["checks"])
        incremental = self.snapshot()

        self.run_pipeline(full_refresh=True)
        self.assertEqual(self.snapshot(), incremental)
