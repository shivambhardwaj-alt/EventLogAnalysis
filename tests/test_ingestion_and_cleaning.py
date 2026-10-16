"""Ingestion (bronze) + validation / type coercion (silver)."""
import json

from tests.helpers import PipelineTestCase, ev, ts


class TestIngestion(PipelineTestCase):
    def test_same_file_is_ingested_only_once(self):
        self.write([ev("a", "view_product", 0)], name="day1.jsonl")
        first = self.run_pipeline()
        second = self.run_pipeline()
        self.assertEqual(first.ingest.rows_ingested, 1)
        self.assertEqual(second.ingest.files_ingested, 0)
        self.assertEqual(second.status, "no_new_data")
        self.assertEqual(self.one("SELECT COUNT(*) FROM raw_events"), 1)

    def test_renamed_copy_is_skipped_but_changed_content_is_loaded(self):
        p = self.write([ev("a", "view_product", 0)], name="day1.jsonl")
        self.run_pipeline()
        (self.incoming / "day1_copy.jsonl").write_bytes(p.read_bytes())
        self.assertEqual(self.run_pipeline().ingest.files_ingested, 0)
        self.write([ev("a", "view_product", 0), ev("b", "add_to_cart", 10)], name="day1.jsonl")  # same name, new content
        s = self.run_pipeline()
        self.assertEqual(s.ingest.files_ingested, 1)
        self.assertEqual(s.exact_duplicates, 1)       # "a" again -> duplicate, not double counted
        self.assertEqual(self.one("SELECT COUNT(*) FROM clean_events"), 2)

    def test_csv_values_are_typed_like_json(self):
        (self.incoming / "partner.csv").write_text(
            "event_id,user_id,event_name,ts,device,country,channel,product_id,price\n"
            f"c1,42,Add To Cart,{ts(0)},Android,in,,p9,19.90\n"
            "c2,u2,view_product,1772445600000,web,US,email,p9,\n"
            "broken,row\n", encoding="utf-8")
        self.run_pipeline()
        rows = {r["event_id"]: r for r in self.q("SELECT * FROM clean_events")}
        self.assertEqual(rows["c1"]["user_id"], "42")
        self.assertEqual(rows["c1"]["event_name"], "add_to_cart")
        self.assertEqual(rows["c1"]["device"], "mobile")
        self.assertEqual(rows["c1"]["country"], "IN")
        self.assertEqual(rows["c1"]["channel"], "unknown")
        self.assertAlmostEqual(rows["c1"]["price"], 19.9)
        self.assertEqual(rows["c2"]["event_ts"], "2026-03-02T10:00:00.000Z")
        self.assertIsNone(rows["c2"]["price"])
        self.assertEqual(self.one("SELECT reason FROM quarantine_events"), "csv_column_count_mismatch")

    def test_empty_and_unsupported_files(self):
        (self.incoming / "empty.jsonl").write_text("", encoding="utf-8")
        (self.incoming / "readme.txt").write_text("hi", encoding="utf-8")
        s = self.run_pipeline()
        self.assertEqual(s.ingest.empty_files, ["empty.jsonl"])
        self.assertEqual(s.ingest.unsupported_files, ["readme.txt"])
        self.assertEqual(s.status, "no_new_data")
        self.assertIsNone(s.drop)


class TestValidation(PipelineTestCase):
    def test_each_invalid_row_gets_a_precise_reason(self):
        good = ev("ok", "view_product", 0)
        bad = {
            "malformed_json": '{"event_id": "x", "user_',
            "not_a_json_object": '["x", 1]',
            "missing_event_id": json.dumps({k: v for k, v in ev("_", "search", 0).items() if k != "event_id"}),
            "missing_user_id": json.dumps(ev("u", "search", 0, user="  ")),
            "missing_event_name": json.dumps(ev("n", "  ", 0)),
            "unknown_event_type": json.dumps(ev("t", "teleport", 0)),
            "invalid_timestamp": json.dumps(ev("i", "search", "31/02/2026")),
            "timestamp_too_old": json.dumps(ev("o", "search", "2001-01-01T00:00:00Z")),
            "timestamp_in_future": json.dumps(ev("f", "search", "2999-01-01T00:00:00Z")),
        }
        self.write([good, *bad.values()])
        s = self.run_pipeline()
        got = sorted(r["reason"] for r in self.q("SELECT reason FROM quarantine_events"))
        self.assertEqual(got, sorted(bad))
        self.assertEqual(s.rows_accepted, 1)
        
        self.assertEqual(self.one("SELECT raw_payload FROM quarantine_events WHERE reason='malformed_json'"),
                         bad["malformed_json"])

    def test_timestamp_formats_normalise_to_the_same_utc_instant(self):
        self.write([
            ev("iso", "search", "2026-03-02T10:00:00Z"),
            ev("offset", "search", "2026-03-02T15:30:00+05:30", user="u2"),
            ev("millis", "search", 1772445600000, user="u3"),
            ev("millis_str", "search", "1772445600000", user="u4"),
            ev("seconds", "search", 1772445600, user="u5"),
        ])
        self.run_pipeline()
        values = {r[0] for r in self.q("SELECT event_ts FROM clean_events")}
        self.assertEqual(values, {"2026-03-02T10:00:00.000Z"})

    def test_cleaning_of_categoricals_and_numbers(self):
        self.write([ev("a", " ADD-TO-CART ", 0, device="iOS", country="gb", channel="  ",
                       properties={"product_id": 77, "price": "-5"})])
        self.run_pipeline()
        r = self.q("SELECT * FROM clean_events")[0]
        self.assertEqual((r["event_name"], r["device"], r["country"], r["channel"]),
                         ("add_to_cart", "mobile", "GB", "unknown"))
        self.assertEqual(r["product_id"], "77")       
        self.assertIsNone(r["price"])                 
