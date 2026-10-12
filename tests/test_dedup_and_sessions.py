"""Duplicate handling and sessionization, including incremental edge cases."""
from tests.helpers import PipelineTestCase, ev

GAP = 30 * 60


class TestDeduplication(PipelineTestCase):
    def test_exact_duplicate_in_same_batch(self):
        e = ev("a", "view_product", 0)
        self.write([e, e, e])
        s = self.run_pipeline()
        self.assertEqual((s.rows_accepted, s.exact_duplicates), (1, 2))

    def test_duplicate_arriving_in_a_later_batch(self):
        self.write([ev("a", "view_product", 0)])
        self.run_pipeline()
        self.write([ev("a", "view_product", 0), ev("b", "add_to_cart", 30)])
        s = self.run_pipeline()
        self.assertEqual((s.rows_accepted, s.exact_duplicates), (1, 1))
        self.assertEqual(self.one("SELECT COUNT(*) FROM clean_events"), 2)

    def test_conflicting_duplicate_keeps_first_arrival(self):
        self.write([ev("a", "view_product", 0), ev("a", "view_product", 99)])
        s = self.run_pipeline()
        self.assertEqual(s.conflicting_duplicates, 1)
        self.assertEqual(self.one("SELECT event_ts FROM clean_events"), "2026-03-02T10:00:00.000Z")

    def test_double_click_is_flagged_but_genuine_repeat_is_kept(self):
        self.write([
            ev("a1", "add_to_cart", 0),
            ev("a2", "add_to_cart", 0.8),   # double click (new id!) -> near duplicate
            ev("a3", "add_to_cart", 6),     # deliberate second click -> real event
            ev("b1", "add_to_cart", 1, properties={"product_id": "OTHER"}),  # other product -> real
        ])
        self.run_pipeline()
        flagged = {r[0] for r in self.q("SELECT event_id FROM clean_events WHERE is_near_duplicate = 1")}
        self.assertEqual(flagged, {"a2"})
        self.assertEqual(self.one("SELECT event_count FROM sessions"), 3)


class TestSessionization(PipelineTestCase):
    def test_gap_boundary_exactly_30_minutes_is_same_session(self):
        self.write([ev("a", "search", 0), ev("b", "search", GAP),            # exactly 30:00 -> same
                    ev("c", "search", GAP + GAP + 1)])                        # 30:01 later -> new
        self.run_pipeline()
        counts = [r[0] for r in self.q("SELECT event_count FROM sessions ORDER BY start_epoch")]
        self.assertEqual(counts, [2, 1])

    def test_out_of_order_lines_are_sessionized_by_event_time(self):
        self.write([ev("c", "search", 600), ev("a", "search", 0), ev("d", "search", 5000), ev("b", "search", 300)])
        self.run_pipeline()
        rows = self.q("SELECT event_count, entry_event FROM sessions ORDER BY start_epoch")
        self.assertEqual([r[0] for r in rows], [3, 1])

    def test_late_event_merges_two_sessions_on_incremental_run(self):
        self.write([ev("a", "search", 0), ev("c", "search", 50 * 60)])      # 50 min apart -> 2 sessions
        self.run_pipeline()
        self.assertEqual(self.one("SELECT COUNT(*) FROM sessions"), 2)
        self.write([ev("b", "search", 25 * 60), ev("x", "search", 0, user="other")])
        s = self.run_pipeline()
        self.assertEqual(s.users_recomputed, 2)
        self.assertEqual(self.one("SELECT COUNT(*) FROM sessions WHERE user_id='u1'"), 1)
        self.assertEqual(self.one("SELECT event_count FROM sessions WHERE user_id='u1'"), 3)

    def test_session_ids_are_stable_across_reruns_and_full_refresh(self):
        self.write([ev("a", "search", 0), ev("b", "search", 4000), ev("c", "search", 0, user="u2")])
        self.run_pipeline()
        before = [tuple(r) for r in self.q("SELECT * FROM sessions ORDER BY session_id")]
        self.run_pipeline(full_refresh=True)
        after = [tuple(r) for r in self.q("SELECT * FROM sessions ORDER BY session_id")]
        self.assertEqual(before, after)

    def test_changing_the_gap_rebuilds_automatically(self):
        self.write([ev("a", "search", 0), ev("b", "search", 20 * 60)])
        self.run_pipeline()
        self.assertEqual(self.one("SELECT COUNT(*) FROM sessions"), 1)
        s = self.run_pipeline(self.cfg.replace(inactivity_gap_seconds=10 * 60))
        self.assertEqual(s.mode, "full_refresh")
        self.assertEqual(self.one("SELECT COUNT(*) FROM sessions"), 2)
