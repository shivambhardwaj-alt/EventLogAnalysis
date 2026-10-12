"""Funnel metrics: strict ordering, conversion math, segments, largest drop."""
from funnel.metrics import two_proportion_p_value
from tests.helpers import PipelineTestCase, ev

STEPS = ["view_product", "add_to_cart", "begin_checkout", "add_payment_info", "purchase"]


def session(user: str, depth: int, start: float = 0, device: str = "desktop") -> list[dict]:
    """A session that reaches the first `depth` funnel steps."""
    return [ev(f"{user}-{i}", STEPS[i], start + 60 * i, user=user, device=device) for i in range(depth)]


class TestFunnel(PipelineTestCase):
    def test_conversion_math_matches_hand_calculation(self):
        # 10 sessions view, 8 add to cart, 6 checkout, 3 payment, 3 purchase
        depths = [5, 5, 5, 4, 4, 4, 3, 2, 2, 1]
        events = [e for i, d in enumerate(depths) for e in session(f"u{i}", d)]
        self.write(events)
        s = self.run_pipeline()
        rows = {r["step_name"]: r for r in self.q("SELECT * FROM funnel_metrics")}
        self.assertEqual([rows[s]["sessions_reached"] for s in STEPS], [10, 9, 7, 6, 3])
        self.assertAlmostEqual(rows["purchase"]["step_conversion"], 0.5)
        self.assertAlmostEqual(rows["purchase"]["overall_conversion"], 0.3)
        self.assertEqual(rows["add_to_cart"]["median_seconds_from_prev"], 60.0)
        self.assertEqual(s.drop.to_step, "purchase")             # 50% drop is the biggest
        self.assertEqual(s.drop.drop_off_sessions, 3)

    def test_steps_out_of_order_or_in_other_sessions_do_not_count(self):
        self.write([
            ev("a1", "add_to_cart", 0, user="a"), ev("a2", "view_product", 60, user="a"),   # cart before view
            ev("b1", "view_product", 0, user="b"), ev("b2", "add_to_cart", 3 * 3600, user="b"),  # next session
            ev("c1", "view_product", 0, user="c"), ev("c2", "add_to_cart", 60, user="c"),   # proper
        ])
        self.run_pipeline()
        reached = {r[0]: r[1] for r in self.q("SELECT step_name, sessions_reached FROM funnel_metrics")}
        self.assertEqual(reached["view_product"], 3)
        self.assertEqual(reached["add_to_cart"], 1)

    def test_no_data_does_not_crash_or_divide_by_zero(self):
        self.write([ev("x", "search", 0)])
        s = self.run_pipeline()
        rows = self.q("SELECT sessions_reached, step_conversion FROM funnel_metrics")
        self.assertTrue(all(r[0] == 0 for r in rows))
        self.assertTrue(all(r[1] is None for r in rows))
        self.assertIsNone(s.drop)

    def test_largest_drop_is_attributed_to_the_right_segment(self):
        events = []
        for i in range(60):   # mobile breaks at payment, desktop does not
            events += session(f"m{i}", 3 if i % 5 else 4, device="mobile")
            events += session(f"d{i}", 5 if i % 5 else 3, device="desktop")
        self.write(events)
        s = self.run_pipeline(self.cfg.replace(min_sample_size=20))
        self.assertEqual(s.drop.to_step, "add_payment_info")
        top = s.drop.segments[0]
        self.assertEqual((top.dimension, top.value), ("device", "mobile"))
        self.assertLess(top.p_value, 0.001)

    def test_two_proportion_test(self):
        self.assertAlmostEqual(two_proportion_p_value(50, 100, 50, 100), 1.0)
        self.assertLess(two_proportion_p_value(10, 100, 60, 100), 1e-6)
        self.assertEqual(two_proportion_p_value(0, 0, 5, 10), 1.0)
