"""AI layer: interface, providers (against a local fake HTTP server), guardrails,
fallback, caching and secret handling. No real network calls are made."""
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from funnel.ai import DropContext, ExplanationService, FunnelStepSummary, RuleBasedExplainer, SegmentFinding, build_service
from funnel.ai.base import ExplainerError
from funnel.ai.guardrails import validate
from funnel.ai.providers import OllamaExplainer, OpenAICompatibleExplainer
from tests.helpers import PipelineTestCase, ev

CTX = DropContext(
    funnel_name="checkout", from_step="begin_checkout", to_step="add_payment_info", step_index=4,
    sessions_prev=2701, sessions_reached=1354, drop_off_sessions=1347, drop_off_rate=0.498704,
    overall_conversion=0.164504, median_seconds_from_prev=51.0, session_gap_minutes=30,
    funnel=(FunnelStepSummary("view_product", 6705, None), FunnelStepSummary("add_payment_info", 1354, 0.501296)),
    segments=(SegmentFinding("device", "mobile", 1450, 313, 0.215862, 0.832134, 893, 1e-200),),
)


class FakeLLM:
    name, model = "fake", "fake-1"

    def __init__(self, text=None, error=None):
        self.text, self.error, self.calls = text, error, 0

    def explain(self, ctx):
        self.calls += 1
        if self.error:
            raise self.error
        return self.text


class TestGuardrails(unittest.TestCase):
    def test_grounded_answer_passes(self):
        text = ("Sessions drop 49.9% at add_payment_info (1,347 of 2,701 lost). Mobile continues at 21.6% "
                "vs 83.2% elsewhere, about 893 sessions, p < 0.001.")
        self.assertTrue(validate(text, CTX).ok, validate(text, CTX).problems)

    def test_invented_numbers_are_rejected(self):
        r = validate("add_payment_info lost 67% of 5,000 sessions after a 12% price rise.", CTX)
        self.assertFalse(r.ok)
        self.assertEqual(r.unverified_numbers, ["67%", "5,000", "12%"])

    def test_must_name_the_step_and_not_be_empty(self):
        self.assertFalse(validate("", CTX).ok)
        self.assertFalse(validate("Something dropped by 49.9%.", CTX).ok)

    def test_rule_based_output_always_passes_its_own_guardrails(self):
        self.assertTrue(validate(RuleBasedExplainer().explain(CTX), CTX).ok)


class TestService(unittest.TestCase):
    def test_success_path(self):
        exp = ExplanationService(FakeLLM("add_payment_info drops 49.9%.")).explain(CTX)
        self.assertEqual((exp.provider, exp.status, exp.is_ai_generated), ("fake", "ok", True))

    def test_provider_error_falls_back(self):
        exp = ExplanationService(FakeLLM(error=ExplainerError("timeout"))).explain(CTX)
        self.assertEqual((exp.provider, exp.status), ("rule_based", "fallback"))
        self.assertIn("timeout", exp.warnings[0])

    def test_provider_bug_falls_back(self):
        exp = ExplanationService(FakeLLM(error=ZeroDivisionError("bug"))).explain(CTX)
        self.assertEqual(exp.status, "fallback")

    def test_hallucination_falls_back(self):
        exp = ExplanationService(FakeLLM("add_payment_info fell 99.9% for 77,777 users")).explain(CTX)
        self.assertEqual(exp.provider, "rule_based")
        self.assertIn("77,777", exp.warnings[0])

    def test_factory_selection_and_secret_hygiene(self):
        self.assertEqual(build_service({}).provider, "rule_based")                     # offline default
        groq = build_service({"GROQ_API_KEY": "gsk_supersecretvalue123"})
        self.assertEqual((groq.provider, groq.model), ("groq", "llama-3.3-70b-versatile"))
        self.assertNotIn("supersecret", repr(groq.primary))
        missing = build_service({"AI_PROVIDER": "groq"})
        self.assertEqual(missing.provider, "rule_based")
        self.assertTrue(missing.setup_warnings)
        self.assertEqual(build_service({"AI_PROVIDER": "ollama"}).provider, "ollama")

    def test_prompt_contains_only_aggregates(self):
        payload = json.loads(CTX.to_prompt_json())
        self.assertNotIn("user_id", json.dumps(payload))
        self.assertEqual(payload["largest_drop"]["drop_off_pct"], 49.9)


class _Handler(BaseHTTPRequestHandler):
    received: list = []

    def do_POST(self):  
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        _Handler.received.append((self.path, dict(self.headers), body))
        if self.path.endswith("/chat/completions"):
            out = {"choices": [{"message": {"content": "add_payment_info lost 49.9% of sessions."}}]}
        else:
            out = {"message": {"content": "add_payment_info lost 1,347 sessions."}}
        data = json.dumps(out).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass


class TestHttpProviders(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), _Handler)
        cls.url = f"http://127.0.0.1:{cls.server.server_port}"
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def test_openai_compatible_request_and_parse(self):
        p = OpenAICompatibleExplainer(name="groq", base_url=self.url + "/v1", model="m1", api_key="k123")
        self.assertIn("49.9%", p.explain(CTX))
        path, headers, body = _Handler.received[-1]
        self.assertEqual(path, "/v1/chat/completions")
        self.assertEqual(headers["Authorization"], "Bearer k123")
        self.assertEqual(body["model"], "m1")
        self.assertEqual(body["messages"][0]["role"], "system")

    def test_ollama_request_and_parse(self):
        self.assertIn("1,347", OllamaExplainer(base_url=self.url, model="llama3.2").explain(CTX))
        self.assertEqual(_Handler.received[-1][0], "/api/chat")

    def test_unreachable_server_raises_explainer_error(self):
        with self.assertRaises(ExplainerError):
            OllamaExplainer(base_url="http://127.0.0.1:9", timeout=1).explain(CTX)


class TestExplanationCache(PipelineTestCase):
    def test_unchanged_data_does_not_call_the_model_again(self):
        events = []
        for i in range(40):
            events += [ev(f"{i}a", "view_product", 0, user=f"u{i}"), ev(f"{i}b", "add_to_cart", 60, user=f"u{i}")][: 1 + i % 2]
        self.write(events)
        llm = FakeLLM("No session reached begin_checkout: a 100.0% drop after add_to_cart.")
        service = ExplanationService(llm)
        first = self.run_pipeline(service=service)
        second = self.run_pipeline(service=service)
        self.assertEqual((first.explanation.status, second.explanation.status), ("ok", "cached"))
        self.assertEqual(llm.calls, 1)


class TestGuardrailsWithTrend(unittest.TestCase):
    def test_known_dates_and_versions_pass_invented_dates_fail(self):
        import dataclasses
        from funnel.ai import DailyPoint
        ctx = dataclasses.replace(
            CTX, trend=(DailyPoint("2026-10-08", 751, 0.85486), DailyPoint("2026-10-09", 666, 0.397898)),
            anomaly_started_on="2026-10-09", baseline_conversion=0.83815, anomaly_conversion=0.417,
            segments=(SegmentFinding("app_version", "2.4.0", 1158, 142, 0.123, 0.84, 831, 1e-200),))
        text = ("add_payment_info conversion fell from 83.8% to 41.7% starting 2026-10-09; "
                "app_version 2.4.0 continues at 12.3% vs 84.0% (about 831 sessions lost).")
        self.assertTrue(validate(text, ctx).ok, validate(text, ctx).problems)
        bad = validate(text.replace("2026-10-09", "2026-10-03"), ctx)
        self.assertFalse(bad.ok)
        self.assertIn("2026-10-03", bad.problems[0])
