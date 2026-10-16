
from __future__ import annotations

import json
import logging
import sqlite3
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse

from .config import SEGMENT_COLUMNS, Config
from .db import sql_blocks

log = logging.getLogger(__name__)
WEB_DIR = Path(__file__).parent / "web"
FILTERS = ("device", "channel", "country", "app_version", "date_from", "date_to")


class ApiError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class Api:
    """Pure request -> JSON logic, kept apart from HTTP so it is easy to test."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.queries = sql_blocks("12_api_queries.sql")

    def _con(self) -> sqlite3.Connection:
        if not self.cfg.warehouse_path.exists():
            raise ApiError(HTTPStatus.SERVICE_UNAVAILABLE, "No warehouse yet - run `python -m funnel run` first.")
        con = sqlite3.connect(f"file:{self.cfg.warehouse_path}?mode=ro", uri=True, timeout=5)
        con.row_factory = sqlite3.Row
        return con

    def _rows(self, sql: str, params: Any = ()) -> list[dict[str, Any]]:
        con = self._con()
        try:
            return [dict(r) for r in con.execute(sql, params)]
        finally:
            con.close()

    def _report_json(self) -> dict[str, Any]:
        path = self.cfg.output_dir / "report.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}

    # ---------------------------------------------------------- endpoints
    def health(self, q: dict) -> dict:
        last = self._rows("SELECT run_id, status, finished_at FROM pipeline_runs ORDER BY started_at DESC LIMIT 1")
        return {"status": "ok", "last_run": last[0] if last else None}

    def dimensions(self, q: dict) -> dict:
        out = {}
        for col in sorted(SEGMENT_COLUMNS):
            out[col] = [r["v"] for r in self._rows(
                f"SELECT {col} AS v, COUNT(*) n FROM sessions GROUP BY {col} ORDER BY n DESC LIMIT 50")]
        dates = self._rows("SELECT MIN(session_date) lo, MAX(session_date) hi FROM sessions")[0]
        out["date_range"] = [dates["lo"], dates["hi"]]
        return out

    def funnel(self, q: dict) -> dict:
        params = {k: (q.get(k) or None) for k in FILTERS}
        for k in ("date_from", "date_to"):
            v = params[k]
            if v is not None and (len(v) != 10 or v[4] != "-" or v[7] != "-"):
                raise ApiError(HTTPStatus.BAD_REQUEST, f"{k} must be YYYY-MM-DD")
        rows = self._rows(self.queries["filtered_funnel"], params)
        return {"filters": {k: v for k, v in params.items() if v}, "sessions_in_filter":
                rows[0]["sessions_in_filter"] if rows else 0,
                "steps": [{k: v for k, v in r.items() if k != "sessions_in_filter"} for r in rows]}

    def daily(self, q: dict) -> dict:
        return {"rows": self._rows("SELECT * FROM daily_funnel ORDER BY session_date, step_index")}

    def anomalies(self, q: dict) -> dict:
        return {"rows": self._rows("SELECT * FROM funnel_anomalies ORDER BY session_date, step_index")}

    def segments(self, q: dict) -> dict:
        try:
            step = int(q.get("step") or 0)
        except ValueError:
            raise ApiError(HTTPStatus.BAD_REQUEST, "step must be an integer") from None
        if not step:
            worst = self._rows("SELECT step_index FROM funnel_metrics WHERE step_index > 1 "
                               "ORDER BY drop_off_rate DESC LIMIT 1")
            step = worst[0]["step_index"] if worst else 2
        return {"step_index": step, "rows": self._rows(
            "SELECT dimension, segment_value, prev_sessions, sessions_reached, step_conversion "
            "FROM funnel_segments WHERE step_index = ? AND prev_sessions > 0 "
            "ORDER BY dimension, prev_sessions DESC", (step,))}

    def alerts(self, q: dict) -> dict:
        return {"rows": self._rows("SELECT severity, kind, message, created_at, delivered FROM alerts "
                                   "ORDER BY created_at DESC, rowid DESC LIMIT 50")}

    def quality(self, q: dict) -> dict:
        return self._report_json().get("data_quality", {})

    def runs(self, q: dict) -> dict:
        runs = self._rows("SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 30")
        tasks = self._rows("SELECT task, status, duration_ms, rows_affected FROM task_runs WHERE run_id = ? "
                           "ORDER BY seq", (runs[0]["run_id"],)) if runs else []
        return {"runs": runs, "latest_tasks": tasks}

    def explanation(self, q: dict) -> dict:
        r = self._report_json()
        return {"largest_drop": r.get("largest_drop"), "explanation": r.get("explanation"),
                "overview": r.get("overview")}

    def user(self, user_id: str) -> dict:
        user_id = user_id.strip()
        if not user_id or len(user_id) > 128:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid user id")
        sessions = self._rows("SELECT * FROM sessions WHERE user_id = ? ORDER BY start_epoch", (user_id,))
        if not sessions:
            raise ApiError(HTTPStatus.NOT_FOUND, f"user {user_id!r} not found")
        events = self._rows(self.queries["user_timeline"], {"user_id": user_id})
        steps = self._rows("SELECT session_id, step_index, step_name FROM funnel_session_steps WHERE user_id = ? "
                           "ORDER BY session_id, step_index", (user_id,))
        return {"user_id": user_id, "sessions": sessions, "events": events, "funnel_steps": steps}

    def routes(self) -> dict[str, Callable[[dict], dict]]:
        return {"/api/health": self.health, "/api/dimensions": self.dimensions, "/api/funnel": self.funnel,
                "/api/daily": self.daily, "/api/anomalies": self.anomalies, "/api/segments": self.segments,
                "/api/alerts": self.alerts, "/api/quality": self.quality, "/api/runs": self.runs,
                "/api/explanation": self.explanation}


def make_handler(api: Api) -> type[BaseHTTPRequestHandler]:
    routes = api.routes()

    class Handler(BaseHTTPRequestHandler):
        server_version = "FunnelDashboard/2.0"

        def _send(self, status: int, body: bytes, ctype: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, payload: Any) -> None:
            self._send(status, json.dumps(payload, default=str).encode("utf-8"), "application/json; charset=utf-8")

        def do_GET(self) -> None:  # noqa: N802
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            try:
                if url.path in ("/", "/index.html"):
                    self._send(200, (WEB_DIR / "dashboard.html").read_bytes(), "text/html; charset=utf-8")
                elif url.path == "/report":
                    path = api.cfg.output_dir / "report.html"
                    if not path.exists():
                        raise ApiError(HTTPStatus.NOT_FOUND, "no report yet - run the pipeline first")
                    self._send(200, path.read_bytes(), "text/html; charset=utf-8")
                elif url.path.startswith("/api/users/"):
                    self._json(200, api.user(unquote(url.path[len("/api/users/"):])))
                elif url.path in routes:
                    self._json(200, routes[url.path](q))
                else:
                    raise ApiError(HTTPStatus.NOT_FOUND, f"no route {url.path}")
            except ApiError as exc:
                self._json(exc.status, {"error": str(exc)})
            except sqlite3.Error as exc:
                log.exception("database error")
                self._json(500, {"error": f"database error: {exc}"})

        def log_message(self, fmt: str, *args: Any) -> None:
            log.debug("%s %s", self.address_string(), fmt % args)

    return Handler


def make_server(cfg: Config, host: str = "127.0.0.1", port: int = 8050) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(Api(cfg)))


def serve(cfg: Config, host: str = "127.0.0.1", port: int = 8050) -> None:
    httpd = make_server(cfg, host, port)
    print(f"Dashboard: http://{host}:{httpd.server_port}   (API at /api/..., Ctrl+C to stop)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        httpd.server_close()
