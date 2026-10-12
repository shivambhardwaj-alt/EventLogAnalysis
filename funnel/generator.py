"""Synthetic event-log generator (seeded, reproducible, no real user data).

It simulates realistic shopping sessions and then deliberately damages the
data the way real pipelines see it:
  * client retries          -> exact duplicates (same batch and next batch)
  * double clicks           -> near-duplicates within ~1s
  * producer bugs           -> same event_id with different content
  * late / offline uploads  -> events arriving in the next day's file
  * out-of-order lines, mixed timestamp formats (ISO Z, +05:30 offsets,
    epoch millis as number and as string), messy event names, prices as
    strings, device aliases, lower-case / missing countries
  * invalid rows: malformed JSON, non-object JSON, missing ids, bad / future /
    ancient timestamps, unknown event types, a broken CSV row
It also plants two realistic incidents:
  * a release regression: app version 2.4.0 ships on day 5 (by default) and
    breaks the payment step for everyone who upgrades (mobile + tablet apps);
  * schema drift: on the last day a producer starts sending undeclared fields.
The exact counts of everything injected go to a ground-truth file, so tests
can prove the pipeline finds precisely what was planted.
"""
from __future__ import annotations

import copy
import csv
import json
import logging
import random
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

FUNNEL = ["view_product", "add_to_cart", "begin_checkout", "add_payment_info", "purchase"]
# probability of continuing to FUNNEL[i] from FUNNEL[i-1]
TRANSITIONS: dict[int, dict[str, float]] = {
    1: {"default": 0.76},
    2: {"default": 0.80},
    3: {"2.4.0": 0.12, "default": 0.85},   # <- planted bug: payment step broken in app release 2.4.0
    4: {"default": 0.85},
}
BUGGY_VERSION, OLD_VERSION, WEB_VERSION = "2.4.0", "2.3.1", "web"
UPGRADE_SHARE = 0.9          # share of app users who install the new release once it ships
DRIFT_FIELDS = {"experiment_id": "exp_checkout_v2"}   # a producer adds an undeclared field...
DRIFT_PROPERTY = ("coupon_code", "SAVE10")            # ...and an undeclared property
DEVICES = [("mobile", 0.55), ("desktop", 0.35), ("tablet", 0.10)]
CHANNELS = [("organic", 0.40), ("paid_search", 0.25), ("social", 0.20), ("email", 0.10), ("referral", 0.05)]
COUNTRIES = [("IN", 0.45), ("US", 0.20), ("GB", 0.10), ("DE", 0.10), ("BR", 0.08), ("JP", 0.07)]
MESSY_NAMES = {
    "add_to_cart": ["Add To Cart", " ADD_TO_CART ", "add-to-cart"],
    "view_product": ["View Product", "VIEW_PRODUCT", "view-product"],
    "begin_checkout": ["Begin Checkout", "BEGIN_CHECKOUT"],
    "purchase": ["Purchase", " purchase"],
    "app_open": ["App Open", "APP_OPEN"],
}
DEVICE_ALIASES = {"mobile": ["iOS", "Android", "MOBILE"], "desktop": ["web", "Desktop"], "tablet": ["iPad", "Tablet"]}

DAY = 86400.0


@dataclass
class GeneratorSettings:
    users: int = 5000
    days: int = 7
    seed: int = 42
    release_day: int | None = None  # day the buggy app release ships (default: days - 2)
    start: datetime | None = None   # UTC midnight of day 1; default = `days` days before today
    inject_issues: bool = True
    csv_partner_share: float = 0.03


def _pick(rng: random.Random, weighted: list[tuple[str, float]]) -> str:
    return rng.choices([v for v, _ in weighted], weights=[w for _, w in weighted], k=1)[0]


def _iso(epoch: float, offset_minutes: int = 0) -> str:
    tz = timezone(timedelta(minutes=offset_minutes))
    s = datetime.fromtimestamp(epoch, tz).isoformat(timespec="milliseconds")
    return s.replace("+00:00", "Z")


class _Sim:
    def __init__(self, s: GeneratorSettings, now: float) -> None:
        self.s = s
        self.rng = random.Random(s.seed)
        self.now = now
        start = s.start or (datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                            - timedelta(days=s.days))
        self.start = start.replace(tzinfo=timezone.utc).timestamp() if start.tzinfo is None else start.timestamp()
        rd = s.release_day if s.release_day is not None else max(1, s.days - 2)
        self.release_day = min(max(1, rd), s.days)
        self.release_epoch = self.start + (self.release_day - 1) * DAY
        self.products = [(f"p_{i:03d}", round(self.rng.uniform(99, 4999), 2)) for i in range(60)]

    def _id(self) -> str:
        return f"{self.rng.getrandbits(64):016x}"

    def _event(self, user: dict, name: str, t: float, product: tuple[str, float] | None = None) -> dict:
        props: dict[str, Any] = {}
        if product:
            props = {"product_id": product[0], "price": product[1]}
        return {"event_id": self._id(), "user_id": user["user_id"], "event_name": name, "ts": round(t, 3),
                "device": user["device"], "country": user["country"], "channel": user["channel"],
                "app_version": user["app_version"], "properties": props}

    def _session(self, user: dict, t: float) -> list[dict]:
        rng = self.rng
        evs = [self._event(user, "app_open", t)]

        def advance(mean: float = 45.0) -> None:
            nonlocal t
            if rng.random() < 0.06:                       # long-but-legal idle inside a session
                t += rng.uniform(15 * 60, 29 * 60)
            else:
                t += min(3.0 + rng.expovariate(1 / mean), 25 * 60)

        if rng.random() < 0.5:
            advance(); evs.append(self._event(user, "search", t))
        if rng.random() < 0.15:                           # browse-only session
            advance(); evs.append(self._event(user, "page_view", t))
        else:
            product = None
            for _ in range(rng.choice([1, 1, 1, 2, 2, 3])):
                product = rng.choice(self.products)
                advance(); evs.append(self._event(user, "view_product", t, product))
            reached = 0
            while reached < len(FUNNEL) - 1:
                probs = TRANSITIONS[reached + 1]
                if rng.random() >= probs.get(user["app_version"], probs["default"]):
                    break
                reached += 1
                advance(70); evs.append(self._event(user, FUNNEL[reached], t, product))
            if reached >= 1 and rng.random() < 0.05:
                advance(); evs.append(self._event(user, "remove_from_cart", t, product))
        if rng.random() < 0.3:
            advance(20); evs.append(self._event(user, "logout", t))
        return evs

    def events(self) -> list[dict]:
        rng, out = self.rng, []
        window_end = self.start + self.s.days * DAY - 3 * 3600
        for i in range(self.s.users):
            user = {"user_id": f"u_{i:05d}", "device": _pick(rng, DEVICES),
                    "country": _pick(rng, COUNTRIES), "channel": _pick(rng, CHANNELS)}
            upgrades = rng.random() < UPGRADE_SHARE
            starts = sorted(rng.uniform(self.start, window_end) for _ in range(rng.choice([1, 1, 1, 2, 2, 3, 4])))
            last_end = -1e18
            for st in starts:
                if st < last_end + 45 * 60:               # keep real sessions clearly apart
                    st = last_end + rng.uniform(45 * 60, 3 * 3600)
                if st > window_end:
                    break
                if user["device"] == "desktop":
                    user["app_version"] = WEB_VERSION
                else:
                    user["app_version"] = BUGGY_VERSION if (upgrades and st >= self.release_epoch) else OLD_VERSION
                sess = self._session(user, st)
                last_end = sess[-1]["ts"]
                out.extend(sess)
        for e in out:
            e["_batch"] = min(int((e["ts"] - self.start) // DAY), self.s.days - 1)
        return out


def _format_variants(rng: random.Random, e: dict) -> dict:
    """Valid-but-messy representations. The pipeline must normalise all of these."""
    r = rng.random()
    if r < 0.06:
        e["ts"] = int(round(e["ts"] * 1000))                    # epoch millis (number)
    elif r < 0.07:
        e["ts"] = str(int(round(e["ts"] * 1000)))               # epoch millis (string)
    elif r < 0.10:
        e["ts"] = _iso(e["ts"], 330)                            # local time with +05:30
    else:
        e["ts"] = _iso(e["ts"])                                 # ISO-8601 Z
    if rng.random() < 0.04 and e["event_name"] in MESSY_NAMES:
        e["event_name"] = rng.choice(MESSY_NAMES[e["event_name"]])
    if rng.random() < 0.05 and "price" in e["properties"]:
        e["properties"]["price"] = f"{e['properties']['price']:.2f}"
    r = rng.random()
    if r < 0.02:
        e["device"] = rng.choice(DEVICE_ALIASES[e["device"]])
    elif r < 0.025:
        e["device"] = None
    r = rng.random()
    if r < 0.02:
        e["country"] = e["country"].lower()
    elif r < 0.025:
        e["country"] = None
    if rng.random() < 0.005:
        e["channel"] = None
    return e


_CORRUPTIONS = [  # (reason the pipeline must assign, probability)
    ("missing_user_id", 0.004),
    ("invalid_timestamp", 0.003),
    ("timestamp_in_future", 0.002),
    ("timestamp_too_old", 0.001),
    ("unknown_event_type", 0.004),
    ("missing_event_id", 0.001),
    ("malformed_json", 0.003),
    ("not_a_json_object", 0.0005),
]


def _corrupt(rng: random.Random, e: dict, reason: str, now: float) -> str:
    """Return the raw line for a corrupted event."""
    if reason == "missing_user_id":
        e["user_id"] = rng.choice([None, "", "null", "None"])
    elif reason == "invalid_timestamp":
        e["ts"] = rng.choice(["not-a-date", "2026-13-45T99:00:00Z", "", "yesterday"])
    elif reason == "timestamp_in_future":
        e["ts"] = _iso(now + rng.uniform(2, 5) * DAY)
    elif reason == "timestamp_too_old":
        e["ts"] = "1999-12-31T23:59:59Z"
    elif reason == "unknown_event_type":
        e["event_name"] = rng.choice(["wishlist_add", "share_product", "chat_open"])
    elif reason == "missing_event_id":
        e.pop("event_id")
    line = json.dumps(e)
    if reason == "malformed_json":
        line = line[: rng.randint(10, max(11, len(line) - 5))]
    elif reason == "not_a_json_object":
        line = json.dumps([e.get("event_id"), e.get("event_name")])
    return line


def generate(out_dir: Path, settings: GeneratorSettings | None = None, now: float | None = None,
             truth_path: Path | None = None) -> dict[str, Any]:
    s = settings or GeneratorSettings()
    now = now or time.time()
    sim = _Sim(s, now)
    rng = sim.rng
    events = sim.events()
    truth: Counter[str] = Counter()
    truth["clean_events_simulated"] = len(events)

    lines: dict[int, list[str]] = {b: [] for b in range(s.days)}
    csv_rows: list[dict] = []

    protected: set[int] = set()
    if s.inject_issues:
        # 1) double clicks -> near duplicates
        extra = []
        for e in events:
            if e["event_name"] in ("add_to_cart", "view_product") and rng.random() < 0.02:
                dup = copy.deepcopy(e)
                dup["event_id"] = sim._id()
                dup["ts"] = round(e["ts"] + rng.uniform(0.2, 1.5), 3)
                protected.update({id(e), id(dup)})
                extra.append(dup)
                truth["near_duplicates"] += 1
        events.extend(extra)

    valid: list[dict] = []
    for e in events:
        if s.inject_issues and id(e) not in protected:
            r, acc, chosen = rng.random(), 0.0, None
            for reason, p in _CORRUPTIONS:
                acc += p
                if r < acc:
                    chosen = reason
                    break
            if chosen:
                truth[f"quarantine:{chosen}"] += 1
                b = e.pop("_batch")
                lines[b].append(_corrupt(rng, e, chosen, now))
                continue
        valid.append(e)

    for e in valid:
        batch = e.pop("_batch")
        is_protected = id(e) in protected
        _format_variants(rng, e)
        if s.inject_issues and batch == s.days - 1 and s.days > 1:   # schema drift on the last day
            if rng.random() < 0.4:
                e.update(DRIFT_FIELDS)
            if e["event_name"] == "add_payment_info" and rng.random() < 0.5:
                e["properties"][DRIFT_PROPERTY[0]] = DRIFT_PROPERTY[1]
        line = json.dumps(e)
        if not s.inject_issues:
            lines[batch].append(line)
            continue
        r = rng.random()
        if r < 0.025:                                      # client retry -> exact duplicate
            lines[batch].append(line)
            target = batch + 1 if (rng.random() < 0.4 and batch + 1 < s.days) else batch
            lines[target].append(line)
            truth["exact_duplicates"] += 1
            truth["cross_batch_duplicates"] += int(target != batch)
        elif r < 0.026 and not is_protected:               # producer bug -> conflicting duplicate
            lines[batch].append(line)
            bad = copy.deepcopy(e)
            bad["ts"] = _iso(_to_epoch(e["ts"]) + rng.uniform(30, 300))
            lines[batch].append(json.dumps(bad))
            truth["conflicting_duplicates"] += 1
        elif r < 0.066 and batch + 1 < s.days:             # offline upload -> arrives a day late
            lines[batch + 1].append(line)
            truth["late_arrivals"] += 1
        elif (r < 0.066 + s.csv_partner_share and batch == 0 and not is_protected
              and e["device"] == "desktop"):
            csv_rows.append(e)                             # same event, legacy CSV source
            truth["csv_rows"] += 1
        else:
            lines[batch].append(line)

    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for b, blines in lines.items():
        rng.shuffle(blines)                                # out-of-order arrival
        if s.inject_issues and blines:
            blines.insert(rng.randrange(len(blines)), "")  # stray blank lines
            truth["blank_lines"] += 1
        path = out_dir / f"events_day_{b + 1:02d}.jsonl"
        path.write_text("\n".join(blines) + "\n", encoding="utf-8")
        written.append(path.name)

    if csv_rows:
        path = out_dir / "partner_export_day_01.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["event_id", "user_id", "event_name", "ts", "device", "country", "channel",
                        "app_version", "product_id", "price"])
            for i, e in enumerate(csv_rows):
                p = e.get("properties", {})
                w.writerow([e["event_id"], e["user_id"], e["event_name"], e["ts"], e["device"] or "",
                            e["country"] or "", e["channel"] or "", e.get("app_version") or "",
                            p.get("product_id", ""), p.get("price", "")])
                if i == len(csv_rows) // 2:
                    w.writerow(["broken", "row", "with", "too", "few"])        # wrong column count
                    truth["quarantine:csv_column_count_mismatch"] += 1
        written.append(path.name)

    result = {
        "seed": s.seed, "users": s.users, "days": s.days,
        "start_utc": _iso(sim.start), "generated_at_utc": _iso(now),
        "files": written,
        "planted_anomaly": {
            "step": "begin_checkout -> add_payment_info",
            "segment": f"app_version = {BUGGY_VERSION} (mobile + tablet apps)",
            "release_day": sim.release_day,
            "release_date_utc": _iso(sim.release_epoch)[:10],
            "continue_probability": TRANSITIONS[3],
        },
        "schema_drift": {"fields": sorted(DRIFT_FIELDS) + [f"properties.{DRIFT_PROPERTY[0]}"],
                         "file": f"events_day_{s.days:02d}.jsonl" if s.inject_issues and s.days > 1 else None},
        "injected": dict(sorted(truth.items())),
    }
    if truth_path:
        truth_path.parent.mkdir(parents=True, exist_ok=True)
        truth_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    log.info("generated %s into %s", written, out_dir)
    return result


def _to_epoch(ts: Any) -> float:
    if isinstance(ts, (int, float)):
        return ts / 1000 if ts > 1e11 else float(ts)
    if isinstance(ts, str) and ts.isdigit():
        return int(ts) / 1000
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
