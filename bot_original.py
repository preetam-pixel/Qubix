import json
import os
import time
from collections import defaultdict, deque

import requests
from kafka import KafkaConsumer

_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env):
    for _line in open(_env, encoding="utf-8"):
        if "=" in _line and not _line.lstrip().startswith("#"):
            _k, _v = _line.strip().split("=", 1)
            os.environ.setdefault(_k, _v)

GATEWAY = os.environ.get("GATEWAY", "http://192.168.65.157:3002")
BROKER = os.environ.get("BROKER", "192.168.65.157:9094")
USERNAME = os.environ.get("HACKATHON_USERNAME", "mn2631")
TOKEN = os.environ.get("BOT_ACCESS_TOKEN")
if not TOKEN:
    raise SystemExit("Set BOT_ACCESS_TOKEN first")

headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

NOTIONAL = 50000      # rupees per trade; flat 20 brokerage makes small trades wasteful
MAX_POS_VALUE = 100000
SMA_N = 8
BAND = 0.015          # 1.5% from the moving average triggers a trade
POS_WORDS = ("beat", "record", "growth", "surge", "upgrade", "profit", "wins", "expands", "raises", "strong")
NEG_WORDS = ("miss", "loss", "plunge", "downgrade", "probe", "lawsuit", "recall", "cuts", "weak", "fraud", "bankruptcy")

closes = defaultdict(lambda: deque(maxlen=SMA_N))
held = defaultdict(int)
tradable = None
live = False


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def order(ticker, action, qty):
    if tradable is not None and ticker not in tradable:
        return
    try:
        r = requests.post(f"{GATEWAY}/api/v1/orders", headers=headers,
                          json={"ticker": ticker, "action": action, "quantity": qty}, timeout=5)
        log("ORDER", action, qty, ticker, r.status_code, r.text[:200])
        if r.ok:
            held[ticker] += qty if action == "BUY" else -qty
    except requests.RequestException as e:
        log("order error", e)


def on_tick(ticker, d):
    c = d.get("close")
    if c is None:
        return
    q = closes[ticker]
    if len(q) == SMA_N:
        sma = sum(q) / SMA_N
        qty = max(1, int(NOTIONAL // c))
        if c > sma * (1 + BAND) and (held[ticker] + qty) * c <= MAX_POS_VALUE:
            order(ticker, "BUY", qty)
        elif c < sma * (1 - BAND) and held[ticker] > 0:
            order(ticker, "SELL", held[ticker])
    q.append(c)


def on_news(d):
    ticker = d.get("ticker")
    text = f"{d.get('headline','')} {d.get('description','')}".lower()
    pos = sum(w in text for w in POS_WORDS)
    neg = sum(w in text for w in NEG_WORDS)
    log("NEWS", ticker, d.get("headline"), f"(+{pos}/-{neg})")


consumer = KafkaConsumer(
    "market.ticks", "market.news",
    bootstrap_servers=[BROKER],
    group_id=f"group-{USERNAME}",
    auto_offset_reset="latest",
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)
try:
    pf = requests.get(f"{GATEWAY}/api/v1/portfolio", headers=headers, timeout=5).json()
    for h in pf.get("holdings", []):
        held[h["ticker"]] = h["quantity"] if not h.get("is_short") else 0
    log("Synced holdings:", dict(held))
except Exception as e:
    log("portfolio sync failed", e)
log("Connected to Kafka. Listening...")

for message in consumer:
    ev = message.value
    et = ev.get("event_type") or ev.get("event")
    if et == "preflight_tick":
        if tradable is None:
            try:
                s = requests.get(f"{GATEWAY}/api/v1/stocks", headers=headers, timeout=5)
                s.raise_for_status()
                tradable = {x["ticker"] for x in s.json()["stocks"]}
                log("Universe:", sorted(tradable))
            except requests.RequestException as e:
                log("stocks error", e)
        try:
            requests.post(f"{GATEWAY}/api/v1/bot/heartbeat", headers=headers,
                          json={"seq": ev["seq"]}, timeout=2).raise_for_status()
            log("heartbeat ack", ev["seq"])
        except requests.RequestException as e:
            log("heartbeat error", e)
    elif message.topic == "market.ticks" and et == "price_tick":
        d = ev.get("data") or {}
        log("TICK", ev.get("simulated_date"), d.get("ticker"), d.get("close"))
        on_tick(d.get("ticker"), d)
    elif message.topic == "market.news":
        on_news(ev.get("data") or {})
