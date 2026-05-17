#!/usr/bin/env python3
"""Second publisher for coordination-history — creates cross-agent transitions."""
import json, time, urllib.request, os

PLATO_URL = os.environ.get("PLATO_URL", "http://localhost:8847")
ROOM = "coordination-history"
AGENT = "hierarchy-feeder2"
INTERVAL = 15

def publish(te, sources, chi, health):
    tile = {"te": round(te, 4), "sources": sources, "chi": chi, "health": health}
    try:
        payload = json.dumps({
            "domain": ROOM,
            "question": f"coordination tick from {AGENT}",
            "answer": json.dumps(tile),
            "tags": ["coordination-history", "hierarchy-feeder2"],
            "source": AGENT,
            "confidence": 0.85
        }).encode()
        req = urllib.request.Request(f"{PLATO_URL}/submit", data=payload, headers={"Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req, timeout=5).read()).get("status")
    except Exception as e:
        return f"err:{e}"

tick = 0
while True:
    status = publish(1.2 + (tick % 5) * 0.1, 43, 110 + tick, "healthy")
    tick += 1
    print(f"  [{tick}] feeder → {status}")
    time.sleep(INTERVAL)
