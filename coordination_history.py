#!/usr/bin/env python3
"""coordination-history — persists rolling coordination metrics to PLATO.

Publishes to localhost:8847/room/coordination-history every 30 seconds.
The night wheel and any agent can query this room to test hypotheses.

Schema per tile:
{
    "timestamp": float,
    "si_te_bits": float,          # transfer entropy
    "lag1_autocorrelation": float,  # CSD-τ
    "euler_characteristic": int,   # SC-χ
    "euler_v": int,                # chain bins
    "euler_e": int,                # multi-source bins
    "source_entropy": float,       # Shannon H
    "active_sources": int,         # sources seen in window
    "te_matrix": {...},            # all pairwise TE values
    "spike_events": [...],         # significant TE spikes
    "null_multiplier": float,      # TE / shuffled null
    "coordination_health": str,    # healthy / degraded / collapsed
    "top_sources": [...],          # top 5 sources by activity
}
"""
import time, json, urllib.request, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from coordination_topology import CoordinationState, running_source_entropy, running_transfer_entropy, running_iat_autocorrelation, running_euler_characteristic

PLATO_URL = os.environ.get("PLATO_URL", "http://localhost:8847")
ROOM = "coordination-history"
PUBLISH_INTERVAL = 30  # seconds

# Replay existing data to warm up state
WARMUP_ROOMS = ["fleet-coord", "research_log", "flux-engine"]

def fetch_room(room, max_tiles=5000):
    try:
        resp = urllib.request.urlopen(f"{PLATO_URL}/room/{room}", timeout=30)
        data = json.loads(resp.read())
        tiles = data.get("tiles", [])
        if len(tiles) > max_tiles:
            step = len(tiles) // max_tiles
            tiles = [tiles[i] for i in range(0, len(tiles), step)][:max_tiles]
        return tiles
    except:
        return []

def publish_tile(state, te, ent, euler, iat, null_te):
    health = "healthy"
    if te < 0.1: health = "collapsed"
    elif te < 0.15: health = "degraded"
    
    spike_events = []
    for sid, info in iat.items():
        if abs(info.get("lag1_autocorr", 0)) > 0.8:
            spike_events.append({
                "source": sid,
                "type": "high_autocorr",
                "value": info["lag1_autocorr"],
                "mean_iat": info["mean_iat"]
            })
    
    tile = {
        "timestamp": time.time(),
        "si_te_bits": round(te, 4),
        "lag1_autocorrelation": round(next((v["lag1_autocorr"] for v in iat.values()), 0), 4),
        "euler_characteristic": euler["chi"],
        "euler_v": euler["V"],
        "euler_e": euler["E"],
        "source_entropy": round(ent, 4),
        "active_sources": len(state.sources),
        "te_matrix": {str(k): round(v, 4) for k, v in state.fleet_transitions.items()},
        "spike_events": spike_events[:5],
        "null_multiplier": round(te / null_te, 1) if null_te > 0 else 0,
        "coordination_health": health,
        "top_sources": sorted(
            [(sid, len(buf.timestamps)) for sid, buf in state.sources.items()],
            key=lambda x: -x[1]
        )[:5],
    }
    
    try:
        payload = json.dumps({
            "domain": ROOM,
            "question": f"coordination tick at {time.time():.0f}",
            "answer": json.dumps(tile),
            "tags": ["coordination-history", "live"],
            "source": "oracle1",
            "confidence": 0.9
        }).encode()
        req = urllib.request.Request(
            f"{PLATO_URL}/submit",
            data=payload,
            headers={"Content-Type": "application/json"}
        )
        resp = json.loads(urllib.request.urlopen(req, timeout=5).read())
        return resp.get("status")
    except Exception as e:
        return f"err:{e}"

def build_te_matrix(state):
    """Build pairwise TE-like weights from transition matrix."""
    matrix = {}
    total = sum(state.fleet_transitions.values())
    if total == 0: return matrix
    for (a, b), count in state.fleet_transitions.items():
        key = f"{a}→{b}"
        matrix[key] = count / total
    return dict(sorted(matrix.items(), key=lambda x: -x[1])[:20])

def detect_spikes(state):
    """Detect sources with unusually high transition probability."""
    spikes = []
    src_counts = {}
    for (a, b), count in state.fleet_transitions.items():
        src_counts[a] = src_counts.get(a, 0) + count
    mean = (sum(src_counts.values()) / len(src_counts)) if src_counts else 0
    std = (sum((c - mean)**2 for c in src_counts.values()) / len(src_counts))**0.5 if src_counts else 0
    for src, count in src_counts.items():
        if std > 0 and count > mean + 2 * std:
            spikes.append({"source": src, "count": count, "z_score": round((count - mean) / std, 2)})
    return sorted(spikes, key=lambda x: -x["z_score"])[:5]

def main():
    print(f"coordination-history publisher starting...")
    print(f"  PLATO: {PLATO_URL}")
    print(f"  Room: {ROOM}")
    print(f"  Interval: {PUBLISH_INTERVAL}s")
    
    state = CoordinationState()
    
    # Warmup: replay existing data
    print(f"\nWarming up from {len(WARMUP_ROOMS)} rooms...")
    for room in WARMUP_ROOMS:
        tiles = fetch_room(room)
        print(f"  {room}: {len(tiles)} tiles")
        for t in tiles:
            source = t.get("source", "unknown")
            chain = t.get("provenance", {}).get("chain_size", 0)
            ts = t.get("provenance", {}).get("timestamp", time.time())
            state.ingest(source, chain, ts)
    
    print(f"\nState: {len(state.sources)} sources, {len(state.fleet_transitions)} transitions")
    
    # Compute null model once
    import random
    seq = list(state.source_sequence)
    shuffled = seq.copy()
    random.shuffle(shuffled)
    null_state = CoordinationState()
    for source in shuffled:
        null_state.ingest(source, 0, 0)
    null_te = running_transfer_entropy(null_state)
    print(f"  Null model TE: {null_te:.4f}")
    
    # Publish loop
    tick = 0
    while True:
        te = running_transfer_entropy(state)
        ent = running_source_entropy(state)
        euler = running_euler_characteristic(state)
        iat = running_iat_autocorrelation(state)
        
        status = publish_tile(state, te, ent, euler, iat, null_te)
        tick += 1
        
        print(f"  [{tick}] TE={te:.4f} H={ent:.4f} χ={euler['chi']} → {status}")
        
        # Also listen for new tiles
        new_tiles = fetch_room("fleet-coord", max_tiles=100)
        for t in new_tiles:
            source = t.get("source", "unknown")
            chain = t.get("provenance", {}).get("chain_size", 0)
            ts = t.get("provenance", {}).get("timestamp", time.time())
            state.ingest(source, chain, ts)
        
        time.sleep(PUBLISH_INTERVAL)

if __name__ == "__main__":
    main()
