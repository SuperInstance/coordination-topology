#!/usr/bin/env python3
"""coordination-history — SAFE edition v2.
Circuit breakers, memory limits, persistence, SELF-HEALING warmup.
"""
import time, json, urllib.request, os, sys, random
from collections import defaultdict
sys.path.insert(0, os.path.dirname(__file__))
from coordination_topology import CoordinationState, running_source_entropy, running_transfer_entropy, running_iat_autocorrelation, running_euler_characteristic

PLATO_URL = os.environ.get("PLATO_URL", "http://localhost:8847")
ROOM = "coordination-history"
PUBLISH_INTERVAL = 30
STATE_FILE = "/tmp/coordination-state.json"

# Circuit breakers
TE_COLLAPSE = 0.1
SAFE_TRIGGER = 5
ALERT_COOLDOWN = 300
MAX_TRANSITIONS = 10000
QUARANTINE = 3

CS = {"low_te_count": 0, "safe_mode": False, "last_alert": 0, "quarantined": set(), "zero_counts": defaultdict(int)}

def save_state(state):
    try:
        d = {
            "sources": {s: {"chain": list(b.chain_sequence)[-500:], "iats": list(b.iat_sequence)[-200:], "ts": list(b.timestamps)[-500:], "last_ts": b.last_timestamp, "transitions": dict(b.transition_counts)} for s,b in state.sources.items()},
            "fleet_transitions": {str(k):v for k,v in state.fleet_transitions.items()},
            "source_seq": list(state.source_sequence)[-1000:],
            "circuit": {"low": CS["low_te_count"], "safe": CS["safe_mode"], "q": list(CS["quarantined"])},
            "ts": time.time()
        }
        with open(STATE_FILE+".tmp","w") as f: json.dump(d,f)
        os.rename(STATE_FILE+".tmp", STATE_FILE)
    except: pass

def fetch_room(room, max_tiles=5000):
    try:
        resp = urllib.request.urlopen(f"{PLATO_URL}/room/{room}", timeout=30)
        data = json.loads(resp.read())
        tiles = data.get("tiles",[])
        if len(tiles) > max_tiles:
            step = len(tiles)//max_tiles
            tiles = [tiles[i] for i in range(0,len(tiles),step)][:max_tiles]
        return tiles
    except: return []

def main():
    state = CoordinationState()
    
    # WARMUP: coordination-history first (self-healing), then other rooms
    print("Warming up...")
    for room in [ROOM] + ["fleet-coord", "research_log", "flux-engine"]:
        tiles = fetch_room(room)
        print(f"  {room}: {len(tiles)} tiles")
        for t in tiles:
            state.ingest(t.get("source","?"), t.get("provenance",{}).get("chain_size",0), t.get("provenance",{}).get("timestamp",time.time()))
    
    print(f"State: {len(state.sources)} sources, {len(state.fleet_transitions)} transitions, TE={running_transfer_entropy(state):.4f}")
    
    # Null model
    seq = list(state.source_sequence); shuffled = seq.copy(); random.shuffle(shuffled)
    ns = CoordinationState()
    for s in shuffled: ns.ingest(s,0,0)
    null_te = running_transfer_entropy(ns)
    print(f"Null model: {null_te:.4f}")
    
    last_save = 0; tick = 0
    while True:
        te = running_transfer_entropy(state)
        ent = running_source_entropy(state)
        euler = running_euler_characteristic(state)
        iat = running_iat_autocorrelation(state)
        
        # Circuit breaker
        if te < TE_COLLAPSE:
            CS["low_te_count"] += 1
        else:
            CS["low_te_count"] = 0
        if CS["low_te_count"] >= SAFE_TRIGGER and not CS["safe_mode"]:
            CS["safe_mode"] = True
            print("  ⚠️ CIRCUIT: SAFE MODE ENTERED")
        elif CS["safe_mode"] and te >= TE_COLLAPSE:
            CS["safe_mode"] = False
            CS["low_te_count"] = 0
            print("  ✅ CIRCUIT: SAFE MODE EXITED")
        
        # Quarantine
        if te < 0.01:
            CS["zero_counts"]["oracle1"] += 1
            if CS["zero_counts"]["oracle1"] >= QUARANTINE:
                CS["quarantined"].add("oracle1")
        else:
            CS["zero_counts"]["oracle1"] = 0
            CS["quarantined"].discard("oracle1")
        
        # Memory
        if len(state.fleet_transitions) > MAX_TRANSITIONS:
            for k in list(state.fleet_transitions.keys())[:len(state.fleet_transitions)-MAX_TRANSITIONS]:
                del state.fleet_transitions[k]
        
        # Publish
        health = "safe_mode" if CS["safe_mode"] else ("collapsed" if te < 0.1 else ("degraded" if te < 0.15 else "healthy"))
        ans = f"TE={te:.4f} H={ent:.4f} src={len(state.sources)} chi={euler['chi']} {health}"
        try:
            payload = json.dumps({"domain":ROOM,"question":f"tick {int(time.time())}","answer":ans,"tags":["history","live"],"source":"oracle1","confidence":0.85}).encode()
            req = urllib.request.Request(f"{PLATO_URL}/submit", data=payload, headers={"Content-Type":"application/json"})
            resp = json.loads(urllib.request.urlopen(req, timeout=5).read())
            status = resp.get("status")
        except Exception as e:
            status = f"err:{e}"
        
        tick += 1
        now = time.time()
        if now - last_save > 60:
            save_state(state)
            last_save = now
            if tick % 10 == 0:
                print(f"  [{tick}] TE={te:.4f} src={len(state.sources)} χ={euler['chi']} {'⚡SAFE' if CS['safe_mode'] else '✓'} → {status}")
        
        # Refresh from PLATO
        try:
            for t in fetch_room("fleet-coord", max_tiles=50):
                state.ingest(t.get("source","?"), t.get("provenance",{}).get("chain_size",0), t.get("provenance",{}).get("timestamp",time.time()))
        except: pass
        
        time.sleep(PUBLISH_INTERVAL)

if __name__ == "__main__":
    main()
