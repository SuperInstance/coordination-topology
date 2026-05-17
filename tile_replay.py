#!/usr/bin/env python3
"""Tile Replay Validator — replays PLATO data through coordination topology algorithms."""

import sys, json, time, urllib.request, random
sys.path.insert(0, '/home/ubuntu/.openclaw/workspace/services/coordination-topology')
from coordination_topology import CoordinationState, running_source_entropy, running_transfer_entropy, running_iat_autocorrelation, running_euler_characteristic

PLATO_URL = "http://localhost:8847"
ROOMS = ["fleet-coord", "research_log", "flux-engine"]

def fetch_all_tiles():
    tiles = []
    for room in ROOMS:
        try:
            resp = urllib.request.urlopen(f"{PLATO_URL}/room/{room}", timeout=30)
            data = json.loads(resp.read())
            room_tiles = data.get("tiles", [])
            print(f"  {room}: {len(room_tiles)} tiles")
            tiles.extend(room_tiles)
        except Exception as e:
            print(f"  {room}: ERROR {e}")
    tiles.sort(key=lambda t: t.get("provenance", {}).get("timestamp", 0))
    print(f"  Total: {len(tiles)} tiles sorted by timestamp")
    return tiles

def main():
    state = CoordinationState()
    tiles = fetch_all_tiles()

    for i, tile in enumerate(tiles):
        source = tile.get("source", "unknown")
        chain = tile.get("provenance", {}).get("chain_size", 0)
        ts = tile.get("provenance", {}).get("timestamp", time.time())
        state.ingest(source, chain, ts)

        if (i + 1) % 1000 == 0:
            te = running_transfer_entropy(state)
            ent = running_source_entropy(state)
            euler = running_euler_characteristic(state)
            iat = running_iat_autocorrelation(state)
            print(f"  Tile {i+1}: TE={te:.4f} bits, H={ent:.4f} bits, chi={euler['chi']}, sources={len(state.sources)}")

    print()
    print("=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    te = running_transfer_entropy(state)
    ent = running_source_entropy(state)
    euler = running_euler_characteristic(state)
    iat = running_iat_autocorrelation(state)
    print(f"  Transfer Entropy: {te:.4f} bits  (baseline: 0.229 bits)")
    print(f"  Source Entropy: {ent:.4f} bits")
    print(f"  Euler Characteristic: V={euler['V']}, E={euler['E']}, chi={euler['chi']}")
    print(f"  Sources tracked: {len(state.sources)}")
    print(f"  Transitions: {len(state.fleet_transitions)}")

    print()
    print("TOP SOURCES BY IAT AUTOCORRELATION:")
    sorted_iat = sorted(iat.items(), key=lambda x: -abs(x[1].get('lag1_autocorr', 0)))[:5]
    for sid, info in sorted_iat:
        print(f"  {sid:25s}: lag1={info['lag1_autocorr']:.4f}, mean_iat={info['mean_iat']:.1f}s, cv={info['cv']:.2f}")

    # Null model
    seq = list(state.source_sequence)
    shuffled = seq.copy()
    random.shuffle(shuffled)
    null_state = CoordinationState()
    for source in shuffled:
        null_state.ingest(source, 0, 0)
    null_te = running_transfer_entropy(null_state)
    null_str = f"{te/null_te:.1f}x" if null_te > 0 else "infinite (null=0)"
    print(f"\n  NULL MODEL (shuffled): TE={null_te:.4f} bits")
    print(f"  SIGNAL/NOISE RATIO: {null_str}")

    delta = abs(te - 0.229)
    print(f"\n  BASELINE VALIDATION: TE={te:.4f} vs expected 0.229 (delta={delta:.4f})")
    if delta < 0.05:
        print("  VALIDATED: TE within 0.05 bits of DeepSeek baseline")
    elif delta < 0.15:
        print(f"  PARTIAL MATCH: TE within {delta:.4f} bits")
    else:
        print(f"  MISMATCH: TE off by {delta:.4f} bits")

if __name__ == "__main__":
    main()
