#!/usr/bin/env python3
"""Topology Dashboard — ASCII visualization of coordination topology room."""
import sys, json, urllib.request
from collections import defaultdict

PLATO_URL = "http://localhost:8847"
TOPOLOGY_ROOM = "coordination-topology"

def fetch_room(room):
    try:
        resp = urllib.request.urlopen(f"{PLATO_URL}/room/{room}", timeout=10)
        return json.loads(resp.read()).get("tiles", [])
    except:
        return []

def ascii_table(rows, headers):
    if not rows:
        return "[no data]"
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    fmt = " | ".join(f"{{:<{w}}}" for w in col_widths)
    sep = "-|-".join("-" * w for w in col_widths)
    lines = [fmt.format(*headers), sep]
    for row in rows:
        lines.append(fmt.format(*[str(c) for c in row]))
    return "
".join(lines)

def sparkline(values, width=20):
    if not values:
        return "[empty]"
    mn, mx = min(values), max(values)
    if mx == mn:
        return "▁" * width
    bars = "▁▂▃▄▅▆▇█"
    result = []
    for v in values:
        idx = int((v - mn) / (mx - mn) * (len(bars) - 1))
        result.append(bars[min(idx, len(bars) - 1)])
    return "".join(result)

def main():
    tiles = fetch_room(TOPOLOGY_ROOM)
    print("=" * 60)
    print("COORDINATION TOPOLOGY DASHBOARD")
    print(f"Tiles: {len(tiles)}")
    print("=" * 60)
    
    if not tiles:
        print("Room not populated yet. Trying fleet-coord directly...")
        tiles = fetch_room("fleet-coord")
        print(f"fleet-coord: {len(tiles)} tiles")
    
    sources = defaultdict(int)
    for t in tiles:
        sources[t.get("source", "?")] += 1
    
    print(f"
SOURCE DISTRIBUTION (top 6)")
    top = sorted(sources.items(), key=lambda x: -x[1])[:6]
    total = sum(sources.values())
    rows = [[s, str(c), f"{c/total*100:.1f}%", sparkline([c])] for s, c in top]
    print(ascii_table(rows, ["Source", "Tiles", "Pct", "Dist"]))
    
    if total > 10:
        print(f"
TE TRANSITION MATRIX (P(source_B | source_A))
")
        names = [s for s, _ in top]
        transition = defaultdict(lambda: defaultdict(int))
        seq = [t.get("source", "?") for t in tiles if t.get("source")]
        for i in range(len(seq) - 1):
            transition[seq[i]][seq[i + 1]] += 1
        
        rows = []
        for a in names:
            row = [a[:12]]
            total_a = sum(transition[a].values()) or 1
            for b in names:
                p = transition[a][b] / total_a
                row.append(f"{p:.2f}" if p > 0.01 else "-")
            rows.append(row)
        print(ascii_table(rows, ["From\To"] + [n[:8] for n in names]))
    
    print(f"
TIMING SUMMARY")
    timestamps = [t.get("provenance", {}).get("timestamp", 0) for t in tiles if t.get("provenance")]
    if timestamps:
        iats = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
        mean_iat = sum(iats) / len(iats) if iats else 0
        print(f"  Mean inter-arrival: {mean_iat:.1f}s")
        print(f"  Tile rate: {1/mean_iat:.2f}/s" if mean_iat > 0 else "")
        print(f"  IAT sparkline: {sparkline(iats[:100])}")

if __name__ == "__main__":
    main()
