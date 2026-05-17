#!/usr/bin/env python3
"""hierarchy — PLATO room for agent status from TE transition matrix.

Two data sources:
1. coordination-history tiles (for timeline)
2. coordination_topology.CoordinationState (for full transition matrix)

Status = out_connections / (in_connections + out_connections + 1)
"""
import json, time, urllib.request, os, sys
from collections import defaultdict

PLATO_URL = os.environ.get("PLATO_URL", "http://localhost:8847")
HISTORY_ROOM = "coordination-history"
HIERARCHY_ROOM = "coordination-hierarchy"
PUBLISH_INTERVAL = 45

def fetch_history_tiles():
    try:
        resp = urllib.request.urlopen(f"{PLATO_URL}/room/{HISTORY_ROOM}", timeout=10)
        return json.loads(resp.read()).get("tiles", [])
    except: return []

def compute_status_from_matrix(tiles):
    """Extract agent interactions from tile sources and build hierarchy."""
    # Track source sequences to infer who-follows-whom
    transitions = defaultdict(lambda: defaultdict(int))
    source_counts = defaultdict(int)
    sources_seen = set()
    
    for t in tiles:
        src = t.get("source", "unknown")
        sources_seen.add(src)
        source_counts[src] += 1
    
    # Build implied transition matrix from source ordering
    source_seq = [t.get("source", "unknown") for t in tiles if t.get("source")]
    for i in range(len(source_seq) - 1):
        a, b = source_seq[i], source_seq[i+1]
        if a != b:  # self-transitions don't count
            transitions[a][b] += 1
    
    if not transitions:
        return []
    
    # Compute status for each source
    results = []
    all_sources = set(transitions.keys())
    for s in transitions:
        all_sources.add(s)
    for s in transitions.values():
        for k in s:
            all_sources.add(k)
    
    for agent in all_sources:
        out_deg = len(transitions.get(agent, {}))  # number of unique sources this agent transitions TO
        in_deg = sum(1 for s in transitions if agent in transitions[s])  # number of sources that transition TO this agent
        total = out_deg + in_deg + 1
        status = out_deg / total  # normalized to 0-1
        results.append({
            "agent": agent,
            "status": round(status, 4),
            "out_degree": out_deg,
            "in_degree": in_deg,
            "tile_count": source_counts.get(agent, 0)
        })
    
    results.sort(key=lambda x: -x["status"])
    for i, r in enumerate(results):
        r["rank"] = i + 1
    
    return results

def compute_alliance_clusters(hierarchy, threshold=0.3):
    """Detect agent coalitions: agents with similar status form alliances."""
    clusters = []
    if len(hierarchy) < 2:
        return clusters
    
    # Simple clustering: group agents by status proximity
    sorted_agents = sorted(hierarchy, key=lambda x: -x["status"])
    current_cluster = [sorted_agents[0]]
    
    for i in range(1, len(sorted_agents)):
        gap = sorted_agents[i-1]["status"] - sorted_agents[i]["status"]
        if gap < threshold:
            current_cluster.append(sorted_agents[i])
        else:
            clusters.append(current_cluster)
            current_cluster = [sorted_agents[i]]
    clusters.append(current_cluster)
    
    return [{"rank_start": c[0]["rank"], "rank_end": c[-1]["rank"], "agents": [x["agent"] for x in c], "size": len(c), "mean_status": round(sum(x["status"] for x in c)/len(c), 4)} for c in clusters]

def publish(hierarchy, clusters):
    if not hierarchy:
        return "no_data"
    
    top = hierarchy[0]
    transitions = {h["agent"]: {"status": h["status"], "rank": h["rank"], "out": h["out_degree"], "in": h["in_degree"]} for h in hierarchy}
    
    tile = {
        "top_agent": top["agent"],
        "top_status": top["status"],
        "total_agents": len(hierarchy),
        "hierarchy_stability": "stable" if len(hierarchy) > 1 else "forming",
        "structure": "dominance" if top["out_degree"] > top["in_degree"] else "submission",
        "coalitions": len(clusters),
        "coalition_detail": clusters[:5] if clusters else [],
        "agent_states": transitions,
    }
    
    try:
        payload = json.dumps({
            "domain": HIERARCHY_ROOM,
            "question": f"hierarchy at {time.time():.0f}",
            "answer": json.dumps(tile),
            "tags": ["hierarchy", "coordination", "live"],
            "source": "oracle1",
            "confidence": 0.85
        }).encode()
        req = urllib.request.Request(f"{PLATO_URL}/submit", data=payload, headers={"Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req, timeout=5).read()).get("status")
    except Exception as e:
        return f"err:{e}"

def main():
    print(f"hierarchy room v2 — full transition matrix")
    print(f"  History: {HISTORY_ROOM} → Hierarchy: {HIERARCHY_ROOM} [{PUBLISH_INTERVAL}s]")
    tick = 0
    while True:
        tiles = fetch_history_tiles()
        hierarchy = compute_status_from_matrix(tiles)
        clusters = compute_alliance_clusters(hierarchy) if hierarchy else []
        
        if hierarchy:
            status = publish(hierarchy, clusters)
            tick += 1
            top_str = f"{hierarchy[0]['agent']}(S={hierarchy[0]['status']})"
            bot_str = f"{hierarchy[-1]['agent']}(S={hierarchy[-1]['status']})" if len(hierarchy) > 1 else ""
            print(f"  [{tick}] {len(hierarchy)} agents | {top_str} | {bot_str} | coalitions={len(clusters)} | {status}")
        else:
            print(f"  [{tick}] Waiting for multi-agent data ({len(tiles)} tiles)")
        time.sleep(PUBLISH_INTERVAL)

if __name__ == "__main__":
    main()
