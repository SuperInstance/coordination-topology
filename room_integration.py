#!/usr/bin/env python3
"""
coordination-topology PLATO Room Integration
Computes live Transfer Entropy, IAT autocorrelation, and source transition matrices.

Phases 3-4:
- Publish pipeline: publish_coordination_tile(), timer loop (30s interval), trigger conditions
- Connect algorithms to live PLATO stream at localhost:8847
- Room HTTP endpoints: POST /submit and GET /room/coordination-topology
- SI-TE computation and fleet CSD computation
- Null model comparison (shuffle source labels, recompute TE, compare)
"""

import json
import time
import math
import random
import threading
from collections import deque, Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import urllib.request
import urllib.error

# ─── CONFIGURATION ───

PLATO_BASE_URL = "http://localhost:8847"
ROOM_NAME = "coordination-topology"
CHAIN_BIN_WIDTH = 100
PUBLISH_INTERVAL = 30.0  # seconds

# ─── DATA STRUCTURES ───


@dataclass
class SourceBuffer:
    """Per-source rolling buffer for coordination metrics."""

    source_id: str
    room_id: str

    # Chain-size sequence — monotonic counter per room
    chain_sequence: deque = field(default_factory=lambda: deque(maxlen=2000))

    # Inter-arrival times in seconds
    iat_sequence: deque = field(default_factory=lambda: deque(maxlen=500))

    # Source activity timestamps (unix float)
    timestamps: deque = field(default_factory=lambda: deque(maxlen=2000))

    # Derived: source entropy window (last N events)
    entropy_window: deque = field(default_factory=lambda: deque(maxlen=100))

    # Last update time (for IAT calculation)
    last_timestamp: float = 0.0

    # Cumulative chain_size (for chain binning)
    chain_head: int = 0


@dataclass
class CoordinationState:
    """Global coordination state across all sources."""

    # All known sources across all rooms
    sources: Dict[str, SourceBuffer] = field(default_factory=dict)

    # Room-level aggregations
    room_te: Dict[str, float] = field(default_factory=dict)
    room_csd: Dict[str, float] = field(default_factory=dict)
    room_entropy: Dict[str, float] = field(default_factory=dict)

    # Transition matrix for the whole fleet
    # Key: (source_A, source_B), Value: count
    fleet_transitions: Dict[Tuple[str, str], int] = field(default_factory=dict)

    # Chain bins for Euler characteristic
    # key: (room_id, bin_index), value: set of sources active in this bin
    chain_bins: Dict[Tuple[str, int], set] = field(default_factory=dict)

    # TE window config
    te_window_size: int = 100  # tiles per source for TE computation
    te_lag: int = 1  # lag for transfer entropy (X_t -> Y_t+lag)

    # IAT window config
    iat_window_size: int = 100  # IATs to keep for autocorrelation
    iat_bin_size: float = 1.0  # seconds — bin IATs for autocorrelation

    # Publish throttle
    last_publish: float = 0.0
    publish_interval: float = 30.0  # seconds between room publications

    # Tracking state
    last_source: str = ""
    last_room: str = ""
    total_tiles: int = 0
    active_rooms: set = field(default_factory=set)

    # Null model state
    null_si_te: float = 0.0
    null_multiplier: float = 0.0
    null_model_computed: bool = False


# ─── ALGORITHMS ───


def running_source_entropy(chain_sequence: List[int]) -> float:
    """
    Compute Shannon entropy of source chain contributions.
    Treats chain sequence as symbol sequence; each step is assigned to a source.
    We use chain_size gaps as proxy for source activation.

    Input: list of chain_size values in order of arrival
    Output: H in bits
    """
    if len(chain_sequence) < 2:
        return 0.0

    mn, mx = min(chain_sequence), max(chain_sequence)
    if mn == mx:
        return 0.0

    # Bin by chain_size intervals
    bin_width = max(1, (mx - mn) / 50)  # 50 bins
    bins = Counter()
    for cs in chain_sequence:
        bin_idx = int((cs - mn) / bin_width)
        bins[bin_idx] += 1

    total = len(chain_sequence)
    H = 0.0
    for count in bins.values():
        p = count / total
        if p > 0:
            H -= p * math.log2(p)

    return H


def running_transfer_entropy(
    source_sequence: List[str], target_source: str, window_size: int = 100
) -> float:
    """
    Transfer Entropy from all sources to target_source.

    TE(X->Y) = sum p(y_{t+1}, y_t, x_t) * log2(p(y_{t+1}|y_t, x_t) / p(y_{t+1}|y_t))

    Online version: maintain a sliding window of last `window_size` events.
    Each event: (prev_source, curr_source)

    TE = H(Y_t) - H(Y_t | X_t)
    """
    if len(source_sequence) < window_size:
        return 0.0

    window = source_sequence[-window_size:]

    # Build joint and marginal counts
    joint = Counter()
    marginal_prev = Counter()
    marginal_curr = Counter()

    for i in range(1, len(window)):
        prev_source = window[i - 1]
        curr_source = window[i]

        if curr_source == target_source:
            joint[prev_source] += 1
            marginal_prev[prev_source] += 1
            marginal_curr[curr_source] += 1

    total = sum(marginal_prev.values())
    if total < 2:
        return 0.0

    # H(Y_t) — entropy of previous source
    H_Y = 0.0
    for count in marginal_prev.values():
        p = count / total
        if p > 0:
            H_Y -= p * math.log2(p)

    # H(Y_t | X_t) — conditional entropy
    H_Y_given_X = 0.0
    for prev_source, joint_count in joint.items():
        p_x = marginal_prev[prev_source] / total
        if marginal_prev[prev_source] > 0:
            p_y_given_x = joint_count / marginal_prev[prev_source]
            if p_y_given_x > 0:
                H_Y_given_X -= p_x * p_y_given_x * math.log2(p_y_given_x)

    TE = H_Y - H_Y_given_X
    return max(0.0, TE)  # TE is non-negative by definition


def running_iat_autocorrelation(iat_sequence: List[float], lag: int = 1) -> float:
    """
    Compute lag-1 autocorrelation of inter-arrival times.
    CSD-τ = ρ₁ = corr(IAT_t, IAT_{t-1})

    This measures whether silence begets silence (positive ρ₁) or
    alternates (negative ρ₁ = coordinated bursts).

    From DeepSeek v4-pro: ρ₁ = -0.4893 (47× Poisson null) in the fleet.
    Negative autocorrelation = coordinated burst pattern.
    """
    if len(iat_sequence) < lag + 2:
        return 0.0

    iat = iat_sequence[-100:]  # last 100 IATs

    # Lag-1 autocorrelation
    if lag >= len(iat):
        return 0.0

    x = iat[:-lag]
    y = iat[lag:]

    mx = sum(x) / len(x)
    my = sum(y) / len(y)

    dx = [xi - mx for xi in x]
    dy = [yi - my for yi in y]

    sum_dx_sq = sum(dxi * dxi for dxi in dx)
    sum_dy_sq = sum(dyi * dyi for dyi in dy)
    sum_dx_dy = sum(dx[i] * dy[i] for i in range(len(dx)))

    denominator = math.sqrt(sum_dx_sq * sum_dy_sq) + 1e-12
    corr = sum_dx_dy / denominator

    return float(corr)


def running_euler_characteristic(
    chain_bins: Dict[Tuple[str, int], set], all_sources: List[str]
) -> Tuple[int, int, int]:
    """
    Compute Euler characteristic for each room's chain-space.

    χ = V - E
    where:
      V = number of chain bins with ≥1 source active
      E = number of chain bins where ≥2 sources overlap (edges)

    β₀ = number of connected components ≈ number of monotonic source sequences
    β₁ = χ - β₀ (coordination loops / holes)

    Returns: (chi, beta0, beta1)
    """
    V_total = 0
    E_total = 0

    for (room_id, bin_idx), sources in chain_bins.items():
        if len(sources) >= 1:
            V_total += 1
        if len(sources) >= 2:
            # Edges = C(sources, 2) = number of source pairs in same bin
            n = len(sources)
            E_total += (n * (n - 1)) // 2

    # β₀: count sources with monotonic chain sequences
    beta0 = len(all_sources)
    beta1 = V_total - E_total - beta0

    chi = V_total - E_total

    return chi, beta0, max(0, beta1)


def update_transition_matrix(
    fleet_transitions: Dict[Tuple[str, str], int], prev_source: str, curr_source: str
) -> None:
    """
    Increment transition count from prev_source to curr_source.
    Called on every tile arrival.
    """
    key = (prev_source, curr_source)
    fleet_transitions[key] = fleet_transitions.get(key, 0) + 1


def build_transition_matrix(
    fleet_transitions: Dict[Tuple[str, str], int], all_sources: List[str]
) -> Tuple[List[str], List[List[float]]]:
    """
    Build a transition probability matrix.
    Returns (source_list, matrix) where matrix[i][j] = P(source_j | source_i)
    """
    source_to_idx = {s: i for i, s in enumerate(all_sources)}
    n = len(all_sources)

    # Count outgoing transitions from each source
    out_counts = Counter()
    for (src, dst), count in fleet_transitions.items():
        out_counts[src] += count

    # Build probability matrix
    matrix = [[0.0] * n for _ in range(n)]
    for (src, dst), count in fleet_transitions.items():
        i = source_to_idx.get(src)
        j = source_to_idx.get(dst)
        if i is not None and j is not None:
            matrix[i][j] = count / out_counts.get(src, 1)

    return all_sources, matrix


def compute_si_te(state: CoordinationState) -> float:
    """
    Source Interleaving Transfer Entropy.

    Key insight: agents don't just write to their own rooms — they interleave.
    SI-TE measures information flow across the entire fleet based on who writes next
    after whom, globally.

    Build global source sequence: all tiles sorted by timestamp,
    extract source_id in order → [oracle1, fleet-bot, oracle1, forgemaster, ...]

    Then for each source pair (X, Y):
    TE(X→Y) = how much knowing X's previous state reduces uncertainty about Y's next state?

    SI-TE = sum of all TE(X→Y) / number of source pairs
    """
    # Build global source sequence (all tiles, sorted by timestamp)
    all_tiles = []
    for source_id, sb in state.sources.items():
        for ts in sb.timestamps:
            all_tiles.append((ts, source_id))

    all_tiles.sort(key=lambda x: x[0])
    global_sequence = [src for _, src in all_tiles]

    if len(global_sequence) < 100:
        return 0.0

    # Compute TE from each source to each other source
    te_sum = 0.0
    te_count = 0

    sources = list(state.sources.keys())
    for src in sources:
        for dst in sources:
            if src != dst:
                te = running_transfer_entropy(global_sequence, dst, window_size=100)
                te_sum += te
                te_count += 1

    si_te = te_sum / te_count if te_count > 0 else 0.0
    return si_te


def compute_fleet_csd(state: CoordinationState) -> float:
    """
    Coordination Silence Decay — fleet-wide IAT autocorrelation.

    Average of per-source lag-1 autocorrelations, weighted by tile count.
    ρ₁ = -0.4893 (DeepSeek v4-pro) means: coordinated bursts, not random Poisson.
    """
    weighted_corr = 0.0
    total_tiles = 0

    for source_id, sb in state.sources.items():
        iat_list = list(sb.iat_sequence)
        if len(iat_list) >= 3:
            corr = running_iat_autocorrelation(iat_list, lag=1)
            tile_count = len(sb.chain_sequence)
            weighted_corr += corr * tile_count
            total_tiles += tile_count

    if total_tiles == 0:
        return 0.0

    return weighted_corr / total_tiles


def compute_null_model(state: CoordinationState, n_shuffles: int = 50) -> Tuple[float, float]:
    """
    Compute null model for SI-TE by shuffling source labels.

    Returns: (null_mean, multiplier) where multiplier = original_SI_TE / null_mean
    """
    # Build global source sequence
    all_tiles = []
    for source_id, sb in state.sources.items():
        for ts in sb.timestamps:
            all_tiles.append((ts, source_id))

    all_tiles.sort(key=lambda x: x[0])
    global_sequence = [src for _, src in all_tiles]

    if len(global_sequence) < 100:
        return 0.0, 0.0

    original_si_te = compute_si_te(state)

    # Compute SI-TE on shuffled sequences
    te_shuffled = []
    sources = list(state.sources.keys())

    for _ in range(n_shuffles):
        shuffled = global_sequence.copy()
        random.shuffle(shuffled)

        # Compute SI-TE on shuffled sequence
        te_sum = 0.0
        te_count = 0

        for src in sources:
            for dst in sources:
                if src != dst:
                    te = running_transfer_entropy(shuffled, dst, window_size=100)
                    te_sum += te
                    te_count += 1

        si_te_shuffled = te_sum / te_count if te_count > 0 else 0.0
        te_shuffled.append(si_te_shuffled)

    null_mean = sum(te_shuffled) / len(te_shuffled) if te_shuffled else 0.0
    multiplier = original_si_te / null_mean if null_mean > 0 else float("inf")

    return null_mean, multiplier


# ─── PLATO API ───


def plato_get(url_path: str) -> Optional[Dict]:
    """Make a GET request to PLATO API."""
    try:
        url = f"{PLATO_BASE_URL}{url_path}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as response:
            data = response.read().decode("utf-8")
            return json.loads(data)
    except (urllib.error.URLError, json.JSONDecodeError) as e:
        print(f"[PLATO GET Error] {url_path}: {e}")
        return None


def plato_post(url_path: str, data: Dict) -> bool:
    """Make a POST request to PLATO API."""
    try:
        url = f"{PLATO_BASE_URL}{url_path}"
        json_data = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=json_data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status == 200
    except (urllib.error.URLError, json.JSONDecodeError) as e:
        print(f"[PLATO POST Error] {url_path}: {e}")
        return False


def get_all_room_names() -> List[str]:
    """Get list of all room names from PLATO."""
    data = plato_get("/rooms")
    if data and "rooms" in data:
        return data["rooms"]
    return []


def get_room_history(room_name: str, limit: int = 100) -> List[Dict]:
    """Get recent tiles from a room."""
    data = plato_get(f"/room/{room_name}/history?limit={limit}")
    if data and "tiles" in data:
        return data["tiles"]
    return []


# ─── CORE LOGIC ───


def on_tile_event(tile: Dict, state: CoordinationState):
    """
    Called when a new tile is appended to any tracked room.
    Extract: source, room_id, chain_size, timestamp
    Update all rolling buffers.
    """
    source = tile.get("source", tile.get("source_id", "unknown"))
    room_id = tile.get("room", "unknown")
    chain_size = tile.get("provenance", {}).get("chain_size", 0)
    ts = tile.get("timestamp", time.time())

    # Get or create SourceBuffer
    if source not in state.sources:
        state.sources[source] = SourceBuffer(source_id=source, room_id=room_id)
        print(f"[coordination-topology] New source: {source} from {room_id}")

    sb = state.sources[source]

    # Update chain sequence
    sb.chain_sequence.append(chain_size)
    sb.timestamps.append(ts)

    # Compute IAT
    if sb.last_timestamp > 0:
        iat = ts - sb.last_timestamp
        sb.iat_sequence.append(iat)
    sb.last_timestamp = ts

    # Update chain bins (for Euler characteristic)
    bin_idx = chain_size // CHAIN_BIN_WIDTH
    key = (room_id, bin_idx)
    if key not in state.chain_bins:
        state.chain_bins[key] = set()
    state.chain_bins[key].add(source)

    # Update transition matrix
    if state.last_source:
        update_transition_matrix(state.fleet_transitions, state.last_source, source)

    state.last_source = source
    state.last_room = room_id
    state.total_tiles += 1

    # Update entropy window
    sb.entropy_window.append(chain_size)

    # Mark room active
    state.active_rooms.add(room_id)


def publish_coordination_tile(state: CoordinationState) -> bool:
    """
    Publish a coordination tile with current topology metrics.
    """
    print(f"[coordination-topology] Publishing tile (sources={len(state.sources)}, tiles={state.total_tiles})")

    # Compute per-source metrics
    te_matrix: Dict[str, Dict[str, float]] = {}
    source_entropies: Dict[str, float] = {}
    iat_autocorrs: Dict[str, float] = {}

    for source_id, sb in state.sources.items():
        # Source entropy
        source_entropies[source_id] = running_source_entropy(list(sb.chain_sequence))

        # IAT autocorrelation
        iat_list = list(sb.iat_sequence)
        if len(iat_list) >= 3:
            iat_autocorrs[source_id] = running_iat_autocorrelation(iat_list, lag=1)

    # Compute TE matrix
    sources = list(state.sources.keys())
    for src in sources:
        te_matrix[src] = {}
        for dst in sources:
            if src != dst:
                # Build global sequence
                all_tiles = []
                for s_id, s_buf in state.sources.items():
                    for t_ts in s_buf.timestamps:
                        all_tiles.append((t_ts, s_id))
                all_tiles.sort(key=lambda x: x[0])
                global_sequence = [s for _, s in all_tiles]

                if len(global_sequence) >= 100:
                    te_matrix[src][dst] = running_transfer_entropy(global_sequence, dst, window_size=100)
                else:
                    te_matrix[src][dst] = 0.0

    # Compute fleet-wide metrics
    si_te = compute_si_te(state)
    csd_tau = compute_fleet_csd(state)
    chi, beta0, beta1 = running_euler_characteristic(state.chain_bins, list(state.sources.keys()))

    # Build room SC-χ
    room_sc_chi: Dict[str, Dict[str, int]] = {}
    for room_id in state.active_rooms:
        room_bins = {k: v for k, v in state.chain_bins.items() if k[0] == room_id}
        chi_r, b0_r, b1_r = running_euler_characteristic(room_bins, list(state.sources.keys()))
        room_sc_chi[room_id] = {"chi": chi_r, "beta0": b0_r, "beta1": b1_r}

    # Build transition matrix
    sources_list, trans_matrix = build_transition_matrix(
        state.fleet_transitions, list(state.sources.keys())
    )

    # Compute null model (if not computed yet or periodically recompute)
    if not state.null_model_computed or state.total_tiles % 5000 == 0:
        state.null_si_te, state.null_multiplier = compute_null_model(state, n_shuffles=50)
        state.null_model_computed = True
        print(f"[coordination-topology] Null model: SI-TE={state.null_si_te:.4f}, multiplier={state.null_multiplier:.2f}x")

    tile = {
        "room": "coordination-topology",
        "source": "coordination-topology",
        "timestamp": time.time(),
        "te_matrix": json.dumps(te_matrix),
        "source_entropy": json.dumps(source_entropies),
        "iat_autocorr": json.dumps(iat_autocorrs),
        "room_sc_chi": json.dumps(room_sc_chi),
        "fleet_transition_matrix": json.dumps([sources_list, trans_matrix]),
        "si_te": si_te,
        "csd_tau": csd_tau,
        "chain_euler_chi": chi,
        "active_sources": len(state.sources),
        "total_tiles_processed": state.total_tiles,
        "last_tile_source": state.last_source,
        "last_tile_room": state.last_room,
        "null_si_te": state.null_si_te,
        "null_multiplier": state.null_multiplier,
    }

    # Publish to PLATO
    success = plato_post(f"/room/{ROOM_NAME}/submit", tile)
    if success:
        state.last_publish = time.time()
        print(f"[coordination-topology] Tile published successfully")
    else:
        print(f"[coordination-topology] Failed to publish tile")

    return success


def should_publish(state: CoordinationState) -> bool:
    """
    Check if a publish should be triggered.
    Triggers:
    1. publish_interval elapsed (default: 30 seconds)
    2. New source agent observed
    3. Tile count exceeds threshold (memory pressure)
    """
    now = time.time()

    # Time-based trigger
    if now - state.last_publish >= state.publish_interval:
        return True

    # No explicit source tracking yet - this can be added
    # No explicit SC-χ change tracking - this can be added

    return False


# ─── MAIN LOOP ───


class CoordinationTopologyService:
    """Main service for coordination-topology room."""

    def __init__(self):
        self.state = CoordinationState()
        self.running = False
        self.poll_thread = None
        self.publish_thread = None
        self.rooms_to_track: List[str] = []

    def refresh_room_list(self):
        """Refresh the list of rooms to track."""
        self.rooms_to_track = get_all_room_names()
        if ROOM_NAME in self.rooms_to_track:
            self.rooms_to_track.remove(ROOM_NAME)  # Don't track ourselves

    def poll_rooms(self):
        """Poll rooms for new tiles."""
        while self.running:
            try:
                self.refresh_room_list()

                for room_name in self.rooms_to_track:
                    tiles = get_room_history(room_name, limit=50)
                    for tile in tiles:
                        # Process tile if it's newer than last seen
                        # For simplicity, process all (deduplication can be added)
                        on_tile_event(tile, self.state)

                # Check if we should publish
                if should_publish(self.state):
                    publish_coordination_tile(self.state)

            except Exception as e:
                print(f"[coordination-topology] Poll error: {e}")

            time.sleep(5)  # Poll every 5 seconds

    def publish_loop(self):
        """Periodic publish loop (30s interval as fallback)."""
        while self.running:
            try:
                time.sleep(self.state.publish_interval)
                if self.running:
                    publish_coordination_tile(self.state)
            except Exception as e:
                print(f"[coordination-topology] Publish loop error: {e}")

    def start(self):
        """Start the service."""
        if self.running:
            return

        self.running = True
        print(f"[coordination-topology] Starting service (poll interval=5s, publish interval={self.state.publish_interval}s)")

        # Start polling thread
        self.poll_thread = threading.Thread(target=self.poll_rooms, daemon=True)
        self.poll_thread.start()

        # Start periodic publish thread
        self.publish_thread = threading.Thread(target=self.publish_loop, daemon=True)
        self.publish_thread.start()

    def stop(self):
        """Stop the service."""
        self.running = False
        print(f"[coordination-topology] Service stopped")


# ─── HTTP SERVER ───


from http.server import HTTPServer, BaseHTTPRequestHandler


class CoordinationTopologyHandler(BaseHTTPRequestHandler):
    """HTTP request handler for coordination-topology room."""

    def __init__(self, *args, service: CoordinationTopologyService, **kwargs):
        self.service = service
        super().__init__(*args, **kwargs)

    def do_POST(self):
        """Handle POST requests."""
        if self.path == "/submit":
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)
                tile = json.loads(body.decode("utf-8"))

                on_tile_event(tile, self.service.state)

                # Check if should publish
                if should_publish(self.service.state):
                    publish_coordination_tile(self.service.state)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                response = {"status": "ok", "message": "Tile processed"}
                self.wfile.write(json.dumps(response).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                response = {"status": "error", "message": str(e)}
                self.wfile.write(json.dumps(response).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        """Handle GET requests."""
        if self.path.startswith("/room/coordination-topology"):
            try:
                # Return current state as JSON
                response_data = {
                    "room": "coordination-topology",
                    "sources": list(self.service.state.sources.keys()),
                    "active_sources": len(self.service.state.sources),
                    "total_tiles_processed": self.service.state.total_tiles,
                    "last_publish": self.service.state.last_publish,
                    "null_multiplier": self.service.state.null_multiplier,
                    "active_rooms": list(self.service.state.active_rooms),
                }

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(response_data, indent=2).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                response = {"status": "error", "message": str(e)}
                self.wfile.write(json.dumps(response).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


def create_http_server(service: CoordinationTopologyService, port: int = 8850):
    """Create HTTP server for coordination-topology room."""

    def handler(*args, **kwargs):
        return CoordinationTopologyHandler(*args, service=service, **kwargs)

    server = HTTPServer(("localhost", port), handler)
    print(f"[coordination-topology] HTTP server listening on port {port}")
    return server


# ─── ENTRY POINT ───


def main():
    """Main entry point."""
    print("[coordination-topology] Starting coordination-topology PLATO room integration")
    print(f"[coordination-topology] PLATO API: {PLATO_BASE_URL}")
    print(f"[coordination-topology] Room: {ROOM_NAME}")
    print(f"[coordination-topology] Publish interval: {PUBLISH_INTERVAL}s")

    # Create and start service
    service = CoordinationTopologyService()
    service.start()

    # Create HTTP server
    http_server = create_http_server(service, port=8850)

    # Run HTTP server in main thread
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        print("\n[coordination-topology] Shutting down...")
        service.stop()
        http_server.shutdown()


if __name__ == "__main__":
    main()