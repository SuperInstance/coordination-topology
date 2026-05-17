import time
from collections import deque, defaultdict
from dataclasses import dataclass, field
from math import log2


@dataclass
class SourceBuffer:
    source_id: str
    chain_sequence: deque = field(default_factory=lambda: deque(maxlen=2000))
    iat_sequence: deque = field(default_factory=lambda: deque(maxlen=500))
    timestamps: deque = field(default_factory=lambda: deque(maxlen=2000))
    transition_counts: dict = field(default_factory=dict)
    last_timestamp: float = 0.0


@dataclass
class CoordinationState:
    sources: dict = field(default_factory=dict)
    fleet_transitions: dict = field(default_factory=lambda: defaultdict(int))
    source_sequence: deque = field(default_factory=lambda: deque(maxlen=2000))
    
    def ingest(self, source: str, chain: int, timestamp: float):
        if source not in self.sources:
            self.sources[source] = SourceBuffer(source_id=source)
        buf = self.sources[source]
        buf.chain_sequence.append(chain)
        buf.timestamps.append(timestamp)
        if buf.last_timestamp > 0:
            iat = timestamp - buf.last_timestamp
            buf.iat_sequence.append(iat)
        buf.last_timestamp = timestamp
        if self.source_sequence:
            prev = self.source_sequence[-1]
            self.fleet_transitions[(prev, source)] += 1
            buf.transition_counts[prev] = buf.transition_counts.get(prev, 0) + 1
        self.source_sequence.append(source)


def running_source_entropy(state: CoordinationState) -> float:
    seq = list(state.source_sequence)
    if len(seq) < 10:
        return 0.0
    total = len(seq)
    counts = defaultdict(int)
    for s in seq:
        counts[s] += 1
    h = 0.0
    for c in counts.values():
        p = c / total
        if p > 0:
            h -= p * log2(p)
    return h


def running_transfer_entropy(state: CoordinationState) -> float:
    trans = dict(state.fleet_transitions)
    if not trans or len(state.source_sequence) < 10:
        return 0.0
    total = sum(trans.values())
    prev_counts = defaultdict(int)
    pair_counts = defaultdict(lambda: defaultdict(int))
    for (prev, curr), count in trans.items():
        prev_counts[prev] += count
        pair_counts[prev][curr] += count
    h_cond = 0.0
    for prev, total_prev in prev_counts.items():
        p_prev = total_prev / total
        h_given = 0.0
        for c in pair_counts[prev].values():
            p = c / total_prev
            if p > 0:
                h_given -= p * log2(p)
        h_cond += p_prev * h_given
    h_total = 0.0
    curr_counts = defaultdict(int)
    for (prev, curr), count in trans.items():
        curr_counts[curr] += count
    for c in curr_counts.values():
        p = c / total
        if p > 0:
            h_total -= p * log2(p)
    return max(0.0, h_total - h_cond)


def running_iat_autocorrelation(state: CoordinationState) -> dict:
    result = {}
    for sid, buf in state.sources.items():
        iats = list(buf.iat_sequence)
        if len(iats) < 20:
            continue
        n = len(iats)
        mean = sum(iats) / n
        var = sum((x - mean) ** 2 for x in iats) / n
        if var == 0:
            continue
        lag1 = sum((iats[i] - mean) * (iats[i + 1] - mean) for i in range(n - 1))
        lag1 /= (n - 1) * var
        result[sid] = {
            "lag1_autocorr": lag1,
            "mean_iat": mean,
            "iat_count": n,
            "cv": (var ** 0.5) / mean if mean > 0 else 0,
        }
    return result


def running_euler_characteristic(state: CoordinationState, bin_width: int = 100) -> dict:
    bins = defaultdict(set)
    for sid, buf in state.sources.items():
        for chain in buf.chain_sequence:
            bins[chain // bin_width].add(sid)
    V = len(bins)
    E = sum(1 for b in bins.values() if len(b) > 1)
    return {"V": V, "E": E, "chi": V - E, "e_over_v": E / V if V > 0 else 0}
