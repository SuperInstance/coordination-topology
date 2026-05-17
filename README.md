# coordination-topology

Online Transfer Entropy (TE), source entropy, inter-arrival time (IAT) autocorrelation, and Euler characteristic computation for multi-agent fleet coordination topology.

## Dependencies

none (standalone Python 3.10+)

## Usage

```python
from coordination_topology import CoordinationState, running_transfer_entropy, running_iat_autocorrelation

state = CoordinationState()

# Ingest tiles as they arrive (streaming/online)
for tile in plato_tiles:
    source = tile["source"]
    chain = tile["provenance"]["chain_size"]
    ts = tile["provenance"]["timestamp"]
    state.ingest(source, chain, ts)

# Query topology at any point
te = running_transfer_entropy(state)          # bits
ent = running_source_entropy(state)           # bits
iat = running_iat_autocorrelation(state)      # per-source dict
euler = running_euler_characteristic(state)   # {V, E, chi}
```

## Metrics
- **Source Interleaving Transfer Entropy (SI-TE)**: mutual information between consecutive source labels — >0.1 bits = structure confirmed
- **Coordination Silence Decay (CSD-τ)**: lag-1 autocorrelation of inter-arrival times — negative = burst coordination
- **Source-Chain Euler Characteristic (SC-χ)**: V−E of source trajectories in chain-space

## Shell Loading

```python
from plato_shell_bridge import PlatoShell
shell = PlatoShell("agent-shell")
shell.load_tool("coordination-topology")
```

## Tests

```bash
python3 -m pytest tests/ -v
```

## License

MIT — Part of the Cocapn Fleet Intelligence System


## Validation

Validated against 34,390 real PLATO tiles from SuperInstance fleet:

```
TE = 1.74 bits (3,384× shuffled null model)
Sources tracked: 54
Euler characteristic: χ = 42 (V=175, E=133)
```

```bash
python3 tile_replay.py
```

## PLATO Room

The `room_integration.py` module connects to a live PLATO server at localhost:8847,
feeds tiles through the algorithms, and publishes topology tiles every 30s.

```bash
python3 room_integration.py
```
