# InsureRaft

A fault-tolerant, strongly-consistent distributed platform for transferring insurance data across carriers, brokers, reinsurers, and regulators — built on the [Raft consensus algorithm](https://raft.github.io/) via [NuRaft](https://github.com/eBay/NuRaft) (eBay).

Every policy creation, claim filing, approval, denial, and payment is replicated to a quorum of nodes before being acknowledged. This gives you zero data loss, automatic failover, and an immutable audit trail that regulators require by law.

---

## Why Raft for Insurance?

Insurance data flows between many parties: a claim filed at a broker must reach the carrier, the reinsurer, and the regulator — in the right order, exactly once, with a tamper-evident record. Today this is solved with fragile point-to-point integrations and shared databases that offer no ordering or consistency guarantees.

InsureRaft replaces all of that with a single replicated log that every participant trusts. The Raft log **is** the audit trail.

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│               InsureRaft Cluster                 │
│                                                  │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐     │
│  │  Node 1  │   │  Node 2  │   │  Node 3  │     │
│  │ (Leader) │◄─►│(Follower)│◄─►│(Follower)│     │
│  │          │   │          │   │          │     │
│  │ LogStore │   │ LogStore │   │ LogStore │     │
│  │ StateMgr │   │ StateMgr │   │ StateMgr │     │
│  └────┬─────┘   └──────────┘   └──────────┘     │
│       │  NuRaft consensus layer                  │
└───────┼──────────────────────────────────────────┘
        │
   ┌────▼──────────────────────────────┐
   │      InsuranceStateMachine        │
   │  commit(log_entry) →              │
   │   • validate idempotency key      │
   │   • apply to policy/claim state   │
   │   • return committed log index    │
   └───────────────────────────────────┘
```

Nodes represent participants: a carrier cluster, a broker node, a reinsurer, or a read-only regulator learner. Membership changes go through Raft configuration changes — no manual coordination required.

---

## Guarantees

| Property | Mechanism |
|---|---|
| **No data loss** | Quorum write — majority must persist before ack |
| **Exactly-once delivery** | `event_id` deduplicated in state machine before commit |
| **Ordered audit log** | Log index is monotonic and immutable |
| **HA / auto failover** | NuRaft leader election — cluster survives minority failure |
| **New party onboarding** | Learner node + snapshot catch-up, zero downtime |
| **Compliance snapshots** | Periodic snapshots exportable as point-in-time state |

---

## Event Model

Every insurance action is a typed, versioned event appended to the Raft log:

| Event | Description |
|---|---|
| `POLICY_CREATED` | New policy issued to a holder |
| `POLICY_UPDATED` | Premium or holder details changed |
| `POLICY_CANCELLED` | Policy cancelled |
| `CLAIM_FILED` | Claim filed against a policy |
| `CLAIM_APPROVED` | Claim approved by adjuster |
| `CLAIM_DENIED` | Claim denied with reason |
| `PAYMENT_ISSUED` | Payment disbursed for an approved claim |
| `ENDORSEMENT_ADDED` | Endorsement or rider added to a policy |

Each event carries a globally unique `event_id` used as an idempotency key — submitting the same event twice is safe.

---

## Project Structure

```
InsureRaft/
├── CMakeLists.txt                  Top-level build (InsureRaft owns this)
├── src/
│   ├── insurance_event.hxx         Event types + NuRaft buffer serialization
│   ├── insurance_log_store.hxx/cxx File-backed durable Raft log store
│   ├── insurance_state_machine.hxx Raft SM: policies, claims, snapshots
│   ├── insurance_state_mgr.hxx     State manager with disk-persisted config
│   └── insurance_server.cxx        Interactive CLI server
└── deps/
    └── nuraft/                     NuRaft (git submodule — eBay/NuRaft)
```

---

## Building

**Prerequisites:** C++11 compiler, CMake ≥ 3.16, OpenSSL

```bash
# Clone with submodules
git clone --recurse-submodules git@github.com:atishay-kasliwal/InsureRaft.git
cd InsureRaft

# Configure and build
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --target insurance_server -j4

# Binary
./build/insurance_server
```

---

## Running a Cluster

Each node is started independently. The first node bootstraps as the leader; additional nodes are added via the `add` command.

```bash
# Terminal 1 — bootstrap node (becomes leader)
./build/insurance_server 1 localhost:26001

# Terminal 2
./build/insurance_server 2 localhost:26002

# Terminal 3
./build/insurance_server 3 localhost:26003
```

From node 1's prompt, add the other nodes:

```
insure[1]> add 2 localhost:26002
insure[1]> add 3 localhost:26003
insure[1]> ls
  server 1: localhost:26001  [LEADER]
  server 2: localhost:26002
  server 3: localhost:26003
```

---

## CLI Commands

### Policy management

```bash
policy <id> <holder> <annual_premium$>       # Create a policy
update_policy <id> <new_premium$>            # Update premium
cancel_policy <id>                           # Cancel a policy
endorse <policy_id> <text...>               # Add an endorsement
```

### Claims

```bash
claim <id> <policy_id> <claimant> <amount$> [notes]   # File a claim
approve <claim_id> [notes]                             # Approve
deny    <claim_id> [notes]                             # Deny
pay     <claim_id> <amount$>                           # Issue payment
```

### Query & cluster

```bash
policies    # List all policies in state machine
claims      # List all claims
stat        # Raft status (leader, log range, term)
ls          # List cluster members
add <id> <host:port>   # Add a peer node
```

### Example workflow

```
insure[1]> policy POL-001 "Alice Johnson" 1200.00
  committed at log index 1

insure[1]> claim CLM-001 POL-001 "Alice Johnson" 4500.00 "Water damage to kitchen"
  committed at log index 2

insure[1]> approve CLM-001 "Damage verified by adjuster"
  committed at log index 3

insure[1]> pay CLM-001 4500.00
  committed at log index 4

insure[1]> claims
  CLAIM   CLM-001  policy: POL-001  claimant: Alice Johnson  amount: $4500.00  status: PAID
```

---

## Data Persistence

Each node stores its data under `./data/srv_<id>/` by default (overridable as a third argument):

```
data/srv_1/
├── raft.log              Raft diagnostic log
├── cluster_config.bin    Persisted cluster membership
├── server_state.bin      Persisted Raft term and vote
└── log/
    ├── start_idx.bin     Log compaction watermark
    └── entry_00000001.bin
    └── entry_00000002.bin
    ...
```

On restart a node loads its log, replays from the last snapshot, and rejoins the cluster automatically.

---

## Technology

| Layer | Choice |
|---|---|
| Consensus | [NuRaft](https://github.com/eBay/NuRaft) (C++ Raft) |
| Transport | ASIO (bundled with NuRaft) |
| TLS | OpenSSL (via NuRaft) |
| Log persistence | File-backed per-entry store |
| Build | CMake ≥ 3.16 |

---

## License

InsureRaft source code is released under the Apache 2.0 License.
NuRaft is copyright eBay Inc., also Apache 2.0.
