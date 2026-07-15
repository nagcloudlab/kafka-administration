 Cluster & metadata plane
  - ZK → KRaft migration: quorum controller, metadata log, why the incident we just hit (stale ephemerals, session semantics) disappears in KRaft
  - Broker registration lifecycle, controlled shutdown, unclean leader election trade-offs
  - Rack awareness + broker placement for real DC/AZ topologies

  Partition & replication mechanics
  - ISR shrink/expand: replica.lag.time.max.ms, what a lagging follower actually looks like in logs
  - Preferred leader election, auto.leader.rebalance.enable, when to disable it
  - Reassignment: kafka-reassign-partitions throttling, how to move a 500 GB partition without saturating the network
  - min.insync.replicas + acks=all — the "durability triangle" and how RF=3 / min.ISR=2 actually behaves under one broker down vs two

  Storage & retention
  - Log segments, index files, log.retention.* vs log.roll.ms, disk-full triage
  - Tiered storage (KIP-405) — when it's worth it
  - Compacted topics: tombstones, min.cleanable.dirty.ratio, cleaner starvation

  Producer/consumer semantics (admin lens)
  - Idempotence + transactions: PID, epoch fencing, transaction coordinator, how EOS actually works end-to-end (like your Postgres sink)
  - Consumer group rebalance protocols: eager vs cooperative-sticky, static membership, why session.timeout.ms matters for graceful shutdown

  Observability & capacity
  - JMX metrics that matter: URP, offline partitions, request queue time, log flush latency, controller failovers
  - Under-replicated partition alerting — what "URP > 0 for 5 min" really means
  - Sizing brokers: CPU vs network vs disk IOPS, page cache behavior

  Security & multi-tenancy
  - SASL (SCRAM/PLAIN/OAUTHBEARER) + mTLS, ACLs, why authorizer choice matters
  - Quotas: producer, consumer, request-rate — how to protect a shared cluster
  - Client-id vs user principal for quota attribution (your producer already sets payments-producer-1)

  Disaster & operations
  - MirrorMaker 2 / cluster linking, active-active vs active-passive
  - Rolling restart discipline, JVM tuning (G1 pauses, page cache sizing)
  - Recovery drills: broker disk failure, ZK/KRaft quorum loss, __consumer_offsets corruption