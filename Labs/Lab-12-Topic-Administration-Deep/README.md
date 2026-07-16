# Lab 12 — Kafka Topics · Deep from the Admin View

The topic is the unit an admin actually operates on. This chapter goes deep — anatomy on disk, every config knob you'll set (or leave alone), partition sizing and growth trade-offs, message-size handshake, naming standards, and the production creation checklist.

## Slide decks

- **[slides.html](slides.html)** — the main deep-dive deck. Concepts, formulas, worked examples. 82 slides, ~50 min narration.
- **[slides-topic-configs.html](slides-topic-configs.html)** — companion property-reference deck. Every topic config knob (~25 properties) with default, purpose, when-to-change, trade-off, example. 40 slides.
- **[slides-trainer-notes.html](slides-trainer-notes.html)** — trainer's speaker notes for the main deck. Per-slide Say / Poll / Watch / Demo / Transition cues. Open side-by-side with `slides.html` on your laptop while the projector shows the deck. Sticky TOC, print-friendly.

### Keyboard shortcuts (both decks)

| Key | Action |
|-----|--------|
| `→` `Space` `PgDn` | Next slide |
| `←` `PgUp` | Previous slide |
| `Home` / `End` | First / last slide |
| `O` | **Overview grid** — click any slide to jump |
| `F` | Fullscreen toggle |
| `?` | Show keyboard help |
| `Esc` | Close overview / help |
| `Ctrl+P` | Save as PDF handout (one slide per page) |
| URL `#N` | Deep-link to slide N |

## Scenarios (do after the slides, in order)

- [12.1 — Topic anatomy on disk (segments, indexes, offsets)](Scenario-12.1-Topic-Anatomy-On-Disk.md)
- [12.2 — Config precedence (broker default vs topic override vs client)](Scenario-12.2-Config-Precedence.md)
- [12.3 — Partition sizing & growth (increase allowed, decrease impossible)](Scenario-12.3-Partition-Sizing-And-Growth.md)
- [12.4 — Message size limits & auto-create traps](Scenario-12.4-Message-Size-And-Auto-Create.md)

## Common prereqs

- Labs 1 and 3 done (cluster up, `cluster.sh` fluent).
- Cluster on 9092–9094 running: `./cluster.sh start-monitoring` in `kafka-lab/`.

Paste at the top of every new terminal:

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

## What this chapter maps to in the 5-day TOC

- **Module 6 · Topic administration** — creation, listing, describing, deleting, configuration, retention, cleanup.
- **Module 7 · Partition management** — partition count, ordering, increasing, distribution, hot partitions.

Between the slides and 4 scenarios, both modules are covered end-to-end from an admin perspective. Where a topic-related concept is already covered elsewhere (retention/compaction in Lab-5.1, ACLs in Lab-7.2, reassignment in Lab-5.2, replication in Lab-4), the slides point at that lab rather than duplicating the demo.

## Teaching flow (recommended)

1. **Open slides.html on projector.** Walk sections 1-7 (~45 min of narration).
2. **Pause after each section** for a hands-on scenario:
   - After Section 2 (Anatomy) → **Scenario 12.1**
   - After Section 3 (Configs) → **Scenario 12.2**
   - After Section 4 (Partitions) → **Scenario 12.3**
   - After Section 5 (Sizing / Errors) → **Scenario 12.4**
3. **Close with Section 7** (Production checklist) — students can screenshot the slide as their reference card.

Full chapter budget: **~2.5 hours** including hands-on.

## Full reset

Each scenario has its own Teardown. If the cluster is wedged after the "auto-create" demo (12.4), the standard reset:

```bash
./cluster.sh stop
rm -rf ./data/*
mkdir -p ./data/zookeeper ./data/broker-101 ./data/broker-102 ./data/broker-103
./cluster.sh start
```

## Not covered in Lab 12

- **Topic ACLs** — Lab-7.2.
- **Retention & compaction internals** — Lab-5.1.
- **Partition reassignment across brokers** — Lab-5.2.
- **Replication factor changes** — via reassignment (Lab-5.2); no separate scenario.
- **Compacted-topic tombstone lifecycle beyond the basics** — Lab-5.1 covers it.
- **Topic ID (UUID) internals** — mentioned in slides, not demoed.
