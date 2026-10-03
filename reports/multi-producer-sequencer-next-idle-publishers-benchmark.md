# `MultiProducerSequencer.next()` with many mostly-idle producers

## TL;DR

- **The old `next()` (CAS loop, check capacity before claiming) beat master's (claim first with `getAndAdd`, then wait for capacity) at every buffer size.** It was about 6–180× faster below 4096 and about 2.5–5× faster from 16384 up (2 forks, 5+5 iterations).
- **Buffer size has a big effect.** Throughput rises sharply with ring size for every variant. On master it goes from 0.003 ops/µs at 256 to about 2.3 ops/µs at 65536.
- **Changing how master waits doesn't close the gap.** Swapping `parkNanos(1L)` for 50µs, 500µs, `Thread.yield()` or a busy spin left master well below the old implementation at almost every size in quick runs. This points at claim-then-wait as the problem, not the wait itself.
- **Caveat:** the runs were on an 8-core machine with 83 threads and no CPU pinning. The error bars are wide, but the old-versus-master gap is clear at most sizes.

## Premise

The setup is one ring buffer, one consumer, and many producers using a shared `MultiProducerSequencer`. A few producers publish as fast as they can, and many publish rarely (once per ms). The question is how fast the busy producers can claim and publish when the ring is shared with lots of mostly-idle publishers, and how that depends on ring size and on how `next()` waits.

## What the benchmark does

JMH benchmark `MultiProducerSequencerNextBenchmark.BusyAndIdleProducers` (in `src/jmh/java/com/lmax/disruptor/`). All threads share one `MultiProducerSequencer` (with `BusySpinWaitStrategy`) and one consumer `Sequence`, which is registered as the gating sequence.

| Thread role | Count (`-tg 2,1,80`) | What it does on each call |
|---|---|---|
| `busyProducer` | 2 | `next()` then `publish(seq)`, as fast as possible. **This is the measured score.** |
| `consumerWithIdle` | 1 | Reads `getHighestPublishedSequence(next, cursor)` and moves its sequence up to that point, freeing slots. It never processes events. It's a pure "keep the ring draining" consumer, mimicking the consumer's barrier and `BatchEventProcessor`. |
| `idlePublisher` | 80 | Waits until its next deadline (every `idleIntervalNanos`, 1ms here), then calls `next()` and `publish(seq)`. If it falls behind, it skips the missed slots rather than catching up in a burst. |

- **Score:** throughput (ops/µs) of `busyProducer`, totalled across both busy producer threads. One op is one `next()` plus one `publish()`. Publishing is required, because the consumer can't move past unpublished slots.
- **Shutdown:** once measurement ends, producers stop claiming and the consumer jumps its sequence to the cursor. That frees any producer still waiting in `next()`, so runs always finish.
- **Not pinned:** busy producers and the consumer can be pinned with `ISOLATED_CPUS`, but these runs weren't.
- **Environment:** 8 cores, Java 21, JMH 1.35, ring sizes 256 to 65536.

Run command:

```
java -jar build/libs/disruptor-4.0.0-SNAPSHOT-jmh.jar MultiProducerSequencerNextBenchmark.BusyAndIdleProducers \
  -f 2 -wi 5 -i 5 -p idleIntervalNanos=1000000 \
  -p bufferSize=256,512,1024,2048,4096,8192,16384,32768,65536 -tg 2,1,80
```

`-tg` thread counts follow the group's method names in alphabetical order: `busyProducer`, `consumerWithIdle`, `idlePublisher`.

## What the columns mean

| Column | Implementation | What happens when the ring is full |
|---|---|---|
| **old rev `parkNanos(1)`** | Revision `83db6c9e`, **CAS-loop** `next()` | Check capacity → if full, `parkNanos(1)` and retry **without having claimed** → else CAS the cursor |
| **master `parkNanos(1L)`** | Master `c871ca49`, **`getAndAdd`** `next()` | Claim the sequence first → then spin with `parkNanos(1L)` until the consumer frees the slot |
| `parkNanos(50µs)` / `parkNanos(500µs)` | Master with the park length changed | Same claim-then-wait, with a longer sleep on each retry |
| `Thread.yield()` | Master with yield instead of park | Same claim-then-wait, giving up the CPU without sleeping |
| busy spin | Master with the wait line removed | Same claim-then-wait, with a tight spin |

### The two implementations

**Old (`83db6c9e`): check, then claim.**

```java
do {
    current = cursor.get();
    next = current + n;
    wrapPoint = next - bufferSize;
    if (wrapPoint > cachedGating || cachedGating > current) {
        gating = min(gatingSequences);
        if (wrapPoint > gating) { parkNanos(1); continue; }   // full: wait, holding nothing
        gatingSequenceCache.set(gating);
    }
    else if (cursor.compareAndSet(current, next)) break;      // claim only when it fits
} while (true);
```

A producer only takes a sequence once there's room for it. If the CAS fails because another producer claimed first, it retries.

**Master (`c871ca49`): claim, then wait.**

```java
current = cursor.getAndAdd(n);                // claimed immediately, unconditionally
nextSequence = current + n;
wrapPoint = nextSequence - bufferSize;
if (wrapPoint > cachedGating || cachedGating > current) {
    while (wrapPoint > (gating = min(gatingSequences)))
        parkNanos(1L);                        // full: wait, *holding* sequence nextSequence
    gatingSequenceCache.set(gating);
}
```

Each claim costs a single atomic instruction and never retries, which is cheap when the ring has space. When the ring is full, the producer already owns a sequence it can't publish yet.

## Results

Busy-producer throughput in ops/µs, totalled across the 2 busy producers, with `-tg 2,1,80` and `idleIntervalNanos=1000000`.

| bufferSize | old rev `parkNanos(1)` † | master `parkNanos(1L)` † | `parkNanos(50µs)` | `parkNanos(500µs)` | `Thread.yield()` | busy spin |
|---:|---:|---:|---:|---:|---:|---:|
| 256 | **0.527** ± 0.420 | 0.003 ± 0.001 | 0.002 | 0.005 | 0.285 | 0.001 |
| 512 | **0.375** ± 0.286 | 0.005 ± 0.005 | 0.008 | 0.024 | 0.001 | 0.336 |
| 1024 | **0.520** ± 0.388 | 0.026 ± 0.011 | 0.025 | 0.032 | 0.034 | ≈0.0001 |
| 2048 | **0.676** ± 0.707 | 0.034 ± 0.014 | 0.242 | 0.091 | 0.477 | ≈0.001 |
| 4096 | **0.770** ± 0.720 | 0.109 ± 0.062 | 0.162 | 0.196 | 0.052 | 0.012 |
| 8192 | **1.211** ± 0.746 | 0.274 ± 0.318 | 0.454 | 0.130 | 0.116 | 0.001 |
| 16384 | **3.124** ± 2.094 | 0.629 ± 0.439 | 0.518 | 0.687 | 0.418 | 0.056 |
| 32768 | **5.684** ± 3.400 | 2.257 ± 2.424 | 1.400 | 0.927 | 0.355 | 0.134 |
| 65536 | **12.284** ± 7.125 | 2.347 ± 4.475 | 4.009 | 5.278 | 2.740 | 0.881 |

† 2 forks, 5+5 iterations, ± JMH 99.9% error. The 2048 row is a rerun after a benchmark fix. The other four columns are single quick runs (1 fork, 1+1 iterations). They have no error estimate and used an earlier version of the idle-publisher scheduling that had a burst bug, so treat them as rough trends only.

For scale: with no idle publishers, 2 producers on master at 65536 reached about 31 ops/µs.

Health check: the 80 idle publishers should total about 0.08 ops/µs. They managed only about 0.002–0.013 ops/µs, so they too spend most of their time blocked in `next()`. The ring is full a lot of the time at every size, which means this benchmark mostly measures how well each implementation recovers from a full ring.

## Main points

1. **Buffer size shows impact.** Every implementation speeds up a lot as the ring grows. Master improves about 800× from 256 to 65536, and the old implementation about 23×. A bigger ring is more slack, so the ring fills less often, and every implementation here is slow whenever it fills.
2. **The old implementation is much better in this scenario.** It wins at every size in the reliable runs, and the gap is widest on small rings, which fill most often. Changing master's wait (50µs, 500µs, yield, spin) never closed the gap, so the difference comes from how sequences are claimed, not from how the producer waits.
