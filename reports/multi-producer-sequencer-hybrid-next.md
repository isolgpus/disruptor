# Hybrid `MultiProducerSequencer.next()`: claim with `getAndAdd` only when there's room

## TL;DR

- **The change:** `next()` keeps master's single `getAndAdd` claim while the ring has room. When the ring is full, it switches to the old check-then-CAS loop, so a waiting producer never holds a claimed-but-unpublished sequence.
- **It fixes the full-buffer case.** With 2 busy producers, 80 mostly-idle publishers and 1 consumer, the hybrid is about **20–175× faster than master** from 256 to 16384 slots, and about 7–8× faster at 32768–65536. It matches or beats the old CAS implementation at every size.
- **The trade-off: it's slower than master when the buffer has room.** With 2 producers and a 65536 ring that rarely fills, the hybrid does **21.5 ops/µs against master's 30.7, about 30% slower**. The error bars don't overlap. It's still faster than the old CAS implementation (17.0 ops/µs).
- **In short,** the hybrid trades about a third of master's throughput when the ring has room for orders of magnitude better throughput when it's full.

## Background

An earlier benchmark (`reports/multi-producer-sequencer-next-idle-publishers-benchmark.md`) found that master's `next()` collapses when the ring buffer is full. With many producers, throughput dropped to almost zero on small and medium rings. An older revision (`83db6c9e`) with a different `next()` did far better in the same setup.

The two versions differ in **when a producer claims its sequence**:

- **Master: claim, then wait.** A producer takes a sequence immediately with `cursor.getAndAdd(n)`. If the ring is full, it then parks while **holding** that sequence.
- **Old: check, then claim.** A producer first checks there's room. If not, it parks **without** claiming anything, and claims with `compareAndSet` only once there's space.

The consumer reads sequences strictly in order. In master, a producer parked on a full ring holds a sequence it can't publish yet, so the consumer can't move past it, even if later sequences are already published. With many producers, many sequences end up claimed but unpublished, and the consumer can only advance as fast as each sleeping producer wakes up, in sequence order. The old version never has this problem, because waiting producers hold nothing.

The old version pays for this when the ring has room. A CAS loop costs more than a single `getAndAdd` under contention, because failed CAS attempts have to retry. Master's design is the faster one in that case, and the measurements below confirm it.

## The change

`next(n)` in `src/main/java/com/lmax/disruptor/MultiProducerSequencer.java` now has two paths:

```java
final long observed = cursor.get();
final long cachedGating = gatingSequenceCache.get();

// Fast path: the cached gating sequence shows room, so claim unconditionally with a single getAndAdd.
if (observed + n - bufferSize <= cachedGating && cachedGating <= observed)
{
    final long current = cursor.getAndAdd(n);
    final long nextSequence = current + n;
    final long wrapPoint = nextSequence - bufferSize;
    final long cachedGatingSequence = gatingSequenceCache.get();

    if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
    {
        // Other producers claimed between our check and getAndAdd, so we're beyond capacity; wait it out.
        long gatingSequence;
        while (wrapPoint > (gatingSequence = Util.getMinimumSequence(gatingSequences, current)))
        {
            LockSupport.parkNanos(1L);
        }
        gatingSequenceCache.set(gatingSequence);
    }
    return nextSequence;
}

// Slow path: near (or at) full. Only claim once capacity is confirmed, so a waiting producer never holds an
// unpublished sequence that would stall the consumer for everyone else.
while (true)
{
    final long current = cursor.get();
    final long nextSequence = current + n;
    final long wrapPoint = nextSequence - bufferSize;
    final long gatingSequence = Util.getMinimumSequence(gatingSequences, current);
    gatingSequenceCache.set(gatingSequence);

    if (wrapPoint > gatingSequence)
    {
        LockSupport.parkNanos(1L);
    }
    else if (cursor.compareAndSet(current, nextSequence))
    {
        return nextSequence;
    }
}
```

- **Fast path (the ring has room):** this is master's behaviour, a single `getAndAdd`. If other producers squeeze in between the check and the claim, the producer can still end up beyond capacity. In that case it falls back to master's wait, so correctness never depends on the check being exact.
- **Slow path (the ring is full):** this is the old behaviour. The producer refreshes the consumer position, parks if there's no room, and claims with a CAS only once there is.
- **`cachedGating <= observed`:** a sanity check on the cached consumer position, copied from master's own condition. The cache can be ahead of the cursor if the cursor is moved backwards (`claim()` / `RingBuffer.claimAndGetPreallocated`). The check sends those cases to the slow path, which refreshes the cache.

## How it was measured

JMH benchmark `MultiProducerSequencerNextBenchmark` (in `src/jmh/java/com/lmax/disruptor/`). All runs used **2 forks with 5 warmup and 5 measurement iterations of 1s**. Scores are throughput in ops/µs, totalled across the producer threads named, ± the JMH 99.9% error. One op is one `next()` plus one `publish()`.

Two scenarios:

| Scenario | Benchmark group | Threads | What it represents |
|---|---|---|---|
| **Full buffer** | `BusyAndIdleProducers` | 2 busy producers, 1 consumer, 80 idle publishers (each publishes once per ms) | Lots of producers. The ring is full much of the time. |
| **Buffer has room** | `ProducersOneConsumer` | 2 producers, 1 consumer | Few producers and a large ring. The ring rarely fills. |

In both, the consumer only advances its sequence to the highest published sequence, freeing slots; it doesn't process events.

Implementations compared:

| Name | Source | `next()` |
|---|---|---|
| **master** | `c871ca49` | Claim with `getAndAdd`, then wait |
| **hybrid** | This change, on master | `getAndAdd` when there's room, check-then-CAS when full |
| **old** | `83db6c9e` | Check-then-CAS always |

- **Environment:** 8 cores, Java 21, JMH 1.35, no CPU pinning.
- **The old revision** uses Gradle 6.5, which won't run on Java 21. It was built by compiling its `src/main` with the same benchmark source and JMH version as the other two, so all three use identical benchmark code.

## Results

### Full buffer: `BusyAndIdleProducers` (`-tg 2,1,80`, `idleIntervalNanos=1000000`)

Busy-producer throughput, ops/µs:

| bufferSize | master | hybrid | old | hybrid ÷ master |
|---:|---:|---:|---:|---:|
| 256 | 0.003 ± 0.001 | **0.529** ± 0.465 | 0.527 ± 0.420 | ~175× |
| 512 | 0.005 ± 0.005 | **0.779** ± 0.933 | 0.375 ± 0.286 | ~155× |
| 1024 | 0.026 ± 0.011 | **4.721** ± 7.738 | 0.520 ± 0.388 | ~180× |
| 2048 | 0.034 ± 0.014 | **2.676** ± 3.933 | 0.676 ± 0.707 | ~80× |
| 4096 | 0.109 ± 0.062 | **12.883** ± 11.464 | 0.770 ± 0.720 | ~120× |
| 8192 | 0.274 ± 0.318 | **10.122** ± 12.076 | 1.211 ± 0.746 | ~37× |
| 16384 | 0.629 ± 0.438 | **15.652** ± 8.013 | 3.124 ± 2.094 | ~25× |
| 32768 | 2.257 ± 2.424 | **16.218** ± 8.272 | 5.684 ± 3.400 | ~7× |
| 65536 | 2.347 ± 4.475 | **19.261** ± 7.903 | 12.284 ± 7.124 | ~8× |

- **Master collapses on a full buffer.** It's almost zero up to 2048 slots and only recovers on very large rings, which fill less often.
- **The hybrid fixes this.** It ties with old at 256 and beats both master and old at every other size. Its error bars are wide, sometimes wider than the score itself, but the gap to master is large enough to be clear at every size.

### Buffer has room: `ProducersOneConsumer` (`-tg 1,2`)

Producer throughput, ops/µs:

| bufferSize | master | hybrid | old | hybrid ÷ master |
|---:|---:|---:|---:|---:|
| 1024 | **7.886** ± 10.888 | 4.918 ± 2.794 | 1.887 ± 2.333 | ~0.6× |
| 65536 | **30.650** ± 3.038 | 21.463 ± 3.340 | 17.031 ± 4.892 | **~0.7×** |

- **Master is fastest when the buffer has room.** This is the case its `getAndAdd` design is built for.
- **The hybrid is about 30% slower than master at 65536.** The error bars don't overlap (hybrid upper bound 24.8, master lower bound 27.6), so this is a real cost, not noise. The most likely cause is the extra `cursor.get()` before `getAndAdd` in the fast path: under contention, each claim then touches the cursor's cache line twice, a read followed by a write. This hasn't been profiled.
- **The hybrid is still faster than the old CAS implementation,** which is about 45% below master here.
- **The 1024 row is too noisy to rank.** Master's error bar is larger than its score, and with two fast producers a 1024 ring probably fills fairly often even without idle publishers.

### Summary

| | Full buffer | Buffer has room (65536) |
|---|---|---|
| **master** | Collapses (0.003–2.3 ops/µs) | **Best** (30.7) |
| **hybrid** | **Best** (0.5–19.3 ops/µs) | About 30% below master (21.5) |
| **old** | Good (0.4–12.3 ops/µs) | Worst (17.0) |

## The trade-off

**This change helps the full-buffer case, at the cost of throughput when the buffer has room.**

- **Gain:** when the ring is full, producers no longer stall the consumer. Throughput goes from nearly zero to usable, typically 20–175× master on small and medium rings.
- **Cost:** when the ring has room, every `next()` does one extra read of the shared cursor before claiming. That costs about 30% of master's throughput in the 2-producer, 65536-slot benchmark.
- **Who benefits:** systems with many producers, small or medium rings, or consumers that sometimes fall behind, where the ring does fill. For systems whose ring practically never fills, master's current `next()` is faster.


To reproduce:

```
./gradlew jmhJar
java -jar build/libs/disruptor-*-jmh.jar MultiProducerSequencerNextBenchmark.BusyAndIdleProducers \
  -f 2 -wi 5 -i 5 -p idleIntervalNanos=1000000 \
  -p bufferSize=256,512,1024,2048,4096,8192,16384,32768,65536 -tg 2,1,80
java -jar build/libs/disruptor-*-jmh.jar MultiProducerSequencerNextBenchmark.ProducersOneConsumer \
  -f 2 -wi 5 -i 5 -p bufferSize=1024,65536 -tg 1,2
```
