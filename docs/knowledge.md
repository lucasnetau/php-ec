# PHP Event Correlator
## Collective Memory Architecture
**Version:** 1.0  
**Status:** Proposed  
**Target:** PHP Event Correlator vNext

---

# Overview

## Purpose

Introduce a **Collective Memory** subsystem that provides shared knowledge across all rules within the Event Correlator.

Instead of individual rules maintaining their own state or loading configuration independently, the Memory subsystem becomes the single source of truth for runtime knowledge.

This enables:

- Stateless rule implementations
- Shared context between rules
- Fast in-memory lookups
- Automatic TTL expiration
- Persistent knowledge across restarts
- Deterministic replay
- Future distributed execution

The Memory subsystem should be considered a peer to the Event Engine rather than an extension of it.

---

# Design Goals

## Primary Goals

- Non-blocking
- In-memory operation
- Shared by every rule
- Read-only from rule implementations
- Support TTL expiration
- Support persistent storage
- Restore automatically during startup
- Support asynchronous persistence
- Replay deterministic
- Easily unit testable

## Non Goals

The Memory subsystem is **not**

- A database
- An event history
- A cache of every event
- A distributed object store
- A replacement for configuration storage

---

# Conceptual Model

Events describe things that happened.

Memory describes things that are currently known.

```
                Event Stream
                     │
                     ▼
             Event Correlator
          ┌──────────┴──────────┐
          │                     │
          ▼                     ▼
     Rule Engine          Memory Engine
          │                     │
     Event Outputs       Memory Updates
          │                     │
          └──────────┬──────────┘
                     ▼
               Future Rules
```

Rules consume:

- Events
- Memory

Rules produce:

- Events
- Actions
- Memory Writes

---

# Memory Categories

Memory is divided into three logical lifetimes.

## 1. Ephemeral

Stored only in RAM.

Lost when the engine stops.

Examples

- compressor running
- last trend direction
- debounce state
- active cycle

Example

```
sensor/42/state/compressor = true
```

---

## 2. Cached

Stored in RAM.

Automatically expires after a TTL.

May optionally be persisted.

Examples

```
weather/outsideTemperature

ttl = 30 minutes
```

```
sensor/42/currentMode

ttl = 15 minutes
```

---

## 3. Persistent

Never expires.

Written asynchronously.

Reloaded on startup.

Examples

```
sensor/42/config/type = freezer

sensor/42/config/location = Kitchen

site/timezone = Australia/Sydney
```

---

# Memory Structure

Memory should be implemented as a hierarchical key/value store.

```
namespace
    key
        value
```

Examples

```
sensor/42/config/type

sensor/42/config/setpoint

sensor/42/state/compressor

sensor/42/state/lastCycle

site/timezone

weather/current
```

This allows efficient indexing while remaining storage independent.

---

# Memory Entry

```php
class MemoryEntry
{
    public string $namespace;

    public string $key;

    public mixed $value;

    public ?DateTimeImmutable $expiresAt;

    public bool $persistent;
}
```

---

# Memory Engine

The Memory Engine owns all memory.

Rules never directly modify storage.

```
Rule

↓

MemoryWrite

↓

Memory Engine

↓

Storage Backend
```

This ensures

- one source of truth
- deterministic replay
- central TTL handling
- asynchronous persistence

---

# Rule Outputs

Currently Rules produce

```
Events
Actions
```

This proposal introduces

```
MemoryWrite
```

Result model becomes

```php
RuleResult
{
    Event[];

    Action[];

    MemoryWrite[];
}
```

---

# MemoryWrite

```php
new MemoryWrite(
    namespace: "sensor/42/state",
    key: "compressor",
    value: true,
    ttl: 300
);
```

The Rule does **not** modify Memory directly.

Instead it emits a MemoryWrite.

The Memory Engine applies the update.

---

# Rule Context

Rules should access Memory through the RuleContext.

```
RuleContext
    Event
    Memory
    Clock
    Logger
```

Example

```php
public function evaluate(
    RuleContext $context
): RuleResult
```

Memory access

```php
$config = $context
    ->memory()
    ->get(
        "sensor/42/config",
        "type"
    );
```

---

# Why Not Singleton?

Singletons introduce several issues.

- difficult testing
- hidden dependencies
- difficult replay
- multiple engine instances
- future distributed execution

Injecting Memory through RuleContext keeps Rules deterministic and testable.

---

# Memory Interface

Rules should only receive read access.

```php
interface Memory
{
    public function get(
        string $namespace,
        string $key
    ): mixed;

    public function has(
        string $namespace,
        string $key
    ): bool;

    public function all(
        string $namespace
    ): iterable;
}
```

Notice there is intentionally **no**

```php
set()
```

Rules never mutate Memory.

---

# Internal Storage

The in-memory implementation should use native PHP arrays.

```
Memory

[
    "sensor/42" => [

        "config" => [...],

        "state" => [...]

    ]
]
```

Lookup complexity

```
O(1)
```

This provides extremely fast access with minimal overhead.

---

# TTL Management

TTL expiration should not scan the entire Memory collection.

Instead maintain a priority queue (min-heap).

```
Expiry Queue

12:00 sensor42

12:01 weather

12:05 compressor

12:10 occupancy
```

Periodic timer

```
peek()

expired?

remove

repeat
```

Complexity

```
Insert

O(log n)

Expiry

O(log n)

Lookup

O(1)
```

---

# Persistence

Persistence should be abstracted.

```
Memory Engine

↓

Memory Backend
```

Possible implementations

```
InMemoryBackend

SQLiteBackend

RedisBackend

PostgresBackend

FilesystemBackend

TimescaleBackend
```

Only Persistent entries are written.

TTL entries may optionally be persisted.

---

# Startup Sequence

```
Start Engine

↓

Load Memory Backend

↓

Populate Memory

↓

Begin Event Processing
```

Rules immediately have access to configuration without performing their own loading.

---

# Event Enrichment

Memory can enrich incoming Events before Rule evaluation.

Incoming

```
TemperatureReading

sensor = 42
```

Memory

```
sensor/42/config/type = freezer

sensor/42/config/location = Kitchen

sensor/42/config/setpoint = -18
```

Rule receives

```
TemperatureReading

sensor = 42

type = freezer

location = Kitchen

setpoint = -18
```

Rules become simpler because static context is automatically attached.

---

# Rule Collaboration

One Rule may compute expensive analytics.

Instead of every Rule repeating the calculation

```
Rule A

↓

Calculate Trend

↓

MemoryWrite
```

Subsequent Rules

```
Rule B

↓

Read Trend
```

```
Rule C

↓

Read Trend
```

Shared computation greatly reduces duplicated processing.

Examples include

- operating mode
- compressor state
- LOESS trend
- rolling averages
- baseline values
- learned thresholds

---

# Memory Lifecycle

```
Rule

↓

MemoryWrite

↓

Memory Engine

↓

Store In Memory

↓

(Optional)

Persist

↓

Future Rules Read Memory
```

---

# Threading Model

The Memory Engine operates entirely inside the Event Correlator event loop.

Requirements

- no blocking operations
- asynchronous persistence
- lock free
- immediate visibility after MemoryWrite
- deterministic ordering

Persistence should never block Rule execution.

---

# Future Extensions

The proposed architecture naturally supports

## Facts

Long-lived knowledge

```
sensor.type

sensor.location

site.timezone
```

---

## Runtime State

Current operating state

```
compressor.running

pump.enabled

cycle.active
```

---

## Derived Knowledge

Shared analytical outputs

```
trend

rollingAverage

predictedRuntime

operatingMode
```

---

## Learned Behaviour

Machine learning or adaptive outputs

```
normalDutyCycle

baselineTemperature

expectedPressure
```

---

## Distributed Memory

Future implementations could synchronise Memory between multiple Event Correlator instances while preserving the same Rule API.

---

# Benefits

Compared to per-rule state

| Traditional Rule State | Collective Memory |
|------------------------|------------------|
| Duplicate configuration | Shared knowledge |
| Rules own state | Memory Engine owns state |
| Hard to test | Deterministic |
| No shared context | Shared context |
| Duplicate calculations | Shared analytics |
| Difficult persistence | Built-in persistence |
| Restart loses state | Automatic restore |

---

# Summary

The Collective Memory subsystem transforms the Event Correlator from an event-processing engine into a knowledge-driven analytics platform.

Key principles:

- Memory is separate from Events.
- Rules are stateless and deterministic.
- Rules read Memory but never modify it directly.
- Rules emit MemoryWrite objects.
- The Memory Engine owns lifecycle, TTLs, persistence and restoration.
- Memory is shared across all Rules.
- Startup restores knowledge before processing events.
- Future analytics can build on shared knowledge instead of recomputing state.

This architecture provides a scalable foundation for advanced detectors, shared analytics, event enrichment, adaptive algorithms and future distributed deployments while remaining fully compatible with the existing event-driven design.