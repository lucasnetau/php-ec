<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation\Memory;

use DateTimeImmutable;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;

/**
 * The Memory Engine owns all Collective Memory.
 *
 * - Rules never modify storage directly; they emit MemoryWrite objects.
 * - This engine applies writes, manages TTL expiry, and handles persistence.
 * - Operates entirely within the event loop (non-blocking).
 */
class MemoryEngine implements MemoryInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    private ArrayMemory $store;
    private ?MemoryBackendInterface $backend;
    private bool $dirty = false;

    public function __construct(?MemoryBackendInterface $backend = null)
    {
        $this->store = new ArrayMemory();
        $this->backend = $backend;
        $this->logger = new NullLogger();
    }

    // ── Read-only interface (delegates to store) ──────────────────────

    public function get(string $namespace, string $key): mixed
    {
        return $this->store->get($namespace, $key);
    }

    public function has(string $namespace, string $key): bool
    {
        return $this->store->has($namespace, $key);
    }

    public function all(string $namespace): iterable
    {
        return $this->store->all($namespace);
    }

    // ── Write operations (called by engine, not by rules) ─────────────

    /**
     * Apply a single MemoryWrite from a rule.
     */
    public function applyWrite(MemoryWrite $write): void
    {
        $expiresAt = $write->ttl !== 0
            ? new DateTimeImmutable(($write->ttl > 0 ? '+' : '') . "{$write->ttl} seconds")
            : null;

        $entry = new MemoryEntry(
            namespace: $write->namespace,
            key: $write->key,
            value: $write->value,
            expiresAt: $expiresAt,
            persistent: $write->persistent,
        );

        $this->store->set($entry);
        $this->dirty = true;
    }

    /**
     * Apply multiple MemoryWrite objects from a rule result.
     * @param MemoryWrite[] $writes
     */
    public function applyWrites(array $writes): void
    {
        foreach ($writes as $write) {
            $this->applyWrite($write);
        }
    }

    // ── TTL management ────────────────────────────────────────────────

    /**
     * Purge expired entries. Should be called periodically.
     * Returns the number of entries removed.
     */
    public function purgeExpired(): int
    {
        return $this->store->purgeExpired();
    }

    // ── Persistence ───────────────────────────────────────────────────

    /**
     * Load entries from the backend into memory.
     * Called during startup before event processing begins.
     */
    public function loadFromBackend(): void
    {
        if ($this->backend === null) {
            return;
        }

        try {
            $entries = $this->backend->load();
        } catch (\Throwable $ex) {
            //Never treat a corrupt/unreadable backend as "no memory": report it and
            //continue with whatever was restored from the saved state snapshot.
            $this->logger->error('Memory backend load failed; continuing without persisted memory', ['exception' => $ex]);
            return;
        }

        // Merge backend entries into memory without overwriting entries already
        // restored from the saved state snapshot, which is the more recent knowledge.
        $loaded = 0;
        foreach ($entries as $entry) {
            if ($entry->isExpired() || $this->store->has($entry->namespace, $entry->key)) {
                continue;
            }
            $this->store->set($entry);
            $loaded++;
        }

        $this->logger->debug("Memory engine loaded {count} entries from backend", ['count' => $loaded]);
    }

    /**
     * Persist current memory state to the backend.
     * Only persistent entries are saved.
     */
    public function persist(): void
    {
        if ($this->backend === null || !$this->dirty) {
            return;
        }

        $all = $this->store->allEntries();
        $persistent = array_values(array_filter($all, fn(MemoryEntry $e) => $e->persistent));

        $this->backend->save($persistent);
        $this->dirty = false;
        $this->logger->debug("Memory engine persisted {count} entries", ['count' => count($persistent)]);
    }

    public function isDirty(): bool
    {
        return $this->dirty;
    }

    public function clearDirtyFlag(): void
    {
        $this->dirty = false;
    }

    /**
     * Get all entries for state serialisation.
     * @return MemoryEntry[]
     */
    public function getState(): array
    {
        return $this->store->allEntries();
    }

    /**
     * Restore state from a serialised array.
     * @param array $state Array of serialised entry data
     */
    public function setState(array $state): void
    {
        $entries = [];
        foreach ($state as $data) {
            if ($data instanceof MemoryEntry) {
                $entries[] = $data;
            } elseif (isset($data['namespace'], $data['key'], $data['value'])) {
                $entries[] = MemoryEntry::fromArray($data);
            }
        }
        $this->store->populate($entries);
    }

    /**
     * Clear all memory.
     */
    public function clear(): void
    {
        $this->store->clear();
    }
}
