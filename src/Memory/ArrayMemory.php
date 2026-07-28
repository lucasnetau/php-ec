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

/**
 * Native PHP array storage for Collective Memory.
 * Provides O(1) lookups via namespace/key hierarchy.
 */
class ArrayMemory implements MemoryInterface
{
    /** @var array<string, array<string, MemoryEntry>> */
    private array $store = [];

    /** @var array<int, list{string, string}> Min-heap of [expiresAt timestamp, namespace, key] for TTL expiry */
    private array $expiryQueue = [];

    private bool $expiryQueueDirty = false;

    public function get(string $namespace, string $key): mixed
    {
        $entry = $this->store[$namespace][$key] ?? null;
        if ($entry === null || $entry->isExpired()) {
            return null;
        }
        return $entry->value;
    }

    public function has(string $namespace, string $key): bool
    {
        $entry = $this->store[$namespace][$key] ?? null;
        return $entry !== null && !$entry->isExpired();
    }

    public function all(string $namespace): iterable
    {
        if (!isset($this->store[$namespace])) {
            return [];
        }
        foreach ($this->store[$namespace] as $key => $entry) {
            if (!$entry->isExpired()) {
                yield $key => $entry->value;
            }
        }
    }

    /**
     * Store a memory entry (called by MemoryEngine, not by rules).
     */
    public function set(MemoryEntry $entry): void
    {
        $this->store[$entry->namespace][$entry->key] = $entry;
        if ($entry->expiresAt !== null) {
            $this->expiryQueue[] = [$entry->expiresAt->getTimestamp(), $entry->namespace, $entry->key];
            $this->expiryQueueDirty = true;
        }
    }

    /**
     * Remove a specific entry.
     */
    public function delete(string $namespace, string $key): void
    {
        unset($this->store[$namespace][$key]);
        if (empty($this->store[$namespace])) {
            unset($this->store[$namespace]);
        }
        $this->expiryQueueDirty = true;
    }

    /**
     * Expire all entries whose TTL has passed.
     * Returns the number of entries expired.
     */
    public function purgeExpired(): int
    {
        if ($this->expiryQueueDirty) {
            usort($this->expiryQueue, fn(array $a, array $b) => $a[0] <=> $b[0]);
            $this->expiryQueueDirty = false;
        }

        $now = time();
        $expired = 0;

        while (!empty($this->expiryQueue) && $this->expiryQueue[0][0] <= $now) {
            [$ts, $ns, $key] = array_shift($this->expiryQueue);
            $entry = $this->store[$ns][$key] ?? null;
            if ($entry !== null && $entry->isExpired()) {
                $this->delete($ns, $key);
                $expired++;
            }
        }

        return $expired;
    }

    /**
     * Get all entries (for persistence).
     * @return MemoryEntry[]
     */
    public function allEntries(): array
    {
        $entries = [];
        foreach ($this->store as $namespace => $keys) {
            foreach ($keys as $key => $entry) {
                if (!$entry->isExpired()) {
                    $entries[] = $entry;
                }
            }
        }
        return $entries;
    }

    /**
     * Populate from an array of entries (for restore from backend).
     * @param MemoryEntry[] $entries
     */
    public function populate(array $entries): void
    {
        $this->store = [];
        $this->expiryQueue = [];
        foreach ($entries as $entry) {
            $this->set($entry);
        }
    }

    /**
     * Clear all entries.
     */
    public function clear(): void
    {
        $this->store = [];
        $this->expiryQueue = [];
    }
}
