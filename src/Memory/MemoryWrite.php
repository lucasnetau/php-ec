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
 * Value object emitted by rules to request a memory update.
 * Rules never modify memory directly; they emit MemoryWrite objects
 * which the MemoryEngine applies.
 */
class MemoryWrite implements \JsonSerializable
{
    /**
     * @param string $namespace The memory namespace (e.g. "sensor/42/state")
     * @param string $key The key within the namespace (e.g. "compressor")
     * @param mixed $value The value to store
     * @param int $ttl Time-to-live in seconds, 0 for no expiry
     * @param bool $persistent Whether to persist this entry across restarts
     */
    public function __construct(
        public readonly string $namespace,
        public readonly string $key,
        public readonly mixed $value,
        public readonly int $ttl = 0,
        public readonly bool $persistent = false,
    ) {}

    public function jsonSerialize(): array
    {
        return [
            'namespace' => $this->namespace,
            'key' => $this->key,
            'value' => $this->value,
            'ttl' => $this->ttl,
            'persistent' => $this->persistent,
        ];
    }
}
