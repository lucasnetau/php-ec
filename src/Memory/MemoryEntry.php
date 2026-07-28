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

/**
 * Value object representing a single memory entry.
 */
class MemoryEntry implements \JsonSerializable
{
    public function __construct(
        public readonly string $namespace,
        public readonly string $key,
        public mixed $value,
        public readonly ?DateTimeImmutable $expiresAt = null,
        public readonly bool $persistent = false,
    ) {}

    public function isExpired(): bool
    {
        return $this->expiresAt !== null && new \DateTimeImmutable('now') >= $this->expiresAt;
    }

    public function jsonSerialize(): array
    {
        return [
            'namespace' => $this->namespace,
            'key' => $this->key,
            'value' => $this->value,
            'expiresAt' => $this->expiresAt?->format('c'),
            'persistent' => $this->persistent,
        ];
    }

    public static function fromArray(array $data): self
    {
        return new self(
            namespace: $data['namespace'],
            key: $data['key'],
            value: $data['value'],
            expiresAt: isset($data['expiresAt']) && $data['expiresAt'] !== null
                ? new DateTimeImmutable($data['expiresAt'])
                : null,
            persistent: $data['persistent'] ?? false,
        );
    }
}
