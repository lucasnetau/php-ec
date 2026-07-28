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
 * Read-only interface for accessing Collective Memory.
 * Rules receive this interface and cannot modify memory directly.
 */
interface MemoryInterface
{
    /**
     * Get a value by namespace and key.
     * @return mixed The stored value, or null if not found / expired
     */
    public function get(string $namespace, string $key): mixed;

    /**
     * Check if a namespace/key pair exists and is not expired.
     */
    public function has(string $namespace, string $key): bool;

    /**
     * Get all non-expired key=>value pairs for a namespace.
     * @return iterable<string, mixed>
     */
    public function all(string $namespace): iterable;
}
