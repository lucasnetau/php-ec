<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/*
 * Simple metrics collector that logs aggregated counters via a PSR‑3 logger.
 */
namespace EdgeTelemetrics\EventCorrelation\Scheduler;

use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;

final class MetricsCollector implements LoggerAwareInterface
{
    use LoggerAwareTrait;

    /** @var array<string, int|float>  metric name => current value */
    private array $counters = [];

    public function __construct(?LoggerInterface $logger = null)
    {
        $this->logger = $logger;
    }

    /**
     * Increment (or decrement) a counter.
     *
     * @param string $group   Logical group, e.g. "events", "actions"
     * @param string $name    Specific metric name, e.g. "seen", "inflight"
     * @param int|float $step Amount to add (default 1)
     */
    public function inc(string $group, string $name, int|float $step = 1): void
    {
        $key = $group . '.' . $name;
        $this->counters[$key] = ($this->counters[$key] ?? 0) + $step;
    }

    /**
     * Set an absolute value (useful for gauges such as memory‑percentage).
     */
    public function set(string $group, string $name, int|float $value): void
    {
        $this->counters[$group . '.' . $name] = $value;
    }

    /**
     * Emit the current snapshot to the logger at INFO level.
     * Each metric is logged as a separate line – this keeps the output
     * readable in log aggregators.
     */
    public function logSnapshot(): void
    {
        foreach ($this->counters as $key => $value) {
            $this->logger?->info('metric', ['metric' => $key, 'value' => $value]);
        }
    }

    /**
     * Schedule periodic logging on the given event loop.
     *
     * @param \React\EventLoop\LoopInterface $loop
     * @param int $intervalSeconds Interval in seconds (default = 60)
     */
    public function schedulePeriodicLogging(\React\EventLoop\LoopInterface $loop, int $intervalSeconds = 60): void
    {
        $loop->addPeriodicTimer($intervalSeconds, fn() => $this->logSnapshot());
    }
}