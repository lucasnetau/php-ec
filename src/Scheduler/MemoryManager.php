<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation\Scheduler;

use Closure;
use EdgeTelemetrics\EventCorrelation\SysInfo;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;

use function function_exists;
use function gc_collect_cycles;
use function gc_mem_caches;
use function hrtime;
use function max;
use function memory_get_peak_usage;
use function memory_get_usage;
use function memory_reset_peak_usage;
use function number_format;
use function round;

/**
 * Monitors PHP memory usage and mitigates memory pressure by pausing/resuming
 * input processes and triggering shutdown when mitigation fails.
 *
 * Extracted from Scheduler to isolate the memory pressure state machine from
 * scheduler coordination logic. The Scheduler creates this manager, wires it to
 * the source/action coordinators, and delegates the legacy
 * checkMemoryPressure()/memoryReclaim() methods here.
 */
class MemoryManager implements LoggerAwareInterface
{
    use LoggerAwareTrait;

    /** @var int Memory limit in bytes (-1 = unlimited, 0 = not yet detected) */
    protected int $memoryLimit = 0;

    /** @var int Last calculated memory percentage used */
    protected int $currentMemoryPercentUsed = 0;

    /** @var bool Whether input processes are currently paused due to memory pressure */
    protected bool $paused = false;

    /** @var int How many times memory pressure mitigation has been triggered */
    protected int $pausedCount = 0;

    /** @var TimerInterface|null Periodic check timer (every 2 seconds) */
    protected ?TimerInterface $checkTimer = null;

    /** @var TimerInterface|null Pressure timeout timer (fires while paused) */
    protected ?TimerInterface $pressureTimer = null;

    /** @var float Interval (seconds) for the pressure-stuck watchdog timer */
    protected float $pressureCheckInterval = 300;

    /**
     * @param LoopInterface $loop
     * @param Closure $inflightActionCount Returns the current number of inflight actions (int)
     * @param Closure $pauseSources Pauses all input processes
     * @param Closure $resumeSources Resumes all input processes
     * @param Closure $shutdown Triggers scheduler shutdown
     * @param int $highWatermark Memory percentage at which mitigation is undertaken
     * @param int $lowWatermark Memory percentage at which mitigation is considered resolved
     * @param int $actionLimitHigh Max outstanding actions before mitigation
     * @param int $actionLimitLow Outstanding action count at which mitigation is resolved
     */
    public function __construct(
        protected LoopInterface $loop,
        protected Closure $inflightActionCount,
        protected Closure $pauseSources,
        protected Closure $resumeSources,
        protected Closure $shutdown,
        protected int $highWatermark,
        protected int $lowWatermark,
        protected int $actionLimitHigh,
        protected int $actionLimitLow,
    ) {
        $this->logger = new NullLogger();
    }

    /**
     * Detect the PHP memory limit from the runtime environment.
     */
    public function initialise(): void
    {
        $sysInfo = new SysInfo();
        $this->memoryLimit = $sysInfo->getMemoryLimit();
        $allowable = $sysInfo->getAllowableMemoryLimit();
        $percentage = $this->memoryLimit === SysInfo::NO_LIMIT ? 100 : ($this->memoryLimit / $allowable) * 100;
        $this->logger->debug(
            "Memory limit set to {bytes} Bytes {percent}% of {total} Total Allowable",
            ['bytes' => $this->memoryLimit, 'total' => $allowable, 'percent' => number_format($percentage, 2)]
        );
    }

    /**
     * Start the periodic memory pressure check timer (every 2 seconds).
     */
    public function start(): void
    {
        $this->checkTimer = $this->loop->addPeriodicTimer(2, $this->check(...));
    }

    /**
     * Cancel all timers owned by this manager.
     */
    public function stop(): void
    {
        if ($this->checkTimer !== null) {
            $this->loop->cancelTimer($this->checkTimer);
            $this->checkTimer = null;
        }
        if ($this->pressureTimer !== null) {
            $this->loop->cancelTimer($this->pressureTimer);
            $this->pressureTimer = null;
        }
    }

    /**
     * Compare memory usage against the limit and watermarks, pausing or resuming
     * input processes as needed. When paused and inflight actions are not reducing,
     * trigger shutdown.
     */
    public function check(): void
    {
        if ($this->memoryLimit === 0) {
            $sysInfo = new SysInfo();
            $this->memoryLimit = $sysInfo->getMemoryLimit();
        } elseif ($this->memoryLimit === SysInfo::NO_LIMIT) {
            return; // unlimited memory, disable pressure checks
        }

        $current_memory_usage = $this->currentMemoryUsage();

        $percent_used = (int)round(($current_memory_usage / $this->memoryLimit) * 100);

        /** Try releasing memory first and recalculate percentage used */
        if ($percent_used >= $this->highWatermark) {
            /** Running this every check cycle negatively impacts the scheduler's performance,
             *   however, since we are paused (or going to pause) at this stage, and are awaiting the external action processes to complete the actual impact will be minimal
             */
            $this->reclaim();
            $current_memory_usage = $this->currentMemoryUsage();
            $percent_used = (int)round(($current_memory_usage / $this->memoryLimit) * 100);
        }

        $this->currentMemoryPercentUsed = $percent_used;

        if (false === $this->paused &&
            ($percent_used >= $this->highWatermark ||
                ($this->inflightActionCount)() > $this->actionLimitHigh)
        ) {
            $inflight = ($this->inflightActionCount)();
            $this->logger->warning(
                "Currently using $percent_used% of memory limit with $inflight inflight actions. Pausing input processes");

            ($this->pauseSources)();
            $this->paused = true;
            ++$this->pausedCount;

            $inflightActionCount = $inflight;
            /** @TODO take into account delaying shutdown if we still have some outstanding actions and memory usage is dropping */
            $this->pressureTimer = $this->loop->addPeriodicTimer($this->pressureCheckInterval, function() use (&$inflightActionCount) {
                $currentActionCount = ($this->inflightActionCount)();
                if ($currentActionCount < $inflightActionCount) {
                    $this->logger->debug("Current action count dropped from {old} to {new}", ['old' => $inflightActionCount, 'new' => $currentActionCount]);;
                }
                if ($this->paused && $currentActionCount >= $inflightActionCount) {
                    $this->logger->critical("Timeout! Input processes are still paused and inflight actions not reducing, shutting down");
                    ($this->shutdown)();
                }
                $inflightActionCount = $currentActionCount;
            });
        } else {
            if ($this->paused &&
                $percent_used <= $this->lowWatermark &&
                ($this->inflightActionCount)() < $this->actionLimitLow) {

                //Cancel the memory pressure timeout
                if ($this->pressureTimer !== null) {
                    $this->loop->cancelTimer($this->pressureTimer);
                    $this->pressureTimer = null;
                }
                $this->reclaim();
                //Resume input
                ($this->resumeSources)();
                $this->paused = false;
            }
        }
    }

    /**
     * Run PHP garbage collection and release memory caches.
     */
    public function reclaim(): void {
        $mark = hrtime(true);
        $memCurr = $this->currentMemoryUsage();
        $cycles = gc_collect_cycles();
        $bytes = gc_mem_caches();
        $saved = max(0, $memCurr - $this->currentMemoryUsage()); //Don't show a negative value if we don't release anything
        $time = (int)round((hrtime(true)-$mark)/1e+3);
        $this->logger->debug("GC Run Complete in $time μs {cycles: $cycles, reclaim: $bytes, reduced: $saved bytes, current: " . round($this->currentMemoryUsage() / 1048576,2) . "MB, max: " . round(memory_get_peak_usage() / 1048576,2) ."MB}");
        if (function_exists('memory_reset_peak_usage')) {
            \memory_reset_peak_usage();
        }
    }

    /**
     * @return int The detected memory limit in bytes (-1 for unlimited, 0 if not yet detected)
     */
    public function getMemoryLimit(): int
    {
        return $this->memoryLimit;
    }

    /**
     * @return int The last calculated memory percentage used
     */
    public function getPercentUsed(): int
    {
        return $this->currentMemoryPercentUsed;
    }

    /**
     * @return bool Whether input processes are currently paused due to memory pressure
     */
    public function isPaused(): bool
    {
        return $this->paused;
    }

    /**
     * @return int How many times memory pressure mitigation has been triggered
     */
    public function getPausedCount(): int
    {
        return $this->pausedCount;
    }

    /**
     * Override in test doubles to control reported memory usage.
     */
    protected function currentMemoryUsage(): int
    {
        return memory_get_usage();
    }
}
