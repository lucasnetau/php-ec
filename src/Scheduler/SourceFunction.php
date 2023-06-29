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

use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

abstract class SourceFunction implements LoggerAwareInterface, EventEmitterInterface
{
    /** We emit events */
    use EventEmitterTrait;

    use LoggerAwareTrait;

    protected bool $running = false;
    protected LoopInterface $loop;

    public function __construct() {
        $this->loop = Loop::get();
    }

    public function start(): void
    {
        $this->running = true;
        $this->functionStart();
    }

    public function terminate($signal): void
    {
        if ($this->running === false) {
            return;
        }
        $this->functionStop();
    }

    public function isStopped() : bool {
        return !$this->running;
    }

    abstract function functionStart() : void;

    abstract function functionStop() : void;

}
