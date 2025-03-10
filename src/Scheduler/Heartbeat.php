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
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class Heartbeat implements EventEmitterInterface {
    use EventEmitterTrait;

    private float $start;
    private int $seq;

    const CONVERSION_FACTOR = 1e+3; //Convert Nano to Microseconds

    public function __construct(protected float|int $heartbeatIntervalSeconds) {}

    public function start(LoopInterface|null $loop = null, $delay = 0): void
    {
        $this->seq = 0;
        $this->start = hrtime(true);
        // @codeCoverageIgnoreStart
        if ($loop === null) {
            $loop = Loop::get();
        }
        // @codeCoverageIgnoreEnd

        $loop->addTimer($delay, function() use ($loop) {
            $loop->futureTick($this->pulse(...));
            $loop->addPeriodicTimer($this->heartbeatIntervalSeconds, $this->pulse(...));
        });
    }

    protected function pulse(): void
    {
        $this->emit('pulse', [
            'runtime' => (int)round((hrtime(true)-$this->start)/self::CONVERSION_FACTOR),
            'seq' => $this->seq++,
        ]);
    }
}