<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Clocks;

use DateTimeImmutable;
use Psr\Clock\ClockInterface;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class BatchClock implements ClockInterface {

    private DateTimeImmutable $now;

    public function __construct(private ?LoopInterface $loop = null)
    {
        $this->loop ??= Loop::get();
        $this->now = new DateTimeImmutable('@0');
    }

    public function now(): DateTimeImmutable
    {
        return $this->now;
    }

    public function set(DateTimeImmutable $now): void
    {
        //Only progress the clock if going forward in time
        if ($now > $this->now) {
            $this->now = $now;
        }
    }
}
