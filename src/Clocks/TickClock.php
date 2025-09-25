<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Clocks;

use DateTimeImmutable;
use DateTimeZone;
use Psr\Clock\ClockInterface;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class TickClock implements ClockInterface {

    private ?DateTimeImmutable $now = null;

    private DateTimeZone $timeZone;

    public function __construct(private ?LoopInterface $loop = null)
    {
        $this->loop ??= Loop::get();
        $this->timeZone = new DateTimeZone('UTC');
    }

    public function now(): DateTimeImmutable
    {
        if ($this->now === null) {
            $this->now = (new DateTimeImmutable('now', $this->timeZone))->setTimezone($this->timeZone);

            // remember clock for current loop tick only and update on next tick
            $this->loop->futureTick(function () {
                $this->now = null;
            });
        }

        return $this->now;
    }
}
