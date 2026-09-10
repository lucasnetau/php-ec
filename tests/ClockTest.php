<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use DateTimeImmutable;
use EdgeTelemetrics\EventCorrelation\Clocks\BatchClock;
use EdgeTelemetrics\EventCorrelation\Clocks\TickClock;
use PHPUnit\Framework\TestCase;
use React\EventLoop\StreamSelectLoop;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Clocks\BatchClock
 * @covers \EdgeTelemetrics\EventCorrelation\Clocks\TickClock
 */
class ClockTest extends TestCase {

    public function testBatchClockStartsAtEpoch() : void {
        $clock = new BatchClock();
        $this->assertSame(0, $clock->now()->getTimestamp());
    }

    public function testBatchClockMovesForward() : void {
        $clock = new BatchClock();
        $time = new DateTimeImmutable('@1700000000');

        $clock->set($time);
        $this->assertSame($time, $clock->now());
    }

    public function testBatchClockNeverMovesBackwards() : void {
        $clock = new BatchClock();
        $later = new DateTimeImmutable('@1700000000');
        $earlier = new DateTimeImmutable('@1600000000');

        $clock->set($later);
        $clock->set($earlier);

        $this->assertSame($later, $clock->now());
    }

    public function testBatchClockKeepsCurrentInstanceOnEqualTime() : void {
        $clock = new BatchClock();
        $later = new DateTimeImmutable('@1700000000');

        $clock->set($later);
        $clock->set(new DateTimeImmutable('@1700000000'));

        $this->assertSame($later, $clock->now(), 'equal timestamps must not replace the stored instance');
    }

    public function testTickClockReturnsUtcTimeCloseToNow() : void {
        $clock = new TickClock(new StreamSelectLoop());
        $now = $clock->now();

        $this->assertSame('UTC', $now->getTimezone()->getName());
        $this->assertLessThan(2, abs(time() - $now->getTimestamp()));
    }

    public function testTickClockIsStableWithinTickAndAdvancesAfterTick() : void {
        $loop = new StreamSelectLoop();
        $clock = new TickClock($loop);

        $first = $clock->now();
        $this->assertSame($first, $clock->now(), 'now() must be cached within a loop tick');

        $loop->futureTick(function () use ($loop) {
            $loop->stop();
        });
        $loop->run();

        $second = $clock->now();
        $this->assertNotSame($first, $second, 'now() must be re-evaluated on the next loop tick');
        $this->assertGreaterThanOrEqual($first->getTimestamp(), $second->getTimestamp());
    }

    public function testTickClockUsesGlobalLoopWhenNoneProvided() : void {
        $clock = new TickClock();
        $this->assertInstanceOf(DateTimeImmutable::class, $clock->now());
    }
}
