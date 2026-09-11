<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use DateInterval;
use EdgeTelemetrics\EventCorrelation\Counter;
use EdgeTelemetrics\EventCorrelation\Library\Histogram;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Counter
 * @covers \EdgeTelemetrics\EventCorrelation\Library\Histogram
 */
class MetricsTest extends TestCase {

    public function testCounterStartsEmpty() : void {
        $counter = new Counter(new DateInterval('PT60S'), Counter::RESOLUTION_SECONDS);
        $this->assertSame(array_fill(0, 60, 0), $counter->getCounter());
    }

    public function testCounterAccumulatesInCurrentSlot() : void {
        $counter = new Counter(new DateInterval('PT60S'), Counter::RESOLUTION_SECONDS);
        $counter->increment();
        $counter->increment();
        $counter->increment();

        $slots = $counter->getCounter();
        $this->assertSame(3, array_sum($slots));
        $this->assertSame(3, $slots[array_key_last($slots)], 'current time slot must be last');
    }

    public function testCounterLastEventTimeIsUnknownBeforeFirstIncrement() : void {
        $counter = new Counter(new DateInterval('PT60S'), Counter::RESOLUTION_SECONDS);
        $this->assertSame(0, $counter->getLastEventTime(), 'no event has been seen yet');
    }

    public function testCounterLastEventTimeAfterIncrement() : void {
        $counter = new Counter(new DateInterval('PT60S'), Counter::RESOLUTION_SECONDS);
        $counter->increment();
        $this->assertLessThan(2, abs(time() - $counter->getLastEventTime()));
    }

    public function testMillisecondCounterLastEventTimeAfterIncrement() : void {
        $counter = new Counter(new DateInterval('PT60S'), Counter::RESOLUTION_MILLISECONDS);
        $counter->increment();
        usleep(200_000);
        $this->assertLessThan(2, abs(time() - $counter->getLastEventTime()), 'millisecond resolution must still return a UNIX timestamp');
    }

    public function testCounterDropsExpiredSlots() : void {
        $counter = new Counter(new DateInterval('PT1S'), Counter::RESOLUTION_SECONDS);
        $counter->increment();
        usleep(1_200_000);
        $counter->increment();

        $this->assertSame(1, array_sum($counter->getCounter()), 'old slots must be flushed after the measurement window');
    }

    public function testLastEventTimeIsDerivedFromMonotonicAnchor() : void {
        $counter = new Counter(new DateInterval('PT60S'), Counter::RESOLUTION_SECONDS);
        $reflection = new ReflectionClass($counter);
        $reflection->getProperty('timeAnchor')->setValue($counter, 1000);
        $reflection->getProperty('epochAnchor')->setValue($counter, 1_700_000_000);
        $reflection->getProperty('lastEventTime')->setValue($counter, 1060);

        $this->assertSame(1_700_000_060, $counter->getLastEventTime());

        $msCounter = new Counter(new DateInterval('PT60S'), Counter::RESOLUTION_MILLISECONDS);
        $reflection = new ReflectionClass($msCounter);
        $reflection->getProperty('timeAnchor')->setValue($msCounter, 1000);
        $reflection->getProperty('epochAnchor')->setValue($msCounter, 1_700_000_000);
        $reflection->getProperty('lastEventTime')->setValue($msCounter, 4000);

        $this->assertSame(1_700_000_003, $msCounter->getLastEventTime());
    }

    public function testHistogramBucketsAreInclusiveUpperBounds() : void {
        $histogram = new Histogram(64, 128, 256);
        foreach ([0, 1, 64, 65, 128, 129, 256, 257] as $value) {
            $histogram->add($value);
        }

        $this->assertSame([
            ['upper_bound' => 64, 'count' => 3],
            ['upper_bound' => 128, 'count' => 2],
            ['upper_bound' => 256, 'count' => 2],
            ['upper_bound' => '+Inf', 'count' => 1],
        ], $histogram->getHistogram());
    }

    public function testHistogramSortsBucketsAndOmitsEmptyOnes() : void {
        $histogram = new Histogram(256, 64, 128);
        $histogram->add(10);

        $this->assertSame([['upper_bound' => 64, 'count' => 1]], $histogram->getHistogram());
    }

    public function testEmptyHistogram() : void {
        $histogram = new Histogram(64, 128);
        $this->assertSame([], $histogram->getHistogram());
    }
}
