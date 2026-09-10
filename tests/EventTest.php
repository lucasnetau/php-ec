<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use DateTime;
use DateTimeImmutable;
use EdgeTelemetrics\EventCorrelation\Event;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Event
 */
class EventTest extends TestCase {

    public function testDatetimeParsingMatrix() : void {
        $cases = [
            'int' => [1700000000, 1700000000],
            'negative int' => [-100, -100],
            'float' => [1700000000.5, 1700000000],
            'numeric string' => ['1700000000', 1700000000],
            'zero string' => ['0', 0],
            'date string' => ['2024-01-02T03:04:05+10:00', 1704128645],
            'immutable' => [new DateTimeImmutable('@1700000000'), 1700000000],
            'mutable' => [new DateTime('@1700000000'), 1700000000],
        ];
        foreach ($cases as $label => [$value, $expectedTimestamp]) {
            $event = new Event(['event' => 'Test', 'datetime' => $value]);
            $this->assertInstanceOf(DateTimeImmutable::class, $event->datetime, "$label must produce DateTimeImmutable");
            $this->assertSame('UTC', $event->datetime->getTimezone()->getName(), "$label must be normalised to UTC");
            $this->assertSame($expectedTimestamp, $event->datetime->getTimestamp(), "$label timestamp");
        }

        $event = new Event(['event' => 'Test']);
        $this->assertInstanceOf(DateTimeImmutable::class, $event->datetime, 'missing datetime must produce DateTimeImmutable');
        $this->assertLessThan(5, abs(time() - $event->datetime->getTimestamp()), 'missing datetime must default to server time');
    }

    public function testFractionalNumericDatetimePreservesMicroseconds() : void {
        $event = new Event(['event' => 'Test', 'datetime' => '1700000000.250000']);
        $this->assertSame('1700000000.250000', $event->datetime->format('U.u'));
    }

    public function testEmptyDatetimeStringIsRejected() : void {
        $this->expectException(InvalidArgumentException::class);
        new Event(['event' => 'Test', 'datetime' => '   ']);
    }

    public function testGarbageDatetimeStringIsRejected() : void {
        $this->expectException(\Exception::class);
        new Event(['event' => 'Test', 'datetime' => 'not-a-date']);
    }

    public function testUnsupportedDatetimeTypeIsRejected() : void {
        $this->expectException(InvalidArgumentException::class);
        new Event(['event' => 'Test', 'datetime' => ['array']]);
    }
}
