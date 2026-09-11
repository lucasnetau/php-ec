<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Library\EventLog;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Library\EventLog
 */
class EventLogTest extends TestCase {

    public function testAddStoresEventPayloadInDataColumn() : void {
        $log = new EventLog();
        $log->add(new Event(['event' => 'SensorReading', 'value' => 42, 'sensor' => 'A1']));

        $events = $log->getEvents();
        $this->assertCount(1, $events);
        $this->assertSame('SensorReading', $events[0]['event']);

        $data = json_decode($events[0]['data'], true);
        $this->assertSame('SensorReading', $data['event']);
        $this->assertSame(42, $data['value']);
        $this->assertSame('A1', $data['sensor']);
    }

    public function testAddRecordsCorrectedDatetime() : void {
        $event = new Event(['event' => 'SensorReading', 'datetime' => '2020-01-01T00:00:00Z']);
        $event->setReceivedTime(new \DateTimeImmutable('2024-06-01T12:34:56Z'));

        $log = new EventLog();
        $log->add($event);

        $this->assertSame('2024-06-01 12:34:56', $log->getEvents()[0]['datetime']);
    }

    public function testLogKeepsOnlyMaxEvents() : void {
        $log = new EventLog(2);
        $log->add(new Event(['event' => 'One']));
        $log->add(new Event(['event' => 'Two']));
        $log->add(new Event(['event' => 'Three']));

        $events = $log->getEvents();
        $this->assertCount(2, $events);
        $this->assertSame('Two', $events[0]['event']);
        $this->assertSame('Three', $events[1]['event']);
    }
}
