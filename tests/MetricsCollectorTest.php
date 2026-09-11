<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Scheduler\MetricsCollector;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use React\EventLoop\StreamSelectLoop;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\MetricsCollector
 */
class MetricsCollectorTest extends TestCase {

    private function captureLogger() : AbstractLogger {
        return new class extends AbstractLogger {
            public array $records = [];

            public function log($level, $message, array $context = []): void {
                $this->records[] = ['level' => $level, 'message' => $message, 'context' => $context];
            }
        };
    }

    public function testIncAndSetTrackValues() : void {
        $logger = $this->captureLogger();
        $collector = new MetricsCollector($logger);

        $collector->inc('events', 'seen');
        $collector->inc('events', 'seen', 4);
        $collector->inc('events', 'seen', -2);
        $collector->set('memory', 'percent_used', 42.5);

        $collector->logSnapshot();

        $metrics = [];
        foreach ($logger->records as $record) {
            $this->assertSame('info', $record['level']);
            $this->assertSame('metric', $record['message']);
            $metrics[$record['context']['metric']] = $record['context']['value'];
        }

        $this->assertSame(3, $metrics['events.seen']);
        $this->assertSame(42.5, $metrics['memory.percent_used']);
    }

    public function testLogSnapshotWithNoLoggerIsNoop() : void {
        $collector = new MetricsCollector();
        $collector->inc('events', 'seen');
        $collector->logSnapshot();

        $this->assertTrue(true);
    }

    public function testSchedulePeriodicLogging() : void {
        $logger = $this->captureLogger();
        $collector = new MetricsCollector($logger);
        $collector->inc('events', 'seen');

        $loop = new StreamSelectLoop();
        $collector->schedulePeriodicLogging($loop, 1);

        $loop->addTimer(1.1, $loop->stop(...));
        $loop->run();

        $this->assertNotEmpty($logger->records, 'periodic logging must emit a snapshot');
    }
}
