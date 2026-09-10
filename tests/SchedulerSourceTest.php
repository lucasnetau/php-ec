<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Library\Source\GeneratorSource;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\State;
use Generator;
use PHPUnit\Framework\TestCase;
use VStelmakh\PsrTestLogger\TestLogger;
use function EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd;
use function file_put_contents;
use function json_encode;
use function mt_rand;
use function mt_srand;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler
 */
class SchedulerSourceTest extends TestCase {

    public function buildObservableScheduler(array $rules = []) : ObservableScheduler {
        $scheduler = new ObservableScheduler($rules);
        $tempState = tempnam(sys_get_temp_dir(), 'php-ec-test-state-');
        if ($tempState !== false) {
            unlink($tempState);
        }
        $scheduler->setSavefileName($tempState);
        $scheduler->setSaveStateInterval(600);
        return $scheduler;
    }

    /**
     * @small
     */
    public function testScheduledExecutesGeneratorBasedSource() : void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $countToTen = function () : Generator {
            for($i = 1;  $i <= 10; $i++) {
                yield new Event(['event' => 'Count', 'value' => $i]);
            }
        };
        $scheduler->register_input_process('test', new GeneratorSource($countToTen(), 0.0001));

        $inputEvents = [];
        $scheduler->setHandleEventCallback(function($event) use (&$inputEvents, $scheduler) {
            if ($event->event === 'Count') {
                $inputEvents[] = $event->value;
            }
        }, null);

        $scheduler->run();

        $this->assertCount(10, $inputEvents);
        $this->assertEquals([1,2,3,4,5,6,7,8,9,10], $inputEvents);
    }

    /**
     * @small
     */
    public function testScheduledExecutesProcessBasedSource() : void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->register_input_process('test', wrap_source_php_cmd(__DIR__ . '/scripts/Source/CountToTen.php', true));

        $inputEvents = [];
        $scheduler->setHandleEventCallback(function($event) use (&$inputEvents, $scheduler) {
            if ($event->event === 'Count') {
                $inputEvents[] = $event->value;
            }
        }, null);

        $scheduler->run();

        $this->assertCount(10, $inputEvents);
        $this->assertEquals([1,2,3,4,5,6,7,8,9,10], $inputEvents);
    }

    /**
     * @small
     */
    public function testSchedulerDropsMalformedEventsFromProcessSource() : void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->register_input_process('test', wrap_source_php_cmd(__DIR__ . '/scripts/Source/MalformedEvent.php', true));

        $inputEvents = [];
        $scheduler->setHandleEventCallback(function($event) use (&$inputEvents) {
            if (!in_array($event->event, [
                Scheduler::CONTROL_MSG_NEW_STATE,
                Scheduler::CONTROL_MSG_RESTORED_STATE,
                Scheduler::CONTROL_MSG_STOP,
                Scheduler::CONTROL_MSG_HEARTBEAT,
            ], true)) {
                $inputEvents[] = $event->event;
            }
        }, null);

        $scheduler->run();

        $this->assertEquals(['Good'], $inputEvents);
    }

    /**
     * @small
     */
    public function testSchedulerSurvivesFuzzedEventPayloads() : void {
        $scheduler = $this->buildObservableScheduler();
        $scheduler->setLogger(new TestLogger());

        mt_srand(0xF0F0);
        $payloads = [];
        $expected = [];
        for ($i = 0; $i < 100; $i++) {
            $name = 'Fuzz:' . $i;
            switch ($i % 5) {
                case 0:
                    $payloads[] = ['event' => $name, 'value' => mt_rand(0, 1000)];
                    $expected[] = $name;
                    break;
                case 1:
                    $payloads[] = ['event' => $name, 'datetime' => mt_rand(0, 2_000_000_000)];
                    $expected[] = $name;
                    break;
                case 2:
                    $payloads[] = ['event' => $name, 'datetime' => '2024-01-02T03:04:05+00:00'];
                    $expected[] = $name;
                    break;
                case 3:
                    $payloads[] = ['event' => $name, 'datetime' => 'not-a-date'];
                    break;
                case 4:
                    $payloads[] = ['id' => $i, 'datetime' => '2024-01-02T03:04:05+00:00'];
                    break;
            }
        }

        $fuzzFile = tempnam(sys_get_temp_dir(), 'php-ec-fuzz-events-');
        file_put_contents($fuzzFile, json_encode($payloads));

        try {
            $scheduler->register_input_process('fuzz', wrap_source_php_cmd(__DIR__ . '/scripts/Source/FuzzEvents.php', true), env: ['FUZZ_FILE' => $fuzzFile]);

            $received = [];
            $scheduler->setHandleEventCallback(function($event) use (&$received) {
                if (str_starts_with($event->event, 'Fuzz:')) {
                    $received[] = $event->event;
                }
            }, null);

            $scheduler->run();
        } finally {
            @unlink($fuzzFile);
        }

        $this->assertSame($expected, $received, 'only well-formed fuzz events may reach the engine');
    }

    /**
     * @small
     */
    public function testSchedulerShutdownTerminatesSourceCleanly() : void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->register_input_process('test', wrap_source_php_cmd(__DIR__ . '/scripts/Source/CountToTen.php', true), env: ['DELAY' => 0.1,]);

        $inputEvents = [];
        $scheduler->setHandleEventCallback(function($event) use (&$inputEvents, $scheduler) {
            if ($event->event === 'Count') {
                $inputEvents[] = $event->value;
                $scheduler->shutdown();
            }
        }, null);

        $scheduler->run();

        $this->assertEquals(State::STOPPED, $scheduler->getExecutionState()->state());

        $this->assertCount(1, $inputEvents);
        $this->assertEquals([1,], $inputEvents);

        $logger->assert()
            ->hasLog()
            ->withMessage('Shutting down running input processes');

        $logger->assert()
            ->hasLog()
            ->withMessage('Sending SIGTERM to input process test');

        $logger->assert()
            ->hasInfo()
            ->withMessage('Input Process {id} exited with code: {code}');
    }
}