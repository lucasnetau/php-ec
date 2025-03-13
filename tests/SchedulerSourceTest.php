<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Library\Source\GeneratorSource;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\State;
use EdgeTelemetrics\EventCorrelation\StateMachine\AEventProcessor;
use Generator;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use VStelmakh\PsrTestLogger\TestLogger;
use function EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd;
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
    public function testSchedulerShutdownTerminatesSourceCleanly() : void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->register_input_process('test', wrap_source_php_cmd(__DIR__ . '/scripts/Source/CountToTen.php', true), env: ['DELAY' => 10,]);

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
            ->withMessage('Input Process test exited on signal: 15');
    }
}