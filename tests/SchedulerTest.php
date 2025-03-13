<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Library\Source\GeneratorSource;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use EdgeTelemetrics\EventCorrelation\StateMachine\AEventProcessor;
use Generator;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use VStelmakh\PsrTestLogger\TestLogger;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler
 */
class SchedulerTest extends TestCase {

    /*protected function tearDown(): void
    {
        //We may have set the rules to Live, reset the static state
        $fix = function () {
            self::$eventstream_live = false;
        };
        $fix = $fix->bindTo(null, AEventProcessor::class);
        $fix();
    }*/

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

    public function testSchedulerSendsInitialisationEventOnStartup(): void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $event = null;
        $scheduler->setHandleEventCallback(static function($received) use ($scheduler, &$event) {
            $event ??= $received;
            if (!$scheduler->getExecutionState()->isStopping()) {
                $scheduler->shutdown();
            }
        }, null);

        $scheduler->run();

        $this->assertNotNull($event);
        $this->assertEquals(Scheduler::CONTROL_MSG_NEW_STATE, $event->event);
    }

    public function testSchedulerShutsDownAfterAllInputCloses(): void {
        $scheduler = $this->buildObservableScheduler();
        $scheduler->setRealtime();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $gen = function() : Generator {
            foreach([] as $_) {
                yield $_;
            }
        };

        $scheduler->register_input_process('test', new GeneratorSource($gen()));

        $scheduler->run();

        $logger->assert()
            ->hasLog()
            ->withMessage('No more input processes running. Shutting down');
    }

    public function testScheduledGeneratesHeartbeatEvent() : void {
        $scheduler = $this->buildObservableScheduler();
        $scheduler->setRealtime();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->setHeartbeatInterval(1);

        $heartbeatEvent = null;
        $scheduler->setHandleEventCallback(function($event) use (&$heartbeatEvent, $scheduler) {
            if ($event->event === Scheduler::CONTROL_MSG_HEARTBEAT) {
                $heartbeatEvent = $event;
                $scheduler->shutdown();
            }
        }, null);

        /* Exit test if we don't get a heartbeat */
        Loop::get()->futureTick(function () use ($scheduler, $logger) {
            Loop::get()->addTimer(1.1, function () use ($scheduler) {
                $scheduler->shutdown();
            });
        });

        $scheduler->run();

        $this->assertNotNull($heartbeatEvent);
        $this->assertInstanceOf(Event::class, $heartbeatEvent);
    }

    public function testTimerIsSetForEarliestTimeout() : void {
        $timer1 = new class() extends Rule\MatchSingleContinuously {

            const EVENTS = [[self::EVENT_MATCH_ANY]];
            const TIMEOUT = 'PT15S';
        };

        $timer2 = new class() extends Rule\MatchSingleContinuously {

            const EVENTS = [[self::EVENT_MATCH_ANY]];
            const TIMEOUT = 'PT50S';
        };

        $scheduler = $this->buildObservableScheduler([$timer2, $timer1]);
        $scheduler->setRealtime();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $timer = null;
        $eventTime = null;
        $scheduler->setHandleEventCallback(null, static function($received) use ($scheduler, &$timer, &$eventTime) {
            $eventTime ??= $received->datetime;
            $timer ??= $scheduler->getNextScheduledTimer();
            if (!$scheduler->getExecutionState()->isStopping()) {
                $scheduler->exit();
            }
            //Loop::stop();
        });

        $scheduler->run();

        $this->assertNotNull($timer);
        $this->assertNotNull($eventTime);

        $calculated = $eventTime->modify('+15 second');
        $this->assertEquals($calculated->format('Y-m-d H:i:s'), $timer->format('Y-m-d H:i:s'));
    }
}