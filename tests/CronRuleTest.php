<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use DateTimeImmutable;
use DateTimeZone;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Rule\Cron;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\StateMachine\IEventMatcher;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Rule\Cron
 */
class CronRuleTest extends TestCase {
    public function testCronAcceptsStartUpEvents(): void {
        $this->assertEquals([Scheduler::CONTROL_MSG_NEW_STATE, Scheduler::CONTROL_MSG_RESTORED_STATE], Cron::initialAcceptedEvents());
    }

    public function testCronFiresOnCronSchedule(): void {
        $rule = new class() extends Cron {
            const CRON_SCHEDULE = '*/1 * * * *';

            public function onSchedule(?DateTimeImmutable $scheduledTime = null): void {
                $this->emit('test', ['msg' => 'onSchedule called']);
            }
        };

        $result = null;
        $rule->on('test', function($msg) use (&$result) {
            $result = $msg;
        });

        //Simulate Rule initialisation
        $time = new DateTimeImmutable('now', new DateTimeZone($rule::TIMEZONE));
        $rule->handle(new Event(['event' => Scheduler::CONTROL_MSG_NEW_STATE, 'datetime' => $time]));
        $rule->fire();
        //echo $time->modify('+1 minute')->format('Y-m-d H:i:ss') . PHP_EOL;
       // echo $rule->getTimeout()?->format('Y-m-d H:i:ss') . PHP_EOL;
        $this->assertTrue($time->modify('+1 minute')->format('Y-m-d H:i:00') === $rule->getTimeout()?->format('Y-m-d H:i:00'), 'Cron scheduled timeout not correct');
        $this->assertNull($result);

        //Simulate one cron alarm, check that the cron timeout advances and that the onSchedule handler is called in the Rule
        $rule->alarm();
      //  echo $time->modify('+2 minute')->format('Y-m-d H:i:ss') . PHP_EOL;
      //  echo $rule->getTimeout()?->format('Y-m-d H:i:ss') . PHP_EOL;
        $this->assertTrue($time->modify('+2 minute')->format('Y-m-d H:i:00') === $rule->getTimeout()?->format('Y-m-d H:i:00'), 'Cron scheduled timeout not correct');
        $this->assertEquals('onSchedule called', $result);

        //Simulate second cron alarm and ensure the cron timeout advances
        $result = null;
        $rule->alarm();
       // echo $time->modify('+3 minute')->format('Y-m-d H:i:00') . PHP_EOL;
     //   echo $rule->getTimeout()?->format('Y-m-d H:i:00') . PHP_EOL;
        $this->assertTrue($time->modify('+3 minute')->format('Y-m-d H:i:00') === $rule->getTimeout()?->format('Y-m-d H:i:00'), 'Cron scheduled timeout not correct');
        $this->assertEquals('onSchedule called', $result);
    }

    public function testCronFiresOnInitialisation(): void {
        $rule = new class() extends Cron {
            const CRON_SCHEDULE = Cron::ON_INITIALISATION;

            public function onSchedule(?DateTimeImmutable $scheduledTime = null): void {
                $this->emit('test', ['msg' => 'onSchedule called']);
            }
        };

        $result = null;
        $rule->on('test', function($msg) use (&$result) {
            $result = $msg;
        });

        //Simulate Rule initialisation
        $time = new DateTimeImmutable('now');
        $handled = $rule->handle(new Event(['event' => Scheduler::CONTROL_MSG_NEW_STATE, 'datetime' => $time]));
        $rule->fire();
        $this->assertTrue((IEventMatcher::EVENT_HANDLED) === $handled);
        $this->assertEquals('onSchedule called', $result);
        $this->assertTrue($rule->complete());
        $this->assertFalse($rule->isTimedOut());
    }

    public function testCronPersistsCronLastRunOnSerialize(): void {
        $rule = new class() extends Cron {
            const CRON_SCHEDULE = '* * * * *';

            public function onSchedule(?DateTimeImmutable $scheduledTime = null): void {
                $this->emit('test', ['msg' => 'onSchedule called']);
            }
        };

        $time = new DateTimeImmutable('now', new DateTimeZone($rule::TIMEZONE));
        $rule->handle(new Event(['event' => Scheduler::CONTROL_MSG_NEW_STATE, 'datetime' => $time]));

        //Simulate one cron alarm to set cronLastRun
        $rule->alarm();

        //Serialize and verify cronLastRun is included in metrics
        $saved = $rule->__serialize();
        $this->assertArrayHasKey('metrics', $saved);
        $this->assertArrayHasKey('cronLastRun', $saved['metrics']);

        //Verify cronLastRun matches the timeout that just fired (ISO 8601 format)
        $expected = $time->modify('+1 minute')->format('Y-m-d\TH:i:00');
        $this->assertStringContainsString($expected, $saved['metrics']['cronLastRun']);

        //Create a fresh instance and restore state
        $class = new \ReflectionClass(get_class($rule));
        $restored = $class->newInstanceWithoutConstructor();
        /** @var Cron $restored */
        $restored->__unserialize($saved);

        //Verify cronLastRun was restored by re-serializing
        $reSerialized = $restored->__serialize();
        $this->assertArrayHasKey('metrics', $reSerialized);
        $this->assertArrayHasKey('cronLastRun', $reSerialized['metrics']);
        $this->assertSame($saved['metrics']['cronLastRun'], $reSerialized['metrics']['cronLastRun']);
    }

    public function testCronCatchesUpOnMissedRunsAfterRestore(): void {
        $scheduleCount = 0;
        $rule = new class() extends Cron {
            const CRON_SCHEDULE = '* * * * *';

            public function onSchedule(?DateTimeImmutable $scheduledTime = null): void {
                $this->emit('test', ['msg' => 'onSchedule called']);
            }
        };

        $rule->on('test', function($msg) use (&$scheduleCount) {
            $scheduleCount++;
        });

        //Simulate Rule initialisation
        $time = new DateTimeImmutable('now', new DateTimeZone($rule::TIMEZONE));
        $rule->handle(new Event(['event' => Scheduler::CONTROL_MSG_NEW_STATE, 'datetime' => $time]));

        //Simulate persisted state with cronLastRun set 3 minutes in the past
        $past = $time->modify('-3 minutes');
        $rule->unserializeMetrics(['cronLastRun' => $past->format('c')]);

        //Simulate state restoration event - triggers catchUpMissedRuns()
        $rule->handle(new Event(['event' => Scheduler::CONTROL_MSG_RESTORED_STATE, 'datetime' => $time]));

        //Verify onSchedule was called for each missed minute (3 missed runs)
        $this->assertSame(3, $scheduleCount);

        //Verify timeout was updated to the next scheduled minute after now
        $expectedNext = $time->modify('+1 minute')->format('Y-m-d H:i:00');
        $this->assertSame($expectedNext, $rule->getTimeout()?->format('Y-m-d H:i:00'));
    }

    public function testCronFiresOnShutdown(): void {
        $rule = new class() extends Cron {
            const CRON_SCHEDULE = Cron::ON_SHUTDOWN;

            public function onSchedule(?DateTimeImmutable $scheduledTime = null): void {
                $this->emit('test', ['msg' => 'onSchedule called']);
            }
        };

        $result = null;
        $rule->on('test', function($msg) use (&$result) {
            $result = $msg;
        });

        //Simulate Rule initialisation
        $time = new DateTimeImmutable('now', new DateTimeZone($rule::TIMEZONE));
        $rule->handle(new Event(['event' => Scheduler::CONTROL_MSG_NEW_STATE, 'datetime' => $time]));
        $rule->fire();
        $this->assertNull($result);
        $this->assertFalse($rule->complete());
        $this->assertFalse($rule->isTimedOut());

        $this->assertContains(Scheduler::CONTROL_MSG_STOP, $rule->nextAcceptedEvents());

        $handled = $rule->handle(new Event(['event' => Scheduler::CONTROL_MSG_STOP, 'datetime' => $time]));
        $rule->fire();
        $this->assertTrue((IEventMatcher::EVENT_HANDLED|IEventMatcher::EVENT_TIMEOUT) === $handled);
        $this->assertEquals('onSchedule called', $result);
        $this->assertTrue($rule->complete());
        $this->assertTrue($rule->isTimedOut());
    }
}