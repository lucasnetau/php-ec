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

            public function onSchedule(): void {
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

            public function onSchedule(): void {
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

    public function testCronFiresOnShutdown(): void {
        $rule = new class() extends Cron {
            const CRON_SCHEDULE = Cron::ON_SHUTDOWN;

            public function onSchedule(): void {
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