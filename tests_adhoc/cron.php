<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Event;
use \EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Rule\Cron;

include __DIR__ . "/../vendor/autoload.php";

Rule::setEventStreamLive();

class RapidCron extends Rule\Cron {
    const CRON_SCHEDULE = '* * * * *';

    const EVENTS = [['TestEvent']];

    public function onSchedule(): void
    {
        error_log('MINUTE CRON: Minute Cron schedule reached');
    }
}

class DailyCron extends Rule\Cron {
    const CRON_SCHEDULE = '@daily';

    const TIMEZONE = 'Australia/Sydney';

    const EVENTS = [['TestEvent']];

    public function onSchedule(): void
    {
        error_log('DAILY CRON: Daily Cron schedule reached');
    }
};

class InitCron extends Rule\Cron {
    const CRON_SCHEDULE = Rule\Cron::ON_INITIALISATION;

    public function onSchedule(): void
    {
        error_log('INIT CRON: System has initialised');
    }
}

class ShutdownCron extends Rule\Cron {
    const CRON_SCHEDULE = Rule\Cron::ON_SHUTDOWN;

    public function onSchedule(): void
    {
        error_log('SHUTDOWN CRON: System shutting down');
    }
}

/** @var Cron[] $rules */
$rules = [
   // 'init' => new InitCron(),
  //  'rapid' => new RapidCron(),
    'daily' => new DailyCron(),
   // 'shutdown' => new ShutdownCron(),
];

$events = [
    'Initialised Event' => new Event(['event' => \EdgeTelemetrics\EventCorrelation\Scheduler::CONTROL_MSG_NEW_STATE]),
    'Regular Event' => new Event(['event' => 'TestEvent']),
    'Stop Event' => new Event(['event' => \EdgeTelemetrics\EventCorrelation\Scheduler::CONTROL_MSG_STOP]),
];

foreach($events as $msg => $event) {
    error_log("******* Sending $msg *******");
    foreach ($rules as $ruleIndex => $rule) {
        $handled = $rule->handle($event);
        if (($handled & $rule::EVENT_HANDLED) !== $rule::EVENT_HANDLED) {
            error_log('event was not handled');
        } else {
            $rule->fire();
        }
        if ($rule->complete() || $rule->isTimedOut()) {
            unset($rules[$ruleIndex]);
        } else {
            if ($rule->getTimeout() !== null) {
                $rule->alarm();
            }
            echo get_class($rule) . ": " . json_encode( $rule) . PHP_EOL;
        }
    }
}