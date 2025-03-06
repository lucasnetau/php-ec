<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler as EventCorrelationScheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use React\EventLoop\Loop;

require __DIR__ . '/../vendor/autoload.php';

class TriggerException extends Rule {

    const EVENTS = [[self::EVENT_MATCH_ANY]];

    public function onComplete() : void
    {
        $action = new Action("exception", $this->consumedEvents[0]);
        $this->emit('data', [$action]);
    }
}

class InvokableClassAction {
    public function __invoke(): void
    {
        error_log('*Success* ran InvokableClassAction');
    }
}

$rules = [
    TriggerException::class,
];

$scheduler = new ObservableScheduler($rules);

$scheduler->register_action('closureAction', function() {
    error_log('*Success* ClosureAction');
});

$scheduler->register_action('invokableClassAction', new InvokableClassAction());

Loop::get()->addTimer(1, function() use ($scheduler) {
    error_log('Testing closureAction');
    $scheduler->queueAction(new Action('closureAction', []));
});
Loop::get()->addTimer(2, function() use ($scheduler) {
    error_log('Testing invokableClassAction');
    $scheduler->queueAction(new Action('invokableClassAction', []));
});

Loop::get()->addTimer(10, function() use ($scheduler) {
    error_log('Stopping Test');
    Loop::stop();
});

$scheduler->run();