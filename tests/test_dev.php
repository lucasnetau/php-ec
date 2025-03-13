<?php declare(strict_types=1);

use Bref\Logger\StderrLogger;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use EdgeTelemetrics\EventCorrelation\Tests\Rules\TimeoutRule;
use VStelmakh\PsrTestLogger\TestLogger;

require_once __DIR__ . '/../vendor/autoload.php';

function buildObservableScheduler(array $rules = []) : ObservableScheduler {
    $scheduler = new ObservableScheduler($rules);
    $tempState = tempnam(sys_get_temp_dir(), 'php-ec-test-state-');
    if ($tempState !== false) {
        unlink($tempState);
    }
    $scheduler->setSavefileName($tempState);
    $scheduler->setSaveStateInterval(600);
    return $scheduler;
}

$unclean = new class() extends Rule {

    const EVENTS = [[self::EVENT_MATCH_ANY]];
    public function handle(Event $event) : int {
        throw new \Exception('This should never happen');
    }
};

$scheduler = buildObservableScheduler([$unclean]);

$scheduler->run();


$scheduler = buildObservableScheduler([TimeoutRule::class]);

$logger = new StdErrLogger();
$scheduler->setLogger($logger);

$wasCalled = false;
$closure = function($vars) use (&$wasCalled, $scheduler) {
    $wasCalled = true;
    $this->logger->info(json_encode($vars));
    $scheduler->shutdown();
};

$scheduler->register_action('recordTimeout', $closure);

$scheduler->run();