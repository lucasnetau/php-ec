<?php declare(strict_types=1);

use Bref\Logger\StderrLogger;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Library\Actions\ActionHelper;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use EdgeTelemetrics\EventCorrelation\Tests\Helpers\RedirectStreamFilter;
use EdgeTelemetrics\EventCorrelation\Tests\Rules\TimeoutRule;
use Psr\Log\LogLevel;
use React\EventLoop\Loop;
use VStelmakh\PsrTestLogger\TestLogger;

require_once __DIR__ . '/../vendor/autoload.php';

//class_exists(RedirectStreamFilter::class);
//$stderrStream = fopen('php://temp', 'w+');
//$stderrRedirect = stream_filter_prepend(\STDERR, "redirect", STREAM_FILTER_WRITE, ['redirect' => $stderrStream]);

//$stdoutStream = fopen('php://temp', 'w+');
//$stdoutRedirect = stream_filter_prepend(\STDOUT, "redirect", STREAM_FILTER_WRITE, ['redirect' => $stdoutStream]);

$helper = new ActionHelper();

$helper->on(ActionHelper::ACTION_SHUTDOWN, function() use($helper) {
    $helper->stop();
});

Loop::futureTick(function () use ($helper) {
    $helper->log(LogLevel::DEBUG, 'test');
    $helper->stop();
});

$helper->run();

//error_log('Done');
//error_log("Redirected StdOut: " . stream_get_contents($stdoutStream,null,0) . PHP_EOL);
