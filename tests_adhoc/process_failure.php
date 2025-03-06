<?php declare(strict_types=1);

use function EdgeTelemetrics\EventCorrelation\php_cmd;
use function EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd;

include __DIR__ . '/../vendor/autoload.php';

$ping = new React\ChildProcess\Process(php_cmd('unknownfile.php'));
$ping->start();

$ping->on('exit', function($code, $term) {
    if ($code === 127) {
        echo "Command not found. Test Pass" . PHP_EOL;
    }
    if ($code !== 0) {
        echo "Process Failure code $code" . PHP_EOL;
    }
});

$ping->stderr->on('data', function ($chunk) {
    error_log('stderr: '. $chunk);
});

$ping->stdout->on('data', function ($chunk) {
    error_log('stdout:' . $chunk);
});

React\EventLoop\Loop::get()->run();

echo "Process Failure Test Completed" . PHP_EOL;