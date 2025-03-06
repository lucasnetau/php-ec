<?php declare(strict_types=1);

use Bref\Logger\StderrLogger;
use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Library\Source\GeneratorSource;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use Psr\Log\LogLevel;

require __DIR__ . '/../../vendor/autoload.php';

$sendCountToStdOut = new class extends Rule\MatchSingle {
    const EVENTS = [['SampleValueEvent']];

    public function onComplete(): void {
        $this->emit('data', [New Action('echo', ['value' => $this->getFirstEvent()->value])]);
    }
};

$rules = [
    $sendCountToStdOut::class,
];

$scheduler = new Scheduler($rules);
$scheduler->setLogger(new StderrLogger(LogLevel::DEBUG));

$scheduler->register_action('echo', function($vars) {
    echo 'Next Value: ' . $vars['value'] . PHP_EOL;
});

function countToTen() : Generator {
    for($i = 1;  $i <= 10; $i++) {
        yield new Event(['event' => 'SampleValueEvent', 'value' => $i]);
    }
};
$numberGenClass = new GeneratorSource(countToTen(), 1.0);

$scheduler->register_input_process('generator', $numberGenClass);

$scheduler->run();