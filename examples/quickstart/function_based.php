<?php declare(strict_types=1);

use Bref\Logger\StderrLogger;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use Psr\Log\LogLevel;

require __DIR__ . '/../../vendor/autoload.php';

class SendCountToStdOut extends Rule\MatchSingle {
    const EVENTS = [['SampleValueEvent']];

    public function onComplete(): void
    {
        $this->emit('data', [New \EdgeTelemetrics\EventCorrelation\Action('echo', ['value' => $this->getFirstEvent()->value])]);
    }
}

$rules = [
    SendCountToStdOut::class,
];

$scheduler = new \EdgeTelemetrics\EventCorrelation\Scheduler($rules);
$scheduler->setLogger(new StderrLogger(LogLevel::DEBUG));

$scheduler->register_action('echo', function($vars) {
    echo 'Next Value: ' . $vars['value'] . PHP_EOL;
});

$numberGenClass = new class() extends Scheduler\SourceFunction {
    protected \React\EventLoop\TimerInterface $timer;

    function functionStart(): void
    {
        $this->timer = $this->loop->addPeriodicTimer(1.0, function () {
            static $count = 1;
            try {
                $event = new Event(['event' => 'SampleValueEvent', 'value' => $count++]);
                $this->emit('data', [$event]);

                if ($count > 10) {
                    $this->exit();
                }
            } catch (Throwable $exception) {
                $this->emit('error', [$exception]);
                $this->exit(255);
                return;
            }
        });
    }

    function exit(int $code = 0): void
    {
        $this->running = false;
        $this->loop->cancelTimer($this->timer);
        unset($this->timer);
        $this->emit('exit', [$code]);
    }

    function functionStop(): void
    {
        $this->exit();
    }
};

$scheduler->register_input_process('generator', $numberGenClass, null, [], false);

$scheduler->run();