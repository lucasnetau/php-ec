<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Rule;
use Bref\Logger\StderrLogger;
use EdgeTelemetrics\EventCorrelation\Scheduler;

use Psr\Log\LogLevel;
use function EdgeTelemetrics\EventCorrelation\php_cmd;
use function EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd;

error_reporting(E_ALL);
ini_set('display_errors', "on");

include __DIR__ . "/../vendor/autoload.php";

class EveryMinuteCron extends Rule\Cron {

    const TIMEZONE = 'Australia/Sydney';
    const CRON_SCHEDULE = '* * * * *';

    public function onSchedule(): void
    {
        error_log('MINUTE CRON: Minute Cron schedule reached ' . (new DateTimeImmutable())->format('Y-m-d H:i:s'));
    }
}

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

$numberGenClass = new class() extends Scheduler\SourceFunction {
    protected \React\EventLoop\TimerInterface $timer;

    function functionStart(): void
    {
        $this->timer = $this->loop->addPeriodicTimer(5.0, function () {
            static $count = 1;
            try {
                $event = new Event(['event' => 'SampleValueEvent', 'value' => $count++]);
                $this->emit('data', [$event]);
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

$scheduler = new class([EveryMinuteCron::class, InitCron::class, ShutdownCron::class]) extends Scheduler {
    public function __construct(array $rules)
    {
        parent::__construct($rules);
        set_exception_handler([$this, "handle_exception"]);
        $this->setLogger(new StderrLogger(LogLevel::DEBUG));

        //$this->engine->setEventStreamLive();

        $this->setSavefileName("/tmp/php_ec-scheduler_test.state");
        $this->setSaveStateInterval(1);
        $this->enableManagementServer(true);
        $this->setHeartbeatInterval(10);
    }

    function handle_exception($exception): void
    {
        $this->logger->emergency("Fatal", ['exception' => $exception,]);
    }
};

$scheduler->register_input_process('generator', $numberGenClass, null, [], false);

$scheduler->run();