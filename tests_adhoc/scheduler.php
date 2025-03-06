<?php declare(strict_types=1);

use Bref\Logger\StderrLogger;
use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\tests\Rules\LogControlMessages;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchAnyRule;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchOneRule;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchOneRuleContinuously;
use EdgeTelemetrics\EventCorrelation\Rule;

use Psr\Log\LogLevel;
use function EdgeTelemetrics\EventCorrelation\php_cmd;
use function EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd;

error_reporting( E_ALL );
ini_set('display_errors', "on");

include __DIR__ . "/../vendor/autoload.php";

class PassToFail extends Rule\MatchSingle {

    const EVENTS = [[Scheduler::CONTROL_MSG_NEW_STATE,Scheduler::CONTROL_MSG_RESTORED_STATE]];

    public function onComplete() : void
    {
        $action = new Action("fail", $this->getFirstEvent());
        $this->emit('data', [$action]);
    }
}

$rules = [
    MatchAnyRule::class,
    MatchOneRule::class,
    MatchOneRuleContinuously::class,
    LogControlMessages::class,
    //PassToFail::class,
];

$scheduler = new class($rules) extends Scheduler {
    const MEMORY_PRESSURE_HIGH_WATERMARK = 75;
    const MEMORY_PRESSURE_LOW_WATERMARK = 50;
    //const RUNNING_ACTION_LIMIT_HIGH_WATERMARK = 200000;

    public function __construct(array $rules)
    {
        parent::__construct($rules);
        set_exception_handler([$this, "handle_exception"]);
        $this->setLogger(new StderrLogger(LogLevel::DEBUG));

        $this->register_input_process('test_data_stream_1', wrap_source_php_cmd(__DIR__ . "/Sources/test_input_1.php"));
        $this->register_input_process('test_data_stream_2', php_cmd(__DIR__ . "/Sources/test_input_2.php"));
        $this->register_input_process('test_misconfigured_input', php_cmd(__DIR__ . "/Sources/noexist.php"), null, [], false);

        if (file_exists('/tmp/php_ec-scheduler_test_logs.txt')) {
            unlink('/tmp/php_ec-scheduler_test_logs.txt');
        }
        $this->register_action('log', php_cmd(__DIR__ . "/Actions/log.php"), null, false, [
            'LOG_FILENAME' => '/tmp/php_ec-scheduler_test_logs.txt',
        ]);
        $this->register_action('fail', php_cmd(__DIR__ . "/Actions/willFail.php"), null, false, []);

        $this->setSavefileName("/tmp/php_ec-scheduler_test.state");
        $this->setSaveStateInterval(1);
        $this->enableManagementServer(true);
        $this->setHeartbeatInterval(10);
    }

    function handle_exception($exception) {
        $this->logger->emergency("Fatal", ['exception' => $exception,]);
    }
};

$scheduler->run();
