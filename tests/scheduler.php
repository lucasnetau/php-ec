<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\tests\Rules\LogEverything;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchAnyRule;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchOneRule;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchOneRuleContinuously;

use function EdgeTelemetrics\EventCorrelation\php_cmd;

error_reporting( E_ALL );
ini_set('display_errors', "on");

include __DIR__ . "/../vendor/autoload.php";

$rules = [
    MatchAnyRule::class,
    MatchOneRule::class,
    MatchOneRuleContinuously::class,
    //LogEverything::class,
];

$scheduler = new class($rules) extends Scheduler {
    const MEMORY_PRESSURE_HIGH_WATERMARK = 75;
    const MEMORY_PRESSURE_LOW_WATERMARK = 50;
    //const RUNNING_ACTION_LIMIT_HIGH_WATERMARK = 200000;

    public function __construct(array $rules)
    {
        set_exception_handler([$this, "handle_exception"]);

        parent::__construct($rules);
        $this->register_input_process('test_data_stream_1', php_cmd(__DIR__ . "/test_input_1.php"));
        $this->register_input_process('test_data_stream_2', php_cmd(__DIR__ . "/test_input_2.php"));

        if (file_exists('/tmp/php_ec-scheduler_test_logs.txt')) {
            unlink('/tmp/php_ec-scheduler_test_logs.txt');
        }
        $this->register_action('log', php_cmd(__DIR__ . "/log.php"), null, false, [
            'LOG_FILENAME' => '/tmp/php_ec-scheduler_test_logs.txt',
        ]);

        $this->setSavefileName("/tmp/php_ec-scheduler_test.state");
        $this->setSaveStateInterval(1);
    }

    function handle_exception($exception) {
        $ex_class = get_class($exception);
        $message = <<<EOM
Exception : {$ex_class} {$exception->getCode()}
File: {$exception->getFile()}
Line: {$exception->getLine()}

Message: {$exception->getMessage()}
Trace: 
{$exception->getTraceAsString()}
EOM;
        fwrite(STDERR, "$message\n");
    }
};

$scheduler->run();
