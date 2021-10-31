<?php declare(strict_types=1);

/**
 * Test: Unhandled exception thrown in action
 */
use Bref\Logger\StderrLogger;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\tests\Rules\LogEverything;
use Psr\Log\LogLevel;

use function EdgeTelemetrics\EventCorrelation\php_cmd;
use function EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd;

require __DIR__ . "/../vendor/autoload.php";

$rules = [
    LogEverything::class,
];

define('STATE_FILE', tempnam(sys_get_temp_dir(), 'php-ec-test.'));

(new class($rules) extends Scheduler {
    public function __construct(array $rules)
    {
        parent::__construct($rules);
        set_exception_handler([$this, "handle_exception"]);
        $this->setLogger(new StderrLogger(LogLevel::DEBUG));

        $this->register_input_process('source_with_exception', wrap_source_php_cmd(__DIR__ . "/Sources/source_with_exception.php"));
        $this->register_input_process('source_with_error', wrap_source_php_cmd(__DIR__ . "/Sources/source_with_error.php"));

        $this->register_action('log', php_cmd(__DIR__ . "/actions/log.php"), null, false, ['LOG_FILENAME' => '/tmp/php-ec-test.txt']);

        $this->setSavefileName(STATE_FILE);
        $this->setSaveStateInterval(1);
    }

    public function handle_exception(Throwable $exception) {
        $this->logger->emergency("Fatal", ['exception' => $exception,]);
    }
})->run();

$state = json_decode(file_get_contents(STATE_FILE), true);
echo json_encode($state, JSON_PRETTY_PRINT);

unlink(STATE_FILE);
unlink('/tmp/php-ec-test.txt');