<?php declare(strict_types=1);

/**
 * Test: Unhandled exception thrown in action
 */
use Bref\Logger\StderrLogger;
use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use Psr\Log\LogLevel;

use function EdgeTelemetrics\EventCorrelation\php_cmd;

require __DIR__ . "/../vendor/autoload.php";

class TriggerException extends Rule {

    const EVENTS = [[self::EVENT_MATCH_ANY]];

    public function fire()
    {
        $action = new Action("exception", $this->consumedEvents[0]);
        $this->emit('data', [$action]);
    }
}

$rules = [
    TriggerException::class,
];

define('STATE_FILE', tempnam(sys_get_temp_dir(), 'php-ec-test.'));

(new class($rules) extends Scheduler {
    public function __construct(array $rules)
    {
        parent::__construct($rules);
        set_exception_handler([$this, "handle_exception"]);
        $this->setLogger(new StderrLogger(LogLevel::DEBUG));

        $this->register_input_process('single_event', php_cmd(__DIR__ . "/Sources/single_event.php"));

        $this->register_action('exception', php_cmd(__DIR__ . "/actions/exception.php"));

        $this->setSavefileName(STATE_FILE);
        $this->setSaveStateInterval(1);
    }

    public function handle_exception(Throwable $exception) {
        $this->logger->emergency("Fatal", ['exception' => $exception,]);
    }
})->run();

$state = json_decode(file_get_contents(STATE_FILE), true);
echo json_encode($state, JSON_PRETTY_PRINT);