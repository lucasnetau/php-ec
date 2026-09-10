<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Error as JsonRpcError;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use PHPUnit\Framework\TestCase;
use React\ChildProcess\Process;
use React\EventLoop\StreamSelectLoop;
use function EdgeTelemetrics\EventCorrelation\php_cmd;
use function explode;
use function json_decode;
use function json_encode;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * ActionHelper manipulates the process output buffers and writes to php://stdout directly,
 * so it is exercised in a child process where its stdout can be captured without disturbing
 * PHPUnit's own output buffering.
 *
 * @covers \EdgeTelemetrics\EventCorrelation\Library\Actions\ActionHelper
 */
class ActionHelperTest extends TestCase {

    public function testActionHelperHandlesRpcAndLogsToStdout(): void {
        $loop = new StreamSelectLoop();
        $process = new Process(php_cmd(__DIR__ . '/scripts/Actions/logToScheduler.php', true));
        $process->start($loop);

        $stdout = '';
        $stderr = '';
        $exitCode = null;
        $process->stdout->on('data', function (string $chunk) use (&$stdout) { $stdout .= $chunk; });
        $process->stderr->on('data', function (string $chunk) use (&$stderr) { $stderr .= $chunk; });

        $process->stdin->write(json_encode(new JsonRpcRequest('unknown/method', [], 'test-request-2')) . "\n");
        $process->stdin->write(json_encode(new JsonRpcRequest(Scheduler::ACTION_RUN_METHOD, ['answer' => 42], 'test-request-1')) . "\n");

        // Give the helper time to handle the requests, then close its stdin to trigger shutdown
        $loop->addTimer(0.5, function () use ($process) {
            $process->stdin->end();
        });
        $loop->addTimer(5.0, function () use ($process) {
            $process->terminate(SIGKILL);
        });
        $process->on('exit', function (?int $code) use (&$exitCode, $loop) {
            $exitCode = $code;
            $loop->stop();
        });

        $loop->run();

        $messages = [];
        foreach (explode("\n", trim($stdout)) as $line) {
            if ($line !== '') {
                $messages[] = json_decode($line, true);
            }
        }

        $this->assertSame(0, $exitCode, 'ActionHelper exited with code ' . var_export($exitCode, true) . ". stderr: $stderr");
        $this->assertNotContains(null, $messages, "stdout was not all valid JSON: $stdout");

        $log = null;
        $response = null;
        $errorResponse = null;
        foreach ($messages as $message) {
            if (($message['method'] ?? null) === Scheduler::RPC_PROCESS_LOG) {
                $log = $message;
            }
            if (($message['id'] ?? null) === 'test-request-1') {
                $response = $message;
            }
            if (($message['id'] ?? null) === 'test-request-2') {
                $errorResponse = $message;
            }
        }

        $this->assertNotNull($log, 'ActionHelper must forward log() to the scheduler');
        $this->assertSame('Action called : {"answer":42}', $log['params']['message']);
        $this->assertNotNull($response, 'ActionHelper must write the RPC response');
        $this->assertTrue($response['result']);
        $this->assertNotNull($errorResponse, 'ActionHelper must answer unknown methods');
        $this->assertSame(JsonRpcError::METHOD_NOT_FOUND, $errorResponse['error']['code']);
    }
}
