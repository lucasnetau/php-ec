<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use EdgeTelemetrics\JSON_RPC\Error as JsonRpcError;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use VStelmakh\PsrTestLogger\TestLogger;
use function EdgeTelemetrics\EventCorrelation\php_cmd;
use function str_repeat;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\ActionExecutionCoordinator
 */
class RpcPayloadSizeTest extends TestCase
{
    private function buildObservableScheduler(): ObservableScheduler
    {
        $scheduler = new ObservableScheduler([]);
        $tempState = tempnam(sys_get_temp_dir(), 'php-ec-test-state-');
        if ($tempState !== false) {
            unlink($tempState);
        }
        $scheduler->setSavefileName($tempState);
        $scheduler->setSaveStateInterval(600);
        return $scheduler;
    }

    /** @small */
    public function testOversizedRequestIsRejectedAndActionSurvives(): void
    {
        $scheduler = $this->buildObservableScheduler();
        $scheduler->setLogger(new TestLogger());
        $scheduler->register_action('smallBuffer', php_cmd(__DIR__ . '/scripts/Actions/smallBuffer.php'));

        $failed = [];
        $completed = [];
        $scheduler->on('action.failed', function($action, $exception) use (&$failed, $scheduler) {
            $failed[] = $exception;
            Loop::futureTick(function () use ($scheduler) {
                $scheduler->queueAction(new Action('smallBuffer', ['payload' => 'small enough']));
            });
        });
        $scheduler->on('action.completed', function($action) use (&$completed, $scheduler) {
            $completed[] = $action;
            Loop::futureTick($scheduler->shutdown(...));
        });

        Loop::futureTick(function () use ($scheduler) {
            $scheduler->queueAction(new Action('smallBuffer', ['payload' => str_repeat('x', 4096)]));
        });

        $scheduler->run();

        $this->assertCount(1, $failed);
        $this->assertSame(JsonRpcError::INVALID_REQUEST, $failed[0]->getCode());
        $this->assertStringContainsString('exceeds the maximum', $failed[0]->getMessage());
        $this->assertCount(1, $completed);
        $this->assertSame(['payload' => 'small enough'], $completed[0]->getVars());
    }

    /** @small */
    public function testActionWithoutHelloStillRunsAfterGracePeriod(): void
    {
        $scheduler = $this->buildObservableScheduler();
        $scheduler->setLogger(new TestLogger());
        $scheduler->register_action('noHello', php_cmd(__DIR__ . '/scripts/Actions/noHello.php'));

        $completed = [];
        $scheduler->on('action.completed', function($action) use (&$completed, $scheduler) {
            $completed[] = $action;
            Loop::futureTick($scheduler->shutdown(...));
        });

        Loop::futureTick(function () use ($scheduler) {
            $scheduler->queueAction(new Action('noHello', ['payload' => 'no hello']));
        });

        $scheduler->run();

        $this->assertCount(1, $completed);
        $this->assertSame(['payload' => 'no hello'], $completed[0]->getVars());
    }
}
