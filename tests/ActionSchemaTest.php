<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Scheduler\ActionExecutionCoordinator;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use VStelmakh\PsrTestLogger\TestLogger;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\ActionExecutionCoordinator::handleAction
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\ActionExecutionCoordinator::register_action
 */
class ActionSchemaTest extends TestCase {

    private function schema() : object {
        return json_decode(json_encode([
            'type' => 'object',
            'required' => ['count'],
            'properties' => [
                'count' => ['type' => 'integer'],
            ],
        ]));
    }

    public function testInvalidParametersEmitArgumentErrorAndDoNotStart() : void {
        $coordinator = new ActionExecutionCoordinator();
        $coordinator->setLogger(new TestLogger());

        $started = 0;
        $errors = [];
        $coordinator->on('action.started', function ($action) use (&$started) { $started++; });
        $coordinator->on('action.argumenterror', function ($action, $exception) use (&$errors) { $errors[] = ['action' => $action, 'exception' => $exception]; });

        $coordinator->register_action('validated', fn() => true, schema: $this->schema());
        $action = new Action('validated', ['count' => 'not-an-int']);
        $coordinator->handleAction($action);

        $this->assertCount(1, $errors);
        $this->assertSame($action, $errors[0]['action']);
        $this->assertInstanceOf(InvalidArgumentException::class, $errors[0]['exception']);
        $this->assertSame(0, $started, 'an action with invalid parameters must not start');
    }

    public function testMissingRequiredParameterIsRejected() : void {
        $coordinator = new ActionExecutionCoordinator();
        $coordinator->setLogger(new TestLogger());

        $errors = [];
        $coordinator->on('action.argumenterror', function ($action, $exception) use (&$errors) { $errors[] = ['action' => $action, 'exception' => $exception]; });

        $coordinator->register_action('validated', fn() => true, schema: $this->schema());
        $coordinator->handleAction(new Action('validated', []));

        $this->assertCount(1, $errors);
    }

    public function testValidParametersStartAction() : void {
        $coordinator = new ActionExecutionCoordinator();
        $coordinator->setLogger(new TestLogger());

        $started = 0;
        $errors = [];
        $coordinator->on('action.started', function ($action) use (&$started) { $started++; });
        $coordinator->on('action.argumenterror', function ($action, $exception) use (&$errors) { $errors[] = ['action' => $action, 'exception' => $exception]; });

        $coordinator->register_action('validated', fn() => true, schema: $this->schema());
        $coordinator->handleAction(new Action('validated', ['count' => 5]));

        $this->assertSame(0, count($errors));
        $this->assertSame(1, $started);
    }
}
