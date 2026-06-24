<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use VStelmakh\PsrTestLogger\TestLogger;
use function EdgeTelemetrics\EventCorrelation\php_cmd;
use function EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler
 * @covers \EdgeTelemetrics\EventCorrelation\Library\Actions\ActionHelper
 */
class RPCCompressionTest extends TestCase
{
    public function buildObservableScheduler(): ObservableScheduler
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
    public function testSourceWithCompressionDeliversEvents(): void
    {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->register_input_process('test', wrap_source_php_cmd(__DIR__ . '/scripts/Source/CountToTen.php', true), env: ['PHPEC_RPC_COMPRESSION' => '1']);

        $inputEvents = [];
        $scheduler->setHandleEventCallback(function($event) use (&$inputEvents, $scheduler) {
            if ($event->event === 'Count') {
                $inputEvents[] = $event->value;
            }
        }, null);

        $scheduler->run();

        $this->assertCount(10, $inputEvents);
        $this->assertEquals([1,2,3,4,5,6,7,8,9,10], $inputEvents);
    }

    /** @small */
    public function testActionWithCompressionExecutesSuccessfully(): void
    {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->register_action('scriptAction',
            php_cmd(__DIR__ . '/scripts/Actions/logToScheduler.php'),
            env: ['PHPEC_RPC_COMPRESSION' => '1']);

        $scheduler->on('action.completed', function($action) use ($scheduler) {
            Loop::futureTick($scheduler->shutdown(...));
        });

        Loop::futureTick(function () use ($scheduler, $logger) {
            $logger->debug('Running action');
            $scheduler->queueAction(new Action('scriptAction', ['scriptAction ran and able to log']));
        });

        $scheduler->run();

        $logger->assert()
            ->hasLog()
            ->withMessage('Action called : ["scriptAction ran and able to log"]');
    }

    /** @small */
    public function testSourceWithoutCompressionStillWorks(): void
    {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->register_input_process('test', wrap_source_php_cmd(__DIR__ . '/scripts/Source/CountToTen.php', true));

        $inputEvents = [];
        $scheduler->setHandleEventCallback(function($event) use (&$inputEvents) {
            if ($event->event === 'Count') {
                $inputEvents[] = $event->value;
            }
        }, null);

        $scheduler->run();

        $this->assertCount(10, $inputEvents);
        $this->assertEquals([1,2,3,4,5,6,7,8,9,10], $inputEvents);
    }

    /** @small */
    public function testActionWithoutCompressionStillWorks(): void
    {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->register_action('scriptAction',
            php_cmd(__DIR__ . '/scripts/Actions/logToScheduler.php'));

        $scheduler->on('action.completed', function($action) use ($scheduler) {
            $scheduler->shutdown();
        });

        Loop::futureTick(function () use ($scheduler) {
            $scheduler->queueAction(new Action('scriptAction', ['backward compat works']));
        });

        $scheduler->run();

        $logger->assert()
            ->hasLog()
            ->withMessage('Action called : ["backward compat works"]');
    }
}
