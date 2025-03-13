<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Library\Actions\InvokableClassAction;
use EdgeTelemetrics\EventCorrelation\Library\Source\GeneratorSource;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\State;
use EdgeTelemetrics\EventCorrelation\StateMachine\AEventProcessor;
use EdgeTelemetrics\EventCorrelation\Tests\Rules\TimeoutRule;
use Exception;
use Generator;
use PHPUnit\Framework\TestCase;
use VStelmakh\PsrTestLogger\TestLogger;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;
use function var_dump;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\StateMachine\AEventProcessor
 * @covers \EdgeTelemetrics\EventCorrelation\Rule
 */
class RuleTest extends TestCase {
    public function buildObservableScheduler(array $rules = []) : ObservableScheduler {
        $scheduler = new ObservableScheduler($rules);
        $tempState = tempnam(sys_get_temp_dir(), 'php-ec-test-state-');
        if ($tempState !== false) {
            unlink($tempState);
        }
        $scheduler->setSavefileName($tempState);
        $scheduler->setSaveStateInterval(600);
        return $scheduler;
    }

    public function testRuleWithTimeoutTriggersAlarmAndOnTimeout(): void {
        $scheduler = $this->buildObservableScheduler([TimeoutRule::class]);
        $scheduler->setRealtime();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $wasCalled = false;
        $closure = function($vars) use (&$wasCalled, $scheduler) {
            $wasCalled = true;
            /** @var $this InvokableClassAction */
            $this->logger->info(json_encode($vars));
            $scheduler->shutdown();
        };

        $scheduler->register_action('recordTimeout', $closure);

        $scheduler->run();

        $this->assertEquals(State::STOPPED, $scheduler->getExecutionState()->state());

        $logger->assert()
            ->hasLog()
            ->withMessage('["alarm called","timeout called"]');

        $this->assertTrue($wasCalled, 'recordTimeout was not called');
    }

    /**
     * @covers \EdgeTelemetrics\EventCorrelation\Scheduler::panic
     */
    public function testRuleThatThrowsCausesPanic(): void {

        $unclean = new class() extends Rule {
            const EVENTS = [[self::EVENT_MATCH_ANY]];
            public function handle(Event $event) : int {
                throw new Exception('This should never happen');
            }
        };

        $scheduler = $this->buildObservableScheduler([$unclean]);

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('This should never happen');

        $scheduler->run();

        $this->assertEquals(State::STOPPED_UNCLEAN, $scheduler->getExecutionState()->state());

        $logger->assert()
            ->hasEmergency()
            ->withMessage('Rules must not throw exceptions');
    }
}