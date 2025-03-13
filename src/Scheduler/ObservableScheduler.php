<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation\Scheduler;

use Closure;
use DateTimeImmutable;
use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Scheduler;

class ObservableScheduler extends Scheduler {

    protected ?Closure $preEventHandlingCallback = null;

    protected ?Closure $preActionHandlingCallback = null;

    protected ?Closure $postEventHandlingCallback = null;

    protected ?Closure $postActionHandlingCallback = null;

    public function __construct(array $rules, bool $live = false) {
        parent::__construct($rules);
        if ($live) {
            $this->engine->setEventStreamLive();
        }
    }

    public function setRealtime(): void{
        $this->engine->setEventStreamLive();
    }

    public function queueEvent(Event $event): void {
        $this->loop->futureTick(function () use ($event) {
            $this->handleEvent($event);
        });
    }

    public function queueAction(Action $action): void {
        $this->loop->futureTick(function () use ($action) {
            $this->handleAction($action);
        });
    }

    public function setHandleEventCallback(null|callable $preCallback, null|callable $postCallback): void {
        $this->preEventHandlingCallback = $preCallback === null ? null : $preCallback(...);
        $this->postEventHandlingCallback = $postCallback === null ? null : $postCallback(...);
    }

    public function setHandleActionCallback(null|callable $preCallback, null|callable $postCallback): void {
        $this->preActionHandlingCallback = $preCallback === null ? null : $preCallback(...);
        $this->postActionHandlingCallback = $postCallback === null ? null : $postCallback(...);
    }

    public function getExecutionState() : State {
        return $this->state;
    }

    public function getErroredActions() : array {
        return $this->erroredActionCommands;
    }

    public function getNextScheduledTimer() : DateTimeImmutable|null {
        if ($this->nextTimer === null) {
            return null;
        }

        return $this->timerScheduledAt->modify("+" . round($this->nextTimer->getInterval() * 1e6) . ' microseconds');
    }

    /** Overridden Functions */

    protected function handleEvent(Event $event): void {
        if (isset($this->preEventHandlingCallback)) {
            ($this->preEventHandlingCallback)($event);
        }
        parent::handleEvent($event);
        if (isset($this->postEventHandlingCallback)) {
            ($this->postEventHandlingCallback)($event);
        }
    }

    protected function handleAction(Action $action): void {
        if (isset($this->preActionHandlingCallback)) {
            ($this->preActionHandlingCallback)($action);
        }
        parent::handleAction($action);
        if (isset($this->postActionHandlingCallback)) {
            ($this->postActionHandlingCallback)($action);
        }
    }
}