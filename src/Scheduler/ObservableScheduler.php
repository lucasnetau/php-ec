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
use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Scheduler;

class ObservableScheduler extends Scheduler {

    protected Closure $eventHandlingCallback;

    protected Closure $actionHandlingCallback;

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

    public function setHandleEventCallback(null|callable $callback): void {
        $this->eventHandlingCallback = $callback === null ? null : $callback(...);
    }

    public function setHandleActionCallback(null|callable $callback): void {
        $this->eventHandlingCallback = $callback === null ? null : $callback(...);
    }

    protected function handleEvent(Event $event): void {
        if (isset($this->eventHandlingCallback)) {
            $callback = $this->eventHandlingCallback;
            $callback($event);
        }
        parent::handleEvent($event);
    }

    protected function handleAction(Action $action): void {
        if (isset($this->actionHandlingCallback)) {
            $callback = $this->actionHandlingCallback;
            $callback($action);
        }
        parent::handleAction($action);
    }
}