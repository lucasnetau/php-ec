<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library;

use EdgeTelemetrics\EventCorrelation\Event;
use function array_shift;
use function count;
use function json_encode;

/**
 * @internal
 */
class EventLog {

    protected array $events = [];

    public function __construct(protected int $maxEvents = 100) {

    }

    public function add(Event $event): void {
        $this->events[] = [
            'datetime' => $event->datetime->format('Y-m-d H:i:s'),
            'event'    => $event->event,
            'data'     => $event->getEventName() ? json_encode($event->getEventName()) : null,
        ];
        if (count($this->events) > $this->maxEvents) {
            array_shift($this->events);
        }
    }

    public function getEvents(): array {
        return $this->events;
    }

}