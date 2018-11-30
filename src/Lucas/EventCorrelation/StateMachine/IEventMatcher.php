<?php declare(strict_types=1);

namespace Lucas\EventCorrelation\StateMachine;

use Lucas\EventCorrelation\Event;

/**
 * Interface IEventMatcher
 * @package Lucas\EventCorrelation\StateMachine
 *
 * Interface declaring that we emit can match and handle events.
 */
Interface IEventMatcher extends \JsonSerializable, \Serializable {

    const EVENT_HANDLED = 1;
    const EVENT_SKIPPED = 2;
    const EVENT_SUPPRESS = 4;
    const EVENT_TIMEOUT = 8;

    const EVENTSOURCE_TIMESTAMP_SOURCE = 1; // Use the event timestamps in the event
    const EVENTSOURCE_TIMESTAMP_SERVER = 2; // Ignore the event timestamps in the event and instead use the server time when received

    public static function initialAcceptedEvents() : array;

    public function handle(Event $event) : int;

    public function nextAcceptedEvents() : array;

    public function complete() : bool;

    public function fire();

    public function alarm();

    public function firstEventDateTime(): ?\DateTimeInterface;

    public function lastSeenEventDateTime(): ?\DateTimeInterface;

    public function getEventChain(): array;

    public function getTimeout(): ?\DateTimeInterface;

    public function isTimedOut() : bool;

}