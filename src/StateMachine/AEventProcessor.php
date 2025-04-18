<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation\StateMachine;

use DateInterval;
use DateMalformedIntervalStringException;
use DateTimeImmutable;
use DateTimeInterface;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\IEvent;
use Evenement\EventEmitterTrait;
use Exception;
use RuntimeException;
use function array_key_exists;
use function count;
use function in_array;
use function array_key_first;
use function array_key_last;
use function array_map;
use function spl_object_id;
use function method_exists;
use function json_encode;
use function json_decode;
use function bin2hex;
use function random_bytes;

/**
 * Class AEventProcessor
 * @package EdgeTelemetrics\EventCorrelation\StateMachine
 *
 * Types of EventProcessors and the various parameters and functions
 *
 *  Required
 *  - function fire() - Action to perform once  complete or timed out.
 *  Optional
 *  - function acceptEvent() - Implement to record context on first event and validate on additional events
 *  - function handle() - Implement custom handler logic (Not needed in most instances)
 *
 *  Constants that can be overridden to change the default behaviour of handleEvent
 *
 * - EVENTS = Array of arrays of events to handle. Each subarray will be processed in order.
 * - TIMEOUT = Single timeout value expressed as a Period string (eg PT30M for 30 Minutes)
 * - HISTORICAL_IGNORE_TIMEOUT (Bool) = When we are in historical mode do we suppress timeouts.
 *          Useful for catching up on events where timeouts may trigger an unintended event
 *          (eg sending a email when a later event complete the processor)
 *
 * * Single Event Processor
 *  - Will process a single event and fire once (EVENTS = array of one array of event(s))
 *  - No timeout (TIMEOUT = default)
 *
 * * Multiple Event Processor
 *  - Will process two or more events in order before firing once all events are complete (EVENTS = array of two or more arrays of events)
 *  - May restrict handling events based on event context (eg only events produced by one sensor)
 *  - No timeout (TIMEOUT = default)
 *
 * * Multiple Event Processor with Timeout
 *  - Will process two or more events in order before firing once all events are complete (EVENTS = array of two or more arrays of events)
 *  - May restrict handling events based on event context (eg only events produced by one sensor)
 *  - Can set a timeout to be triggered if next event not seen within that time frame
 *  - Can fire on timeout or completion
 */
abstract class AEventProcessor implements IEventMatcher, IEventGenerator {
    /** We generate events and alarms */
    use EventEmitterTrait;

    protected string $instanceId;

    /**
    * @var bool Are we processing the events live in real time, or is this historical data. Default to false (historical).
    */
    protected static bool $eventstream_live = false;

    /**
     * @var bool Flag is actions have been fired. Used to prevent double firing
     */
    protected bool $actionFired = false;

    /**
     * @var IEvent[] array of Events that have been consumed
     */
    protected array $consumedEvents = [];

    /**
     * @var array Used by an event processor to keep track of the context of processing.
     */
    protected array $context = [];

    /**
     * @var bool Flag if the processor has completed due to timeout been reached
     */
    protected bool $isTimedOut = false;

    /**
     * @var ?DateTimeInterface Calculated next timeout for the processor
     */
    protected ?DateTimeInterface $timeout = null;

    /**
     * @var string[] Used when loading a serialised event processor (two step pass)
     */
    protected array $unresolved_events;

    /**
     * @var string Timeout expressed as a period string
     */
    const TIMEOUT = 'PT0S';

    /**
     * @var string No Timeout expressed as a period string
     */
    const NO_TIMEOUT_STRING = 'PT0S';

    /**
     * @var bool When we are in historical mode do we suppress timeouts.
     */
    const HISTORICAL_IGNORE_TIMEOUT = false; // We can ignore the timeout if following up on events

    /**
     * @var string[][] Array of array of events this processor will handle
     */
    const EVENTS = [[]];

    public function __construct() {
        $this->instanceId = $this->generateInstanceId();
    }

    protected function generateInstanceId() : string {
        //Generate a random 5 byte instance id
        return bin2hex(random_bytes(5));
    }

    /**
     * Get the event(s) that this state machine class will start on
     * @return string[]
     */
    public static function initialAcceptedEvents() : array
    {
        return static::EVENTS[0];
    }

    /**
     * Get the event(s) that this state machine is waiting for next
     * @return string[]
     */
    public function nextAcceptedEvents() : array
    {
        $count = count($this->consumedEvents);
        if ($count < count(static::EVENTS))
        {
            return static::EVENTS[$count];
        }
        return [];
    }

    /**
     * Check if we accept this event type
     * @param Event $event
     * @return bool
     */
    protected function acceptEventType(Event $event) : bool {
        $acceptedTypes = $this->nextAcceptedEvents();
        return in_array($event->event, $acceptedTypes, true) || in_array(IEventMatcher::EVENT_MATCH_ANY, $acceptedTypes, true);
    }

    /**
     * Process an incoming event
     * @param Event $event
     * @return int
     * @throws Exception
     */
    public function handle(Event $event) : int
    {
        if ($this->complete())
        {
            throw new RuntimeException("Already complete, cannot handle additional events");
        }
        if ($this->acceptEventType($event) && $this->acceptEvent($event))
        {
            if (!$this->acceptEventTime($event))
            {
                $this->isTimedOut = true;
                return self::EVENT_TIMEOUT;
            }
            $this->consumedEvents[] = $event;
            $this->updateTimeout();
            return self::EVENT_HANDLED;
        }
        return self::EVENT_SKIPPED;
    }

    /**
     * Check if this event and the related context is something we will accept
     * @param Event $event
     * @return bool
     */
    public function acceptEvent(Event $event) : bool
    {
        return empty($this->consumedEvents) ? $this->acceptInitialEvent($event) : $this->acceptSubsequentEvent($event);
    }

    /**
     * Check if we accept the initial event and save context if required
     * @param Event $event
     * @return bool
     */
    public function acceptInitialEvent(Event $event) : bool
    {
        return true; //For simple Rules we always accept the event if it passes the acceptEventType check, no context is checked
    }

    /**
     * Check if we accept any subsequent event against stored context
     * @param Event $event
     * @return bool
     */
    public function acceptSubsequentEvent(Event $event) : bool
    {
        return true; //For simple Rules we always accept the event if it passes the acceptEventType check, no context is checked
    }

    /**
     * Have all events expected been handled?
     * @return bool
     */
    public function complete() : bool
    {
        return (count($this->consumedEvents) === count(static::EVENTS));
    }

    /**
     * Get the current chain of events that have been consumed
     * @return IEvent[]
     */
    public function getEventChain() : array
    {
        return $this->consumedEvents;
    }

    /**
     * Get the first event we handled
     * @return Event|null
     */
    public function getFirstEvent() : ?Event {
        return $this->consumedEvents[array_key_first($this->consumedEvents)] ?? null;
    }

    /**
     * Get the most recent (last) event we handled
     * @return Event|null
     */
    public function getLastEvent() : ?Event {
        return $this->consumedEvents[array_key_last($this->consumedEvents)] ?? null;
    }

    /**
     * Return one or more events matching the specified type in the order they were captured
     * @param string $type
     * @return IEvent|IEvent[]|null
     */
    public function getEventOfType(string $type) {
        $matched = [];
        foreach($this->consumedEvents as $event) {
            if ($event->getEventName() === $type) {
                $matched[] = $event;
            }
        }
        $cnt = count($matched);
        if (0 === $cnt) {
            return null;
        } elseif (1 === $cnt) {
            return $matched[0];
        } else {
            return $matched;
        }
    }

    /**
     * Trim the number of events we have consumed, retaining the most recent $length number of events
     * @param int $length
     */
    public function trimEventChain(int $length) : void
    {
        if ($length < 0)
        {
            throw new RuntimeException("Length must be equal to or greater than zero");
        }
        if ($length >= count($this->consumedEvents))
        {
            return;
        }
        $this->consumedEvents = array_slice($this->consumedEvents, (-1 * $length), $length, false);
    }

    /**
     * Get the data time of the first event consumed
     * @return ?DateTimeInterface
     */
    public function firstEventDateTime(): ?DateTimeInterface
    {
        return ($this->consumedEvents[array_key_first($this->consumedEvents)] ?? null)?->datetime;
    }

    /**
     * Get the date time of the last event consumed
     * @return DateTimeImmutable|null
     */
    public function lastSeenEventDateTime(): ?DateTimeInterface
    {
        return ($this->consumedEvents[array_key_last($this->consumedEvents)] ?? null)?->datetime;
    }

    /**
     * Check if we accept the timestamp of the event based on timeout
     * @param Event $event
     * @return bool
     */
    public function acceptEventTime(Event $event) : bool
    {
        $timeout = $this->getTimeout();
        return (null === $timeout || $event->datetime <= $timeout);
    }

    /**
     * Update the timeout after consuming an event. Cache this value rather than calculating it each call
     * @throws Exception
     */
    public function updateTimeout() : void
    {
        /**
         * There is no timeout if any of the following are matched:
         *
         * > Timeout interval string is 0 seconds
         * > Event stream is not live and we are ignoring timeouts while processing historical events
         * > We have not consumed any events
         * > We have completed our task
         */
        if (self::NO_TIMEOUT_STRING == static::TIMEOUT
            || (!self::$eventstream_live && static::HISTORICAL_IGNORE_TIMEOUT)
            || empty($this->consumedEvents)
            || $this->complete())
        {
            $this->timeout = null;
        }
        else
        {
            $lastSeen = $this->lastSeenEventDateTime();
            if (null !== $lastSeen) {
                try {
                    $timeout = new DateInterval(static::TIMEOUT);
                } catch (DateMalformedIntervalStringException $ex) {
                    trigger_error("TIMEOUT in " . $this::class . " cannot be parsed: " . $ex->getMessage(), E_USER_WARNING);
                }
                $this->timeout = $lastSeen->add($timeout ?? new DateInterval(static::NO_TIMEOUT_STRING));
            }
        }
    }

    /**
     * Get date time of when this state machine will timeout
     * @return ?DateTimeInterface
     */
    public function getTimeout() : ?DateTimeInterface
    {
        return $this->timeout;
    }

    /**
     * Check if we have timed out
     * @return bool
     */
    public function isTimedOut() : bool
    {
        return $this->isTimedOut;
    }

    /**
     * Wake up the EventProcessor once the request timeout has been reached
     */
    public function alarm() : void
    {
        $this->isTimedOut = true;
    }

    /**
     * Use the real time timestamp
     */
    public static function setEventStreamLive() : void
    {
        self::$eventstream_live = true;
    }

    /**
     * @return array
     */
    public function jsonSerialize() : array
    {
        $return = [];
        $return['events'] = array_map(function ($event) {
            return spl_object_id($event);
        }, $this->consumedEvents);

        $return['id'] = $this->instanceId;
        $return['actionFired'] = $this->actionFired;
        $return['isTimedOut'] = $this->isTimedOut;
        $return['context'] = $this->context;
        if ($this->timeout instanceof DateTimeInterface)
        {
            $return['timeout'] = $this->timeout->format('c');
        }

        if (method_exists($this, 'serializeMetrics'))
        {
            $return['metrics'] = $this->serializeMetrics();
        }

        return $return;
    }

    /**
     * @return false|string|null
     * @deprecated
     */
    public function serialize()
    {
        return json_encode($this, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);
    }

    /**
     * @return array
     */
    public function __serialize(): array
    {
        return $this->jsonSerialize();
    }

    /**
     * @param string $data
     * @throws Exception
     * @deprecated Support left to support loading older state files
     */
    public function unserialize($data)
    {
        $data = json_decode($data, true);
        $this->__unserialize($data);
    }

    /**
     * @param array<string, mixed> $data
     * @throws Exception
     */
    public function __unserialize(array $data) {
        //Detect old serialisation format
        if (count($data) === 1 && array_key_exists('data', $data)) {
            $this->unserialize($data['data']);
            return;
        }
        
        $this->unresolved_events = $data['events'];

        $this->instanceId = $data['instanceId'] ?? $this->generateInstanceId(); //Generate a new ID if we don't have one serialized
        $this->actionFired = $data['actionFired'];
        $this->isTimedOut = $data['isTimedOut'];
        $this->context = $data['context'];
        if (isset($data['metrics']) && method_exists($this, 'unserializeMetrics'))
        {
            $this->unserializeMetrics($data['metrics']);
            unset($data['metrics']);
        }
        $this->updateTimeout();
    }

    /**
     * @param IEvent[] $events
     */
    public function resolveEvents(array $events) : void
    {
        $this->consumedEvents = array_map(function($event_id) use ($events) { return $events[$event_id]; } , $this->unresolved_events);
        unset($this->unresolved_events);
    }

    /** Completion Callback handlers */
    public function fire(): void
    {
        if ($this->complete()) {
            $this->onComplete();
            $this->onDone();
        } elseif ($this->isTimedOut()) {
            $this->onTimeout();
            $this->onDone();
        } else {
            $this->onProgress();
            if ($this->complete()) {
                $this->onComplete();
                $this->onDone();
            }
        }
    }

    /** Called when EventProcessor has consumed all required events */
    public function onComplete() : void {}

    /** Called when EventProcessor's timeout has been reached */
    public function onTimeout() : void {}

    /** Called on consuming an event */
    public function onProgress() : void {}

    /** Called when either timeout or complete is reached */
    public function onDone() : void {}
}
