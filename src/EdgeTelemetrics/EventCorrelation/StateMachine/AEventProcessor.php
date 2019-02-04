<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\StateMachine;

use EdgeTelemetrics\EventCorrelation\Event;
use Evenement\EventEmitterTrait;
use function count;
use function in_array;
use function array_key_first;
use function array_key_last;
use function array_map;
use function spl_object_hash;
use function method_exists;
use function json_encode;
use function json_decode;

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
 *          (eg sending a email when a later event complete the processor
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

    /**
    * @var bool Are we processing the events live in real time, or is this historical data. Default to false (historical).
    */
    protected static $eventstream_live = false;

    /**
     * @var bool Flag is actions have been fired. Used to prevent double firing
     */
    protected $actionFired = false;

    /**
     * @var array of Events that have been consumed
     */
    protected $consumedEvents = [];

    /**
     * @var array Used by an event processor to keep track of the context of processing.
     */
    protected $context = [];

    /**
     * @var bool Flag if the processor has completed due to timeout been reached
     */
    protected $isTimedOut = false;

    /**
     * @var \DateTimeInterface|null Calculated next timeout for the processor
     */
    protected $timeout = null;

    /**
     * @var array Used when loading a serialised event processor (two step pass)
     */
    protected $unresolved_events;

    /**
     * @var string Timeout expressed as a period string
     */
    const TIMEOUT = 'PT0S';

    /**
     * @var bool When we are in historical mode do we suppress timeouts.
     */
    const HISTORICAL_IGNORE_TIMEOUT = false; // We can ignore the timeout if following up on events

    /**
     * @var array Array of array of events this processor will handle
     */
    const EVENTS = [[]];

    /**
     * Get the event(s) that this state machine class will start on
     * @return array
     */
    public static function initialAcceptedEvents() : array
    {
        return static::EVENTS[0];
    }

    /**
     * Get the event(s) that this state machine is waiting for next
     * @return array
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
     * Process an incoming event
     * @param Event $event
     * @return int
     * @throws \Exception
     */
    public function handle(Event $event) : int
    {
        if ($this->complete())
        {
            throw new \Exception("Already complete, cannot handle additional events");
        }
        if (in_array($event->event, $this->nextAcceptedEvents(), true))
        {
            if ($this->acceptEvent($event))
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
        return true;
    }

    /**
     * Have all events expected been handled?
     * @return bool
     */
    public function complete() : bool
    {
        return (count($this->consumedEvents) == count(static::EVENTS));
    }

    /**
     * Get the current chain of events that have been consumed
     * @return array
     */
    public function getEventChain() : array
    {
        return $this->consumedEvents;
    }

    /**
     * Get the data time of the first event consumed
     * @return \DateTimeImmutable|null
     */
    public function firstEventDateTime(): ?\DateTimeInterface
    {
        if (0 === count($this->consumedEvents))
        {
            return null;
        }
        return $this->consumedEvents[array_key_first($this->consumedEvents)]->datetime;
    }

    /**
     * Get the date time of the last event consumed
     * @return \DateTimeImmutable|null
     */
    public function lastSeenEventDateTime(): ?\DateTimeInterface
    {
        if (0 === count($this->consumedEvents))
        {
            return null;
        }
        return $this->consumedEvents[array_key_last($this->consumedEvents)]->datetime;
    }

    /**
     * Check if we accept the timestamp of the event based on timeout
     * @param Event $event
     * @return bool
     */
    public function acceptEventTime(Event $event) : bool
    {
        $timeout = $this->getTimeout();
        return (null === $timeout || $event->datetime < $timeout);
    }

    /**
     * Update the timeout after consuming an event. Cache this value rather than calculating it each call
     * @throws \Exception
     */
    public function updateTimeout()
    {
        /**
         * There is no timeout if any of the following are matched:
         *
         * > Timeout interval string is 0 seconds
         * > Event stream is not live and we are ignoring timeouts while processing historical events
         * > We have not consumed any events
         * > We have completed our task
         */
        if ('PT0S' == static::TIMEOUT
            || (false === self::$eventstream_live && static::HISTORICAL_IGNORE_TIMEOUT)
            || 0 == count($this->consumedEvents)
            || $this->complete())
        {
            $this->timeout = null;
        }
        else
        {
            $this->timeout = $this->lastSeenEventDateTime()->add(new \DateInterval(static::TIMEOUT));
        }
    }

    /**
     * Get date time of when this state machine will timeout
     * @return \DateTimeImmutable|null
     */
    public function getTimeout() : ?\DateTimeInterface
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
    public function alarm()
    {
        $this->isTimedOut = true;
    }

    /**
     * Use the real time timestamp
     */
    public static function setEventStreamLive()
    {
        self::$eventstream_live = true;
    }

    public function jsonSerialize()
    {
        $return = [];
        $return['events'] = array_map(function($event){ return spl_object_hash($event); } , $this->consumedEvents);

        $return['actionFired'] = $this->actionFired;
        $return['isTimedOut'] = $this->isTimedOut;
        $return['context'] = $this->context;

        if (method_exists($this, 'serializeMetrics'))
        {
            $return['metrics'] = $this->serializeMetrics();
        }

        return $return;
    }

    public function serialize()
    {
        return json_encode($this);
    }

    public function unserialize($data)
    {
        $data = json_decode($data, true);
        $this->unresolved_events = $data['events'];

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

    public function resolveEvents($events)
    {
        $this->consumedEvents = array_map(function($event_id) use ($events) { return $events[$event_id]; } , $this->unresolved_events);
        unset($this->unresolved_events);
    }
}