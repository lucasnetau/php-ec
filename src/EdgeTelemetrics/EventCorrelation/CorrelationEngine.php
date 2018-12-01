<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

use EdgeTelemetrics\EventCorrelation\StateMachine\IEventMatcher;
use EdgeTelemetrics\EventCorrelation\StateMachine\IEventGenerator;
use EdgeTelemetrics\EventCorrelation\StateMachine\IActionGenerator;

use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;

class CorrelationEngine implements EventEmitterInterface {
    use EventEmitterTrait;

    protected $eventProcessors = [];

    protected $initialEventLookup = [];

    /**
     * @var array Array of IEventMatchers
     */
    protected $waitingForNextEvent = [];

    protected $timeouts = [];

    protected $eventstream_live = false;

    protected $dirty = false;

    protected $timeoutsSorted = false;

    /** @var array  */
    protected $statistics = [];

    const MAX_TIME_VARIANCE = 600; // In seconds

    public function __construct(array $rules)
    {
        foreach($rules as $matcher)
        {
            foreach($matcher::initialAcceptedEvents() as $eventname)
            {
                $this->initialEventLookup[$eventname][] = $matcher;
            }
        }
    }

    public function isFlagSet(int $check, int $flag)
    {
        return ($check & $flag) == $flag;
    }

    public function handle(Event $event)
    {
        $handledMatchers = [];
        $skipMatchers = [];
        $timedOutMatchers = [];
        $suppress = false;

        /** If the event stream is live we want to make sure the event timestamp is within
         *  10 minutes of the current time, otherwise we will set it to the server time.
         */
        if (true === $this->eventstream_live)
        {
            $now = new \DateTimeImmutable();
            if (abs($now->getTimestamp() - $event->datetime->getTimestamp()) > (self::MAX_TIME_VARIANCE))
            {
                echo "Correcting received time to {$now->format('c')}\n";
                $event->setReceivedTime($now);
            }
        }

        /**
         * Check existing state machines first to see if the event can be handled
         */
        if (array_key_exists($event->event, $this->waitingForNextEvent))
        {
            foreach($this->waitingForNextEvent[$event->event] as $key => $matcher)
            {
                /* @var $matcher IEventMatcher */
                $expecting = $matcher->nextAcceptedEvents();
                $result = $matcher->handle($event);
                if ($this->isFlagSet($result, $matcher::EVENT_HANDLED))
                {
                    $handledMatchers[] = $matcher;
                    $skipMatchers[] = get_class($matcher);
                    /** Update which events we are expecting next **/
                    $this->removeWatchForEvents($matcher, $expecting);
                    if (!$matcher->complete())
                    {
                        $this->addWatchForEvents($matcher, $matcher->nextAcceptedEvents());
                    }
                }

                if ($this->isFlagSet($result, $matcher::EVENT_TIMEOUT))
                {
                    $timedOutMatchers[] = $matcher;
                }

                //Matcher has told us to suppress further processing of this event.
                if ($this->isFlagSet($result, $matcher::EVENT_SUPPRESS))
                {
                    $suppress = true;
                    break;
                }
            }
        }

        /**
         * Finally check if we need to start up any more state machines for this event.
         * A new state machine will not be created if an existing state machine suppressed the event
         * or if a state machine of the same class handled the event
         */
        if (false === $suppress && array_key_exists($event->event, $this->initialEventLookup))
        {
            foreach($this->initialEventLookup[$event->event] as $class)
            {
                /** If this className has already handled this event, don't create another **/
                if (in_array($class, $skipMatchers))
                {
                    continue;
                }
                $matcher = $this->constructMatcher($class);
                $result = $matcher->handle($event);
                if ($this->isFlagSet($result, $matcher::EVENT_HANDLED))
                {
                    $handledMatchers[] = $matcher;
                    $this->eventProcessors[spl_object_hash($matcher)] = $matcher;
                }

                if (!$matcher->complete())
                {
                    $this->addWatchForEvents($matcher, $matcher->nextAcceptedEvents());
                }

                /** The current matcher has told us to suppress further processing of this event, break out of processing any further */
                if ($this->isFlagSet($result, $matcher::EVENT_SUPPRESS))
                {
                    break;
                }
            }
        }

        /** For any matchers that processed this event fire any actions, then update timeout or destroy if complete **/
        foreach($handledMatchers as $matcher)
        {
            $matcher->fire();
            $this->addTimeout($matcher);

            if ($matcher->complete())
            {
                unset($this->eventProcessors[spl_object_hash($matcher)]);
                unset($matcher);
                continue;
            }
        }
        /**  Fire any action and destroy any timed out matchers **/
        foreach($timedOutMatchers as $matcher)
        {
            $this->removeTimeout($matcher);
            $matcher->fire();
            unset($this->eventProcessors[spl_object_hash($matcher)]);
            unset($matcher);
        }

        /** When we are parsing historical event stream data manually trigger any timeouts **/
        if (false === $this->eventstream_live)
        {
            $this->checkTimeouts($event->datetime);
        }

        /** Flag as dirty **/
        $this->dirty = true;
    }

    /**
     * Construct a new matcher EventProcessor and attach handlers for any events
     * @param $className
     * @return IEventGenerator|IEventMatcher
     * @throws \RuntimeException;
     */
    public function constructMatcher($className)
    {
        if (is_a($className, 'EdgeTelemetrics\EventCorrelation\StateMachine\IEventMatcher', true))
        {
            /** @var IEventMatcher|IActionGenerator $matcher */
            $matcher = new $className();
            if (is_a($matcher, 'EdgeTelemetrics\EventCorrelation\StateMachine\IEventGenerator') ||
                is_a($matcher, 'EdgeTelemetrics\EventCorrelation\StateMachine\IActionGenerator')
            ) {
                /** @var IEventGenerator $matcher */
                $matcher->on('data', [$this, 'handleEmit']);
            }
            return $matcher;
        }
        else
        {
            throw new \RuntimeException("{$className} does not implement EdgeTelemetrics\EventCorrelation\StateMachine\IEventMatcher");
        }
    }

    public function handleEmit($data)
    {
        /** Check if this is an event */
        if (is_a($data, 'EdgeTelemetrics\EventCorrelation\IEvent'))
        {
            $this->emit('event', [$data]);
        }
        elseif (is_a($data, 'EdgeTelemetrics\EventCorrelation\Action'))
        {
            $this->emit('action', [$data]);
        }
        else
        {
            throw new \RuntimeException("Expected event to implement IEvent");
        }
    }

    /**
     * Keep note that the state machine $matcher is waiting for events $events
     * @param $matcher
     * @param $events
     */
    public function addWatchForEvents(IEventMatcher $matcher, array $events)
    {
        foreach($events as $eventName)
        {
            $this->waitingForNextEvent[$eventName][spl_object_hash($matcher)] = $matcher;
        }
    }

    /**
     * Remove record that $matcher is waiting for certain events
     * @param $matcher
     * @param $events
     */
    public function removeWatchForEvents(IEventMatcher $matcher, array $events)
    {
        foreach($events as $eventName)
        {
            unset($this->waitingForNextEvent[$eventName][spl_object_hash($matcher)]);
            if (0 == count($this->waitingForNextEvent[$eventName]))
            {
                unset($this->waitingForNextEvent[$eventName]);
            }
        }

        if (empty($this->waitingForNextEvent))
        {
            $this->waitingForNextEvent = []; // Create new memory to allow GC of Array
        }
    }

    /**
     * Add timeout will add or remove a timeout for the matcher passed in.
     * @param IEventMatcher $matcher
     * @todo Look at whether we should keep track of the sorted state of the timeouts with a flag, do a quick
     *  insert at the front or end of the array by checking first/last instead of sorting for big sets
     */
    public function addTimeout(IEventMatcher $matcher)
    {
        $timeout = $matcher->getTimeout();
        if (null === $timeout)
        {
            $this->removeTimeout($matcher);
        }
        else
        {
            //@TODO check if we really need to add this in case where timeout is the same as current
            $this->timeouts[spl_object_hash($matcher)] = ['timeout' => $timeout, 'matcher' => $matcher];
            $this->timeoutsSorted = false;
        }
    }

    public function removeTimeout(IEventMatcher $matcher)
    { 
        unset($this->timeouts[spl_object_hash($matcher)]);
    }

    /**
     * Get the current timeouts for all running state machines. Sort the list prior to returning
     * @return array<\DateTimeImmutable>
     */
    public function getTimeouts()
    {
        //Sort by timeout
        if (false === $this->timeoutsSorted) {
            echo "Sorted timeouts\n";
            array_multisort(array_map(function ($element) {
                return $element['timeout']->format('Ydm His');
            }, $this->timeouts), SORT_DESC, $this->timeouts);
            $this->timeoutsSorted = true;
        }
        return $this->timeouts;
    }

    public function setEventStreamLive()
    {
        /**
         * When setting the event stream to live we need to reset our tracking of timeouts,
         * as some may have been suppressed during the historical event processing and now need to be queued
         */
        $this->eventstream_live = true;
        StateMachine\AEventProcessor::setEventStreamLive();
        $this->timeouts = [];

        foreach($this->eventProcessors as $matcher )
        {
            $matcher->updateTimeout();
            $this->addTimeout($matcher);
        }
        $this->checkTimeouts(new \DateTimeImmutable());
    }

    public function isRealtime()
    {
        return $this->eventstream_live;
    }

    /**
     * Check if any timeouts are prior to the $time passed and if so trigger the timeout logic for the respective matcher
     * @param \DateTimeInterface $time
     */
    public function checkTimeouts(\DateTimeInterface $time)
    {
        foreach ( $this->getTimeouts() as $timeout)
        {
            if ($time > $timeout['timeout'])
            {
                /**
                 * @var IEventMatcher $matcher
                 */
                $matcher = $timeout['matcher'];
                $matcher->alarm();
                $matcher->fire();
                if ($matcher->isTimedOut())
                {
                    $this->removeWatchForEvents($matcher, $matcher->nextAcceptedEvents());
                    $this->removeTimeout($matcher);
                    unset($matcher);
                }
                $this->dirty = true;
            }
            else
            {
                /**
                 * Timeouts are sorted so if current event is before timeout then return early
                 */
                return;
            }
        }
    }

    public function getState() : array
    {
        $state = [];
        $state['eventstream_live'] = $this->eventstream_live;
        $state['matchers'] = [];
        $state['events'] = [];
        foreach($this->waitingForNextEvent as $matchers)
        {
            foreach($matchers as $matcher)
            {
                /** @var IEventMatcher $matcher */
                $matcher_hash = spl_object_hash($matcher);
                if (isset($state['matchers'][$matcher_hash]))
                {
                    continue;
                }

                foreach($matcher->getEventChain() as $event)
                {
                    $event_hash = spl_object_hash($event);
                    if (isset($state['events'][$event_hash]))
                    {
                        continue;
                    }
                    $state['events'][$event_hash] = serialize($event);
                }

                $state['matchers'][] = serialize($matcher);
            }
        }
        $this->dirty = false;
        return $state;
    }

    public function setState($state)
    {

    }

    /**
     * Return true if state has been modified, false if it hasn't changed since last clean
     * @return bool
     */
    public function isDirty(): bool
    {
        return $this->dirty;
    }

    /**
     * Clear the dirty flag
     */
    public function clearDirtyFlag()
    {
        $this->dirty = false;
    }
}