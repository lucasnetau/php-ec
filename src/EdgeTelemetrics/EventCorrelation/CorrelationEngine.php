<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

use EdgeTelemetrics\EventCorrelation\StateMachine\AEventProcessor;
use EdgeTelemetrics\EventCorrelation\StateMachine\IEventMatcher;
use EdgeTelemetrics\EventCorrelation\StateMachine\IEventGenerator;
use EdgeTelemetrics\EventCorrelation\StateMachine\IActionGenerator;

use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;

use function abs;
use function array_key_exists;
use function get_class;
use function in_array;
use function spl_object_hash;
use function count;
use function is_a;
use function array_multisort;
use function array_map;
use function array_sum;
use function array_slice;
use function serialize;

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

    protected $epsCounter;

    /**
     * Counter length (in seconds) for recording events per second metrics.
     */
    const EPS_COUNTER_LENGTH = 3600;

    protected $lastEventReal;

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
        $this->epsCounter = array_fill(0,self::EPS_COUNTER_LENGTH, 0);
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

        /** Record that we have seen an event of this type */
        $this->incrStat('seen', (string)$event->event);

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

        /** When we are parsing historical event stream data manually trigger any timeouts up until 1 second before the current event
         * Any timeouts at the current time will be triggered after handling the current incoming event
         */
        if (false === $this->eventstream_live)
        {
            $this->checkTimeouts($event->datetime->modify('-1 second'));
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
                    /** Record that we handled an event of this type */
                    $this->incrStat('handled', (string)$event->event . "|" . get_class($matcher));
                }

                if ($this->isFlagSet($result, $matcher::EVENT_TIMEOUT))
                {
                    $timedOutMatchers[] = $matcher;
                }

                //Matcher has told us to suppress further processing of this event.
                if ($this->isFlagSet($result, $matcher::EVENT_SUPPRESS))
                {
                    $suppress = true;
                    /** Record that the event was suppressed */
                    $this->incrStat('suppressed', (string)$event->event);
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
                    $this->incrStat('init_matcher', $class);
                    $this->incrStat('handled', (string)$event->event . "|" . get_class($matcher));

                    if (!$matcher->complete())
                    {
                        $this->addWatchForEvents($matcher, $matcher->nextAcceptedEvents());
                    }
                }
                else
                {
                    //Matcher did not want our event after all, discard it.
                    unset($matcher);
                    continue;
                }

                /** The current matcher has told us to suppress further processing of this event, break out of processing any further */
                if ($this->isFlagSet($result, $matcher::EVENT_SUPPRESS))
                {
                    /** Record that the event was suppressed */
                    $this->incrStat('suppressed', (string)$event->event);
                    break;
                }
            }
        }

        if (count($handledMatchers) == 0)
        {
            $this->incrStat('unhandled', (string)$event->event);
        }

        /** For any matchers that processed this event fire any actions, then update timeout or destroy if complete **/
        foreach($handledMatchers as $matcher)
        {
            $matcher->fire();
            $this->addTimeout($matcher);

            if ($matcher->complete())
            {
                /** Record stat of matcher completing */
                $this->incrStat('completed_matcher', get_class($matcher));
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
            /** Record stat of matcher timeout */
            $this->incrStat('completed_matcher', get_class($matcher));
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

        /** Increment event per second counters */
        $this->incrEps();
    }

    /**
     * Construct a new matcher EventProcessor and attach handlers for any events
     * @param string $className
     * @return IEventMatcher
     * @throws \RuntimeException;
     */
    public function constructMatcher(string $className)
    {
        if (is_a($className, 'EdgeTelemetrics\EventCorrelation\StateMachine\IEventMatcher', true))
        {
            /** @var IEventMatcher $matcher */
            $matcher = new $className();
            $this->attachListeners($matcher);
            /** @var IEventMatcher $matcher */
            return $matcher;
        }
        else
        {
            throw new \RuntimeException("{$className} does not implement EdgeTelemetrics\EventCorrelation\StateMachine\IEventMatcher");
        }
    }

    public function attachListeners(IEventMatcher $matcher)
    {
        if (is_a($matcher, 'EdgeTelemetrics\EventCorrelation\StateMachine\IEventGenerator') ||
            is_a($matcher, 'EdgeTelemetrics\EventCorrelation\StateMachine\IActionGenerator')
        ) {
            /** @var IEventGenerator|IActionGenerator $matcher */
            $matcher->on('data', [$this, 'handleEmit']);
        }
    }

    public function handleEmit($data)
    {
        /** Check if this is an event */
        if (is_a($data, 'EdgeTelemetrics\EventCorrelation\IEvent'))
        {
            /** @var \EdgeTelemetrics\EventCorrelation\IEvent $data */
            $this->incrStat('emit_event', $data->event);
            $this->emit('event', [$data]);
        }
        elseif (is_a($data, 'EdgeTelemetrics\EventCorrelation\Action'))
        {
            /** @var \EdgeTelemetrics\EventCorrelation\Action $data */
            $this->incrStat('emit_action', $data->getCmd());
            $this->emit('action', [$data]);
        }
        else
        {
            throw new \RuntimeException("Expected rules to emit an IEvent or Action. Unable to handle object of class " . get_class($data));
        }
    }

    /**
     * Keep note that the state machine $matcher is waiting for events $events
     * @param IEventMatcher $matcher
     * @param array $events
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
     * @param IEventMatcher $matcher
     * @param array $events
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
     * @return array
     */
    public function getTimeouts()
    {
        //Sort by timeout
        if (false === $this->timeoutsSorted) {
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
                    /** Remove all references if the matcher is complete */
                    $this->removeWatchForEvents($matcher, $matcher->nextAcceptedEvents());
                    $this->removeTimeout($matcher);
                    unset($this->eventProcessors[spl_object_hash($matcher)]);
                    unset($matcher);
                }
                else
                {
                    /** Update the timeout for this matcher after it has alarmed but has not timed out */
                    $this->addTimeout($matcher);
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
        $state['statistics'] = $this->statistics;
        $state['load'] = $this->calcLoad();
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
        return $state;
    }

    public function setState($state)
    {
        $this->statistics = $state['statistics'];
        $events = [];
        foreach($state['events'] as $hash => $eventData)
        {
            /**
             * First we unserialize all the events. We construct a table using the saved  object hashes instead of the new ones
             * to allow the saved state machines to identify their events.
             */
            $event = unserialize($eventData);
            $events[$hash] = $event;
        }
        foreach($state['matchers'] as $matcherState)
        {
            /**
             * Reconstruct each state machine.
             * 1. Unserialise to an object
             * 2. Pass in all events to the state machine for it to resolve it's recorded events
             * 3. Attach listeners to the state machine for events
             * 4. Add state machine to our records
             * 5. Let the engine know what events to forward to the state machine.
             */
            /** @var AEventProcessor $matcher */
            $matcher = unserialize($matcherState);
            $matcher->resolveEvents($events);
            $this->attachListeners($matcher);
            $this->eventProcessors[spl_object_hash($matcher)] = $matcher;
            $this->addWatchForEvents($matcher, $matcher->nextAcceptedEvents());
        }

        if (true === $state['eventstream_live'])
        {
            $this->setEventStreamLive();
        }
        else
        {
            foreach($this->eventProcessors as $matcher )
            {
                $this->addTimeout($matcher); //No need to call updateTimeout() first, it is done by the unserialisation
            }
        }
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

    /**
     * @param string $name
     * @param string $group
     * @param int $incr
     */
    public function incrStat(string $group, string $name, $incr = 1)
    {
        if (!isset($this->statistics[$group])) { $this->statistics[$group] = []; }
        if (!isset($this->statistics[$group][$name])) { $this->statistics[$group][$name] = 0; }
        $this->statistics[$group][$name] += $incr;
    }

    /**
     * Increment the event per second counters
     */
    public function incrEps()
    {
        $time = time();
        $index = $time % self::EPS_COUNTER_LENGTH;

        $this->flushOldEps();
        $this->epsCounter[$index]++;

        $this->lastEventReal = $time;
    }

    public function flushOldEps()
    {
        $time = time();
        $index = $time % self::EPS_COUNTER_LENGTH;

        /** Don't flush if we are tracking the current second */
        if (null === $this->lastEventReal || $time == $this->lastEventReal)
        {
            return;
        }

        /** Check if we have not processed an event for the max measurement period and reset all counters */
        if (($time - $this->lastEventReal) >= self::EPS_COUNTER_LENGTH) {
            array_fill(0,self::EPS_COUNTER_LENGTH, 0);
        }
        else
        {
            /** We are a new time period */
            $lastIndex = $this->lastEventReal % self::EPS_COUNTER_LENGTH;
            if ($index >= $lastIndex)
            {
                for($i = $lastIndex+1; $i <= $index; $i++)
                {
                    $this->epsCounter[$i] = 0;
                }
            }
            else
            {
                for($i = 0; $i <= $index; $i++)
                {
                    $this->epsCounter[$i] = 0;
                }
                for($i = $lastIndex+1; $i < self::EPS_COUNTER_LENGTH; $i++)
                {
                    $this->epsCounter[$i] = 0;
                }
            }
        }
    }

    public function calcLoad()
    {
        $time = time();
        $index = $time % self::EPS_COUNTER_LENGTH; /* seconds in an hour. Calculating over an hour */

        $this->flushOldEps();

        /** @var array $shiftedArray Shift the counter so that the current time modulus is the last item in the array */
        $shiftedArray = array_merge(array_slice($this->epsCounter, $index+1), array_slice($this->epsCounter, 0, $index+1));

        return [
            'lastEvent' => $this->lastEventReal,
            'hour' => array_sum($this->epsCounter),
            'fifteen' => array_sum(array_slice($shiftedArray, -900)),
            'minute' => array_sum(array_slice($shiftedArray, -60)),
            'counter' => implode(",", $shiftedArray),
        ];
    }
}
