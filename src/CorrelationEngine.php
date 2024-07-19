<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation;

use DateInterval;
use EdgeTelemetrics\EventCorrelation\Rule\UndefinedRule;
use EdgeTelemetrics\EventCorrelation\Scheduler\Messages\ExecuteSource;
use EdgeTelemetrics\EventCorrelation\StateMachine\AEventProcessor;
use EdgeTelemetrics\EventCorrelation\StateMachine\IEventMatcher;
use EdgeTelemetrics\EventCorrelation\StateMachine\IEventGenerator;
use EdgeTelemetrics\EventCorrelation\StateMachine\IActionGenerator;
use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use DateTimeImmutable;
use DateTimeInterface;
use Exception;
use RuntimeException;

use function abs;
use function array_key_exists;
use function array_keys;
use function array_merge;
use function class_exists;
use function error_log;
use function fwrite;
use function get_class;
use function in_array;
use function ini_restore;
use function ini_set;
use function round;
use function spl_object_id;
use function count;
use function is_a;
use function array_sum;
use function array_slice;
use function serialize;
use function uasort;
use function unserialize;
use function implode;

/**
 * Class CorrelationEngine
 * @package EdgeTelemetrics\EventCorrelation
 */
class CorrelationEngine implements EventEmitterInterface {
    use EventEmitterTrait;

    /**
     * @var array<string, IEventMatcher>
     */
    protected array $eventProcessors = [];

    /**
     * @var array<string, class-string<IEventMatcher>[]>
     */
    protected array $initialEventLookup = [];

    /**
     * @var array<string, IEventMatcher[]> Array of IEventMatched object hashes waiting for an event identified by the root key
     */
    protected array $waitingForNextEvent = [];

    /** @var array<string, array<string, mixed>>  */
    protected array $timeouts = [];

    protected bool $eventstream_live = false;

    protected bool $dirty = false;

    protected bool $timeoutsSorted = false;

    protected Counter $epsCounter;

    /**
     * Counter length (in seconds) for recording events per second metrics.
     */
    const EPS_COUNTER_LENGTH = 3600;

    /** @var ?int Last event time based on wall clock time */
    protected ?int $lastEventReal = null;

    /** @var array<string, mixed>  */
    protected array $statistics = [];

    const MAX_TIME_VARIANCE = 600; // In seconds

    protected array $emitMapping = [
        'event' => Event::class,
        'action' => Action::class,
        'source' => ExecuteSource::class,
    ];

    /**
     * CorrelationEngine constructor.
     * @param class-string<IEventMatcher>[] $rules Class name of rules to load
     */
    public function __construct(array $rules)
    {
        foreach($rules as $matcher)
        {
            /** @var class-string<IEventMatcher> $eventname */
            foreach($matcher::initialAcceptedEvents() as $eventname)
            {
                $this->initialEventLookup[$eventname][] = $matcher;
            }
        }
        $this->epsCounter = new Counter(new DateInterval('PT' . static::EPS_COUNTER_LENGTH . 'S'), Counter::RESOLUTION_SECONDS);
    }

    /**
     * @param int $check
     * @param int $flag
     * @return bool
     */
    public function isFlagSet(int $check, int $flag) : bool
    {
        return ($check & $flag) == $flag;
    }

    /**
     * @param Event $event
     * @throws Exception
     * @TODO Setup queueing of incoming events in a time bucket, setup an out of order tolerance with the help of the bucket to re-ordering incoming events
     * @TODO Clock source for non-live timestreams should be largest time seen minus the out of order tolerance
     */
    public function handle(Event $event) : void
    {
        $handledMatchers = [];
        $skipMatchers = [];
        $timedOutMatchers = [];
        $suppress = false;

        /** Record that we have seen an event of this type */
        $this->incrStat('seen', (string)$event->event);


        if ($this->eventstream_live) {
            /**
             * If the event stream is live we want to make sure the event timestamp is within
             *  MAX_TIME_VARIANCE seconds of the current time, otherwise we will set it to the server time.
             */
            $now = new DateTimeImmutable();
            if (abs($now->getTimestamp() - $event->datetime->getTimestamp()) > (self::MAX_TIME_VARIANCE)) {
                echo "Correcting received time to {$now->format('c')}\n";
                $event->setReceivedTime($now);
            }
        } else {
            /** When we are parsing historical event stream data manually trigger any timeouts up till the current event (CheckTimeouts triggers event prior to the time passed in)
             * Any timeouts at the current time will be triggered after handling the current incoming event
             * @TODO This might not be the best place to call this where multiple stream interlace with different timestamps.
             * @TODO This should be moved to checking if a matcher would handle the event then to call it's timeout check first and other items.
             */
            $this->checkTimeouts($event->datetime->modify('-1 second'));
        }

        /**
         * Check existing state machines first to see if the event can be handled
         */
        $awaitingMatchers = array_merge(($this->waitingForNextEvent[$event->event] ?? []),
            ($this->waitingForNextEvent[IEventMatcher::EVENT_MATCH_ANY] ?? []));

        foreach ($awaitingMatchers as $matcher) {
            /* @var $matcher IEventMatcher */
            $expecting = $matcher->nextAcceptedEvents();
            $result = $matcher->handle($event);
            if ($this->isFlagSet($result, $matcher::EVENT_HANDLED)) {
                $handledMatchers[spl_object_id($matcher)] = $matcher;
                $skipMatchers[] = get_class($matcher);
                /** Update which events we are expecting next **/
                $this->removeWatchForEvents($matcher, $expecting);
                if (!$matcher->complete()) {
                    $this->addWatchForEvents($matcher, $matcher->nextAcceptedEvents());
                }
                /** Record that we handled an event of this type */
                $this->incrStat('handled', (string)$event->event . "|" . get_class($matcher));
            }

            // If we are timed out then flag the matcher for timeout processing.
            if ($this->isFlagSet($result, $matcher::EVENT_TIMEOUT)) {
                $timedOutMatchers[spl_object_id($matcher)] = $matcher;
            }

            //Matcher has told us to suppress further processing of this event.
            if ($this->isFlagSet($result, $matcher::EVENT_SUPPRESS)) {
                $suppress = true;
                /** Record that the event was suppressed */
                $this->incrStat('suppressed', (string)$event->event);
                break; //Jump out of the foreach loop as we are suppressing continued processing of this event.
            }
        }

        /**
         * Finally check if we need to start up any more state machines for this event.
         * A new state machine will not be created if an existing state machine suppressed the event
         * or if a state machine of the same class handled the event
         */
        if (false === $suppress) {
            $awaitingMatchers = array_diff(
                array_merge(($this->initialEventLookup[$event->event] ?? []),
                    ($this->initialEventLookup[IEventMatcher::EVENT_MATCH_ANY] ?? [])),
                $skipMatchers /** If this className has already handled this event, don't create another **/
            );

            foreach ($awaitingMatchers as $class) {
                $matcher = $this->constructMatcher($class);
                $result = $matcher->handle($event);
                if ($this->isFlagSet($result, $matcher::EVENT_HANDLED)) {
                    $handledMatchers[spl_object_id($matcher)] = $matcher;
                    $this->eventProcessors[spl_object_id($matcher)] = $matcher;
                    $this->incrStat('init_matcher', $class);
                    $this->incrStat('handled', (string)$event->event . "|" . get_class($matcher));

                    if (!$matcher->complete()) {
                        $this->addWatchForEvents($matcher, $matcher->nextAcceptedEvents());
                    }
                } else {
                    //Matcher did not want our event after all, discard it.
                    unset($matcher);
                    continue;
                }

                /** The current matcher has told us to suppress further processing of this event, break out of processing any further */
                if ($this->isFlagSet($result, $matcher::EVENT_SUPPRESS)) {
                    /** Record that the event was suppressed */
                    $this->incrStat('suppressed', (string)$event->event);
                    break;
                }
            }
        }

        if (empty($handledMatchers)) {
            $this->incrStat('unhandled', (string)$event->event);
        }

        $stateChanged = !(empty($handledMatchers) && empty($timedOutMatchers));

        /**  Fire any action and destroy any timed out matchers **/
        foreach ($timedOutMatchers as $objectId => $matcher) {
            $this->removeTimeout($matcher);
            $matcher->fire();
            /** Record stat of matcher timeout */
            $this->incrStat('completed_matcher_timeout', get_class($matcher));
            $this->removeMatcher($matcher);
            unset($handledMatchers[$objectId]);
            unset($matcher);
        }
        unset($timedOutMatchers);

        /** For any matchers that processed this event fire any actions, then update timeout or destroy if complete **/
        foreach ($handledMatchers as $matcher) {
            $matcher->fire();
            $this->addTimeout($matcher);

            if ($matcher->complete()) {
                /** Record stat of matcher completing */
                $this->incrStat('completed_matcher', get_class($matcher));
                $this->removeMatcher($matcher);
                unset($matcher);
            }
        }
        unset($handledMatchers);

        /** Flag as dirty except when we have a control message and state doesn't change **/
        if ($stateChanged || !in_array($event->event, Scheduler::CONTROL_MESSAGES, true)) {
            $this->dirty = true;
        }

        /** Increment event per second counters */
        $this->epsCounter->increment();
    }

    /**
     * Construct a new matcher EventProcessor and attach handlers for any events
     * @param string $className
     * @return IEventMatcher
     * @throws RuntimeException;
     */
    public function constructMatcher(string $className): IEventMatcher
    {
        if (is_a($className, IEventMatcher::class, true))
        {
            /** @var IEventMatcher $matcher */
            $matcher = new $className();
            $this->attachListeners($matcher);
            return $matcher;
        }
        else
        {
            throw new RuntimeException("$className does not implement " . IEventMatcher::class);
        }
    }

    /**
     * @param IEventMatcher $matcher
     */
    public function attachListeners(IEventMatcher $matcher) : void
    {
        if (is_a($matcher, IEventGenerator::class) ||
            is_a($matcher, IActionGenerator::class)
        ) {
            /** @var IEventGenerator|IActionGenerator $matcher */
            $matcher->on('data', [$this, 'handleEmit']);
        }
    }

    /**
     * @param object $data
     */
    public function handleEmit(object $data) : void
    {
        foreach($this->emitMapping as $key => $class) {
            if (is_a($data, $class))
            {
                if (is_a($data, Event::class)) {
                    $name = $data->event;
                } elseif (is_a($data, Action::class)){
                    $name = $data->getCmd();
                } else {
                    $name = $data::class;
                }
                $this->incrStat("emit_{$key}", $name);
                $this->emit($key, [$data]);
                return;
            }
        }

        throw new RuntimeException("Expected rules to emit an IEvent or Action. Unable to handle object of class " . get_class($data));
    }

    /**
     * Add customer objects that map be emitted and how they map to the Scheduler emit types
     */
    public function addCustomEmit(string $emit_name, string $className): void
    {
        if (!ctype_alpha($emit_name)) {
            throw new RuntimeException('Custom emit types must be alpha with no spaces');
        }

        if (!class_exists($className)) {
            throw new RuntimeException("Unknown class $className");
        }

        $this->emitMapping[$emit_name] = $className;
    }

    /**
     * Remove matcher from our state tables
     * @param IEventMatcher $matcher
     */
    protected function removeMatcher(IEventMatcher $matcher) : void {
        $this->clearWatchForEvents($matcher);
        unset($this->eventProcessors[spl_object_id($matcher)]);
    }

    /**
     * Keep note that the state machine $matcher is waiting for events $events
     * @param IEventMatcher $matcher
     * @param string[] $events
     */
    public function addWatchForEvents(IEventMatcher $matcher, array $events) : void
    {
        foreach($events as $eventName)
        {
            $this->waitingForNextEvent[$eventName][spl_object_id($matcher)] = $matcher;
        }
    }

    /**
     * Remove record that $matcher is waiting for certain events
     * @param IEventMatcher $matcher
     * @param string[] $events
     */
    public function removeWatchForEvents(IEventMatcher $matcher, array $events) : void
    {
        $matcherHash = spl_object_id($matcher);
        foreach($events as $eventName)
        {
            unset($this->waitingForNextEvent[$eventName][$matcherHash]);
            if (0 === count($this->waitingForNextEvent[$eventName]))
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
     * Remove record that $matcher is waiting for any events
     * @param IEventMatcher $matcher
     */
    public function clearWatchForEvents(IEventMatcher $matcher) : void {
        $matcherHash = spl_object_id($matcher);
        $events = [];
        foreach(array_keys($this->waitingForNextEvent) as $eventName) {
            if (array_key_exists($matcherHash, $this->waitingForNextEvent[$eventName])) {
                $events[] = $eventName;
            }
        }
        $this->removeWatchForEvents($matcher, $events);
    }

    /**
     * Add timeout will add or remove a timeout for the matcher passed in.
     * @param IEventMatcher $matcher
     */
    public function addTimeout(IEventMatcher $matcher) : void
    {
        $timeout = $matcher->getTimeout();
        if (null === $timeout)
        {
            $this->removeTimeout($matcher);
        }
        else
        {
            //@TODO check if we really need to add this in case where timeout is the same as current
            $this->timeouts[spl_object_id($matcher)] = ['timeout' => $timeout, 'matcher' => $matcher];
            $this->timeoutsSorted = false;
        }
    }

    /**
     * Remove any registered timeout for the matcher
     * @param IEventMatcher $matcher
     */
    public function removeTimeout(IEventMatcher $matcher) : void
    { 
        unset($this->timeouts[spl_object_id($matcher)]);
    }

    /**
     * Get the current timeouts for all running state machines. Sort the list prior to returning
     * @return array<string, mixed>
     */
    public function getTimeouts(): array
    {
        //Sort by timeout
        if (false === $this->timeoutsSorted) {
            uasort($this->timeouts, function ($a, $b) { return $a['timeout'] <=> $b['timeout']; });
            $this->timeoutsSorted = true;
        }
        return $this->timeouts;
    }

    /**
     * Set the Engine to start processing events in real-time against the clock vs historical data
     */
    public function setEventStreamLive() : void
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
        $this->checkTimeouts(new DateTimeImmutable());
    }

    /**
     * Return true if we are processing events in realtime or in historical batches
     * @return bool
     */
    public function isRealtime() : bool
    {
        return $this->eventstream_live;
    }

    /**
     * Check if any timeouts are prior to the $time passed and if so trigger the timeout logic for the respective matcher
     * @param DateTimeInterface $time
     * @return int Returns the number of alarms triggered by timeouts
     */
    public function checkTimeouts(DateTimeInterface $time): int
    {
        $triggered = 0;
        foreach ( $this->getTimeouts() as $timeout)
        {
            if ($time >= $timeout['timeout'])
            {
                /**
                 * @var IEventMatcher $matcher
                 */
                $matcher = $timeout['matcher'];
                $matcher->alarm();
                $matcher->fire();
                $triggered++;
                if ($matcher->isTimedOut())
                {
                    /** Remove all references if the matcher is timed out */
                    $this->removeTimeout($matcher);
                    /** Record stat of matcher timeout */
                    $this->incrStat('matcher_timeout', get_class($matcher));
                    $this->removeMatcher($matcher);
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
                break;
            }
        }
        return $triggered;
    }

    /**
     * Get the current representation of the engine's state as an array
     * @return array<string, mixed>
     */
    public function getState() : array
    {
        $state = [];
        $state['eventstream_live'] = $this->eventstream_live;
        $state['matchers'] = [];
        $state['events'] = [];
        $state['statistics'] = $this->statistics;
        $state['load'] = $this->calcLoad();
        /** @var IEventMatcher $matcher */
        foreach($this->eventProcessors as $matcher)
        {
            if ($matcher->complete()) {
                continue; //Don't save the matcher if it is complete
            }

            foreach($matcher->getEventChain() as $event)
            {
                $event_hash = spl_object_id($event);
                if (isset($state['events'][$event_hash]))
                {
                    continue;
                }
                $state['events'][$event_hash] = serialize($event);
            }

            $state['matchers'][] = serialize($matcher);
        }
        return $state;
    }

    /**
     * @param array $state
     */
    public function setState(array $state) : void
    {
        $this->statistics = $state['statistics'];
        $events = [];
        foreach($state['events'] as $hash => $eventData)
        {
            /**
             * First we unserialize all the events. We construct a table using the saved object hashes instead of the new ones
             * to allow the saved state machines to identify their events.
             */
            $event = @unserialize($eventData, ['allowed_classes' => [Event::class]]);
            if (!($event instanceof IEvent)) {
                fwrite(STDERR,'FATAL: Unserialisation of Event did not return an instance of IEvent' . PHP_EOL);
                exit();
            }
            $events[$hash] = $event;
        }

        //Set our handler if we unserialize the engine state and defined classed don't exist anymore (eg Generated Classes)
        ini_set('unserialize_callback_func', 'EdgeTelemetrics\EventCorrelation\handleMissingClass');
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
            $matcher = @unserialize($matcherState);
            if (!($matcher instanceof AEventProcessor)) {
                fwrite(STDERR,'FATAL: Unserialisation of Matcher did not return an instance of AEventProcessor' . PHP_EOL);
                exit(1);
            }
            if ($matcher instanceof UndefinedRule /** && error_on_undefined=false */) {
                // Skip this if the rule is undefined
                /** @TODO Log the matcherState to a file if we need to fixup and restore */
                continue;
            }
            $matcher->resolveEvents($events);
            $this->attachListeners($matcher);
            $this->eventProcessors[spl_object_id($matcher)] = $matcher;
            $this->addWatchForEvents($matcher, $matcher->nextAcceptedEvents());
        }
        ini_restore('unserialize_callback_func');

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
    public function clearDirtyFlag() : void
    {
        $this->dirty = false;
    }

    /**
     * @param string $name
     * @param string $group
     * @param int $incr
     */
    public function incrStat(string $group, string $name, int $incr = 1) : void
    {
        if (!isset($this->statistics[$group])) { $this->statistics[$group] = []; }
        if (!isset($this->statistics[$group][$name])) { $this->statistics[$group][$name] = 0; }
        $this->statistics[$group][$name] += $incr;
    }

    /**
     * Calculate the Load metrics for the engine
     * @return array{lastEvent: string, hour: float, fifteen: float, minute: float, counter: string}
     */
    public function calcLoad() : array
    {
        $counter = $this->epsCounter->getCounter();

        return [
            'lastEvent' => DateTimeImmutable::createFromFormat('U', (string)$this->epsCounter->getLastEventTime())->format('c'),
            'hour' => round(array_sum($counter) / 3600, 2),
            'fifteen' => round(array_sum(array_slice($counter, -900)) / 900, 2),
            'minute' => round(array_sum(array_slice($counter, -60)) / 60, 2),
            'counter' => implode(",", $counter),
        ];
    }
}
