<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation\Rule;

use Cron\CronExpression;
use DateTimeImmutable;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use Exception;
use function count;
use function in_array;

/**
 * Class Cron
 * @package EdgeTelemetrics\EventCorrelation\Rule
 */
abstract class Cron extends Rule
{
    /**
     * @var string
     */
    const CRON_SCHEDULE = '';

    const ON_INITIALISATION = '@reboot';
    const ON_SHUTDOWN = '@shutdown';

    /** @var string The timezone that this rule runs in */
    const TIMEZONE = 'UTC';

    /**
     * @var Event Holds the initialising event for the rule (Engine Start or Restored)
     */
    protected Event $initEvent;

    /**
     * @var CronExpression
     */
    private CronExpression $cron;

    public function __construct()
    {
        parent::__construct();
        if (!in_array(static::CRON_SCHEDULE, [self::ON_INITIALISATION, self::ON_SHUTDOWN], true)) {
            $this->cron = new CronExpression(static::CRON_SCHEDULE);
        }
    }

    /**
     * Process an incoming event
     * @param Event $event
     * @return int
     * @throws Exception
     */
    public function handle(Event $event) : int
    {
        $handled = parent::handle($event);
        if (($handled & self::EVENT_HANDLED) === self::EVENT_HANDLED) {
            if (count($this->consumedEvents) === 1 && in_array($event->event, $this->initialAcceptedEvents(), true)) {
                //Don't track the initialising event directly in the consumed events to allow processing of the static::EVENTS array correctly
                $this->initEvent = $event;
                $this->consumedEvents = [];
            }
            if ($event->event === Scheduler::CONTROL_MSG_STOP) {
                $this->isTimedOut = true;
            }
        }
        return $handled;
    }

    /**
     * @return string[]
     */
    public static function initialAcceptedEvents() : array {
        return [Scheduler::CONTROL_MSG_NEW_STATE, Scheduler::CONTROL_MSG_RESTORED_STATE];
    }

    /**
     * @return string[]
     */
    public function nextAcceptedEvents() : array
    {
        $nextEvents = parent::nextAcceptedEvents();
        if ((static::CRON_SCHEDULE === self::ON_SHUTDOWN || (count($this->consumedEvents) === 0 && empty($nextEvents))) && !in_array(Scheduler::CONTROL_MSG_STOP, $nextEvents, true)) {
            $nextEvents[] = Scheduler::CONTROL_MSG_STOP;
        }
        return $nextEvents;
    }

    /**
     * @param Event $event
     * @return bool
     */
    protected function acceptEventType(Event $event) : bool {
        if (in_array($event->event, static::initialAcceptedEvents(), true)) {
            return true;
        }
        if (static::CRON_SCHEDULE === self::ON_SHUTDOWN && $event->is_event(Scheduler::CONTROL_MSG_STOP)) {
            return true;
        }
        return parent::acceptEventType($event);
    }

    /**
     * @param Event $event
     * @return bool
     */
    public function acceptEvent(Event $event) : bool
    {
        return true;
    }

    /**
     * @return bool
     */
    public function complete(): bool
    {
        /** @psalm-suppress RedundantPropertyInitializationCheck */
        if (!isset($this->initEvent)) {
            return false;
        }
        $lastEvent = $this->getLastEvent() ?? $this->initEvent;

        if (static::CRON_SCHEDULE === self::ON_INITIALISATION || (static::CRON_SCHEDULE === self::ON_SHUTDOWN && $lastEvent->is_event(Scheduler::CONTROL_MSG_STOP))) {
            return true;
        }
        return false;
    }

    /**
     * @return void
     */
    public function fire(): void
    {
        if ($this->complete()) {
            $this->onSchedule();
        } elseif ($this->isTimedOut()) {
            $this->onTimeout();
            $this->onDone();
        } else {
            $this->onProgress();
        }
    }

    /**
     * @return void
     * @throws Exception
     */
    public function updateTimeout() : void
    {
        /** @psalm-suppress TypeDoesNotContainType */
        if (!isset($this->cron) || $this->complete()) {
            $this->timeout = null;
        } else {
            if (static::$eventstream_live) {
                $currentTime = 'now';
            } else {
                /** @var DateTimeImmutable $currentTime */
                $currentTime = ($this->getLastEvent() ?? $this->initEvent)->datetime;
            }
            $this->timeout = DateTimeImmutable::createFromMutable($this->cron->getNextRunDate($currentTime, 0, false, static::TIMEZONE));
        }
    }

    //Called when the timeout is reached
    public function alarm() : void {
        $this->onSchedule();
        if (!$this->complete() && !$this->isTimedOut()) {
            $this->updateTimeout();
        }
    }

    abstract public function onSchedule() : void;
}