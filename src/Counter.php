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
use DateTimeImmutable;

class Counter {

    const RESOLUTION_MILLISECONDS = 1;
    const RESOLUTION_SECONDS = 2;

    /**
     * @var int
     */
    protected int $counter_length;

    /**
     * @var int
     */
    protected int $resolution;

    /**
     * @var array
     */
    protected array $counter;

    /**
     * @var int
     */
    protected int $lastEventTime = 0;

    /**
     * @param DateInterval $interval
     * @param int $resolution
     */
    public function __construct(DateInterval $interval, int $resolution) {
        $counter_length = (new DateTimeImmutable('@0'))->add($interval)->getTimestamp();
        if (self::RESOLUTION_MILLISECONDS === $resolution) {
            $counter_length *= 1000;
        }
        $this->counter_length = $counter_length;
        $this->resolution = $resolution;
        $this->initCounter();
    }

    /**
     * Increment the counter for the current time
     */
    public function increment()
    {
        $time = $this->getTime();

        $this->flushOldSlots($time);

        $index = $this->getIndexForTime($time);
        $this->counter[$index]++;

        $this->lastEventTime = $time;
    }

    /**
     * Return the current counters as an array shifted to ensure
     * @return array
     */
    public function getCounter() : array {
        $time = $this->getTime();
        $index = $this->getIndexForTime($time);
        $this->flushOldSlots($time);
        /** Shift the counter so that the current time modulus is the last item in the array */
        return array_merge(array_slice($this->counter, $index+1), array_slice($this->counter, 0, $index+1));
    }

    /**
     * @return int
     */
    public function getLastEventTime() : int {
        return $this->lastEventTime;
    }

    /**
     * Initialise the counter data structure
     */
    protected function initCounter() {
        $this->counter = array_fill(0,$this->counter_length, 0);
    }

    /**
     * Get the current time as an integer relative to the required resolution
     * @return int
     */
    protected function getTime() : int
    {
        if (self::RESOLUTION_MILLISECONDS === $this->resolution) {
            return (int)round(microtime(true) * 1000);
        } else {
            return time();
        }
    }

    /**
     * @param int $time
     * @return int
     */
    protected function getIndexForTime(int $time) : int {
        return $time % $this->counter_length;
    }

    /**
     * Set any slots between the last event time and current time to 0
     * @param int $time
     */
    protected function flushOldSlots(int $time) {
        /** Don't flush if we are tracking the current time */
        if ($time === $this->lastEventTime)
        {
            return;
        }

        /** Check if we have not processed an event for the max measurement period and reset all counters */
        if (($time - $this->lastEventTime) >= $this->counter_length) {
            $this->initCounter();
        } else {
            /** We are a new time period */
            $lastIndex = $this->getIndexForTime($this->lastEventTime);
            $currentIndex = $this->getIndexForTime($time);
            if ($currentIndex >= $lastIndex)
            {
                for($i = $lastIndex+1; $i <= $currentIndex; $i++)
                {
                    $this->counter[$i] = 0;
                }
            }
            else
            {
                for($i = 0; $i <= $currentIndex; $i++)
                {
                    $this->counter[$i] = 0;
                }
                for($i = $lastIndex+1; $i < $this->counter_length; $i++)
                {
                    $this->counter[$i] = 0;
                }
            }
        }
    }
}