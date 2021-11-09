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

use DateTimeImmutable;
use DateTimeZone;
use Exception;
use function ucfirst;
use function method_exists;
use function property_exists;
use function get_object_vars;
use function json_encode;
use function json_decode;

/**
 * Class Event
 * @package EdgeTelemetrics\EventCorrelation
 * @property mixed $id
 * @property string $event
 * @property ?DateTimeImmutable $datetime
 * @property ?DateTimeImmutable $receivedTime
 */
class Event implements IEvent {
    /**
     * @var mixed
     */
    protected $id;

    /**
     * @var string
     */
    protected string $event;

    /**
     * @var ?DateTimeImmutable
     */
    protected ?DateTimeImmutable $datetime = null;

    /**
     * @var ?DateTimeImmutable
     */
    protected ?DateTimeImmutable $receivedTime = null;

    /**
     * Event constructor.
     * @param array $kvp Key Value pairs
     * @throws Exception
     */
    public function __construct(array $kvp)
    {
        //$this->datetime = DateTime::createFromFormat('U', $kvp['datetime'], new DateTimeZone('UTC'));
        if (isset($kvp['datetime']))
        {
            //Parse the date string into a DateTimeImmutable (supports timezone encoded in string), then normalise the datetime to UTC timezone
            $timezone = new DateTimeZone('UTC');
            $this->datetime = (new DateTimeImmutable($kvp['datetime'], $timezone))->setTimezone($timezone);
            unset($kvp['datetime']);
        }
        else
        {
            /** If the event data doesn't have a datetime then set it to the server time when received **/
            $this->datetime = new DateTimeImmutable();
        }
        foreach ($kvp as $key => $value) {
            $this->$key = $value;
        }
    }

    /**
     * @param string $event_name
     * @return bool
     */
    public function is_event(string $event_name) : bool {
        return $this->event === $event_name;
    }

    /**
     * Will return class variable, will first look for a function get<Name> and then check class properties
     * @param string $name
     * @return mixed|null
     */
    public function __get(string $name)
    {
        $methodName = "get" . ucfirst($name);
        if (method_exists($this, $methodName))
        {
            return $this->$methodName();
        }
        else if (property_exists($this, $name))
        {
            return $this->$name;
        }
        return null;
    }

    public function getId()
    {
        return $this->id;
    }

    public function getEventName(): string
    {
        return $this->event;
    }

    /**
     * @param DateTimeImmutable $time
     */
    public function setReceivedTime(DateTimeImmutable $time) : void
    {
        $this->receivedTime = $time;
    }

    /**
     * Get the event datetime. If the receivedTime property is set this will be used
     * as the server time when received was too different from the event timestamp
     * @return ?DateTimeImmutable
     */
    public function getDatetime() : ?DateTimeImmutable
    {
        return ($this->receivedTime ?? $this->datetime);
    }

    /**
     * Convert this Event into a plain PHP Array
     * @return array
     */
    public function toArray() : array {
        $object = get_object_vars($this);
        $object['datetime'] = $this->datetime->format(IEvent::DATETIME_SERIALISATION_FORMAT);
        if (null !== $this->receivedTime) {
            $object['receivedTime'] = $this->receivedTime->format(IEvent::DATETIME_SERIALISATION_FORMAT);
        }
        else
        {
            unset($object['receivedTime']);
        }
        return $object;
    }

    /**
     * @return array
     */
    public function jsonSerialize() : array
    {
        return $this->toArray();
    }

    /**
     * @return false|string
     * @deprecated
     */
    public function serialize()
    {
        return json_encode($this, JSON_PRESERVE_ZERO_FRACTION|JSON_UNESCAPED_SLASHES);
    }

    /**
     * @return array
     */
    public function __serialize(): array
    {
        return $this->toArray();
    }

    /**
     * @param string $data
     * @throws Exception
     * @deprecated Left to support loading older state files
     */
    public function unserialize($data)
    {
        $data = json_decode($data, true);
        $this->__unserialize($data);
    }

    /**
     * @param array $data
     * @throws Exception
     */
    public function __unserialize(array $data) {
        $this->datetime = new DateTimeImmutable($data['datetime']);
        unset($data['datetime']);
        if (isset($data['receivedTime'])) {
            $this->receivedTime = new DateTimeImmutable($data['receivedTime']);
            unset($data['receivedTime']);
        }
        foreach($data as $key => $value)
        {
            $this->$key = $value;
        }
    }
}
