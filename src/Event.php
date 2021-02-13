<?php declare(strict_types=1);

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

class Event implements IEvent {
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
            $this->datetime = new DateTimeImmutable($kvp['datetime'], new DateTimeZone('UTC'));
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

    /**
     * @param DateTimeImmutable $time
     */
    public function setReceivedTime(DateTimeImmutable $time)
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
        if (null === $this->receivedTime)
        {
            return $this->datetime;
        }
        else {
            return $this->receivedTime;
        }
    }

    /**
     * @return array|mixed
     */
    public function jsonSerialize()
    {
        $object = get_object_vars($this);
        $object['datetime'] = $this->datetime->format('c');
        if (null !== $this->receivedTime) {
            $object['receivedTime'] = $this->receivedTime->format('c');
        }
        else
        {
            unset($object['receivedTime']);
        }
        return $object;
    }

    /**
     * @return false|string
     */
    public function serialize()
    {
        return json_encode($this);
    }

    /**
     * @param string $data
     * @throws Exception
     */
    public function unserialize($data)
    {
        $data = json_decode($data, true);
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
