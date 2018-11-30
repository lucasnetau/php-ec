<?php declare(strict_types=1);

namespace Lucas\EventCorrelation;

class Event implements IEvent {
    protected $event;

    protected $datetime;

    protected $received_time;

    /**
     * Event constructor.
     * @param array $kvp Key Value pairs
     * @throws \Exception
     */
    public function __construct(array $kvp)
    {
        //$this->datetime = DateTime::createFromFormat('U', $kvp['datetime'], new DateTimeZone('UTC'));
        if (isset($kvp['datetime'])) {
            $this->datetime = new \DateTimeImmutable($kvp['datetime'], new \DateTimeZone('UTC'));
            unset($kvp['datetime']);
        }
        else
        {
            /** If the event data doesn't have a datetime then set it to the server time when received **/
            $this->datetime = new \DateTimeImmutable();
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
     * @param \DateTimeInterface $time
     */
    public function setReceivedTime(\DateTimeInterface $time)
    {
        $this->received_time = $time;
    }

    /**
     * Get the event datetime. If the received_time property is set this will be used
     * as the server time when received was too different from the event timestamp
     * @return \DateTimeImmutable
     */
    public function getDatetime()
    {
        if (null === $this->received_time)
        {
            return $this->datetime;
        }
        else {
            return $this->received_time;
        }
    }

    /**
     * @return array|mixed
     */
    public function jsonSerialize()
    {
        $object = get_object_vars($this);
        $object['datetime'] = $this->datetime->format('c');
        if (null !== $this->received_time) {
            $object['received_time'] = $this->datetime->format('c');
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
     * @throws \Exception
     */
    public function unserialize($data)
    {
        $data = json_decode($data, true);
        $this->datetime = new \DateTimeImmutable($data['datetime']);
        if (isset($data['received_time'])) {
            $this->received_time = new \DateTimeImmutable($data['received_time']);
            unset($data['received_time']);
        }
        unset($data['datetime']);
        foreach($data as $key => $value)
        {
            $this->$key = $value;
        }
    }
}
