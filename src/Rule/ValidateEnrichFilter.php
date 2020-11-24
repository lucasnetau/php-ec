<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Rule;

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Rule;
use Exception;

abstract class ValidateEnrichFilter extends Rule
{
    protected bool $passed_checks = true;

    /**
     * @param Event $event
     * @return int
     * @throws Exception
     *
     * Validate, Enrich, and Filter the event. Suppress the event if it fails validation
     */
    public function handle(Event $event): int
    {
        $result = parent::handle($event);
        if (($result & self::EVENT_HANDLED) === self::EVENT_HANDLED) {
            $event = json_decode(json_encode($event), true);
            $event = $this->filter($this->enrich($this->validate($event)));

            if (null === $event) {
                $this->passed_checks = false;
                return self::EVENT_HANDLED|self::EVENT_SUPPRESS;
            }
        }
        return $result;
    }

    /**
     * @param array $event
     * @return array|null
     * Check and validate the incoming event for clear issues (eg missing data / fields)
     */
    abstract protected function validate(array $event) : ?array;

    /**
     * @param array|null $event
     * @return array|null
     * Enrich the event with extra information, typically pre-loaded from configuration / lookup table
     */
    abstract protected function enrich(?array $event) : ?array;

    /**
     * @param array|null $event
     * @return array|null
     * Filter out any events that don't meet out spec
     */
    abstract protected function filter(?array $event) : ?array;

    public function fire()
    {
        if ($this->complete() && !$this->actionFired)
        {
            if ($this->passed_checks) {
                $this->firePassed();
            } else {
                $this->fireFailed();
            }
            $this->actionFired = true;
        }
    }

    abstract protected function firePassed();

    abstract protected function fireFailed();


}