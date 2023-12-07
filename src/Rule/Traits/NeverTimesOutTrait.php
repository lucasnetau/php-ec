<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Rule\Traits;

trait NeverTimesOutTrait {

    public function isTimedOut() : bool {
        return false;
    }

    public function updateTimeout() : void
    {
        $this->timeout = null;
    }

    public function onTimeout() : void {
        throw new \RuntimeException('Timeout reached but rule is marked as never timing out.');
    }
}