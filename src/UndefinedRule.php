<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

class UndefinedRule extends Rule
{
    public function fire() {}
    public function complete(): bool
    {
        return true;
    }
}