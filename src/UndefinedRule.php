<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

abstract class UndefinedRule extends Rule
{
    public function fire() {}
    public function complete(): bool
    {
        return true;
    }
}