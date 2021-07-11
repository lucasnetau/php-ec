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

/**
 * Class UndefinedRule
 * @package EdgeTelemetrics\EventCorrelation
 * An alias to this Rule will be automatically created by the CorrelationEngine if when loading saved state a Rule class definition is no longer found.
 */
class UndefinedRule extends Rule
{
    public function fire() {}
    public function complete(): bool
    {
        return true;
    }
}