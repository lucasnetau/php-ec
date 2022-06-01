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

use EdgeTelemetrics\EventCorrelation\Rule;

/**
 * Class UndefinedRule
 * @final
 * @package EdgeTelemetrics\EventCorrelation
 * An alias to this Rule will be automatically created by the CorrelationEngine if when loading saved state a Rule class definition is no longer found.
 */
final class UndefinedRule extends Rule
{
    public function fire() : void {}
    public function complete(): bool
    {
        return true;
    }
}