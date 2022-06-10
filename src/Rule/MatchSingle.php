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
use function count;

/**
 * Class MatchSingle
 * @package EdgeTelemetrics\EventCorrelation\Rule
 */
abstract class MatchSingle extends Rule
{
    public function __construct()
    {
        parent::__construct();
        if (count(static::EVENTS) !== 1) {
            throw new \RuntimeException('A rule of MatchSingle must have only one event group defined, ' . count(static::EVENTS) . ' are defined');
        }
    }
}