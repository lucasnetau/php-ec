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
use JsonSerializable;
use Serializable;

interface IEvent extends JsonSerializable, Serializable {

    public function setReceivedTime(DateTimeImmutable $time);
    public function getDatetime() : ?DateTimeImmutable;

}