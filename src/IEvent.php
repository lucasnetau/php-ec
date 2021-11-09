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
use DateTimeInterface;
use JsonSerializable;
use Serializable;

interface IEvent extends JsonSerializable, Serializable {

    /**
     * Format for serialising the event to JSON
     */
    public const DATETIME_SERIALISATION_FORMAT = DateTimeInterface::RFC3339_EXTENDED;

    /**
     * Get the event's unique id
     * @return null|mixed
     */
    public function getId();

    /**
     * Get the name of the event
     * @return string
     */
    public function getEventName() : string;

    /**
     * @param DateTimeImmutable $time
     */
    public function setReceivedTime(DateTimeImmutable $time) : void;

    /**
     * @return DateTimeImmutable|null
     */
    public function getDatetime() : ?DateTimeImmutable;

}