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

use Closure;
use DateTimeImmutable;
use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Event;

/**
 * Class OnSchedule
 * @package EdgeTelemetrics\EventCorrelation\Rule
 */

class OnSchedule extends Cron {
    private Closure $callback;

    public function __construct(Closure $do, string|null $schedule = null, string|\DateTimeZone|null $timezone = null) {
        parent::__construct($schedule, $timezone);
        $this->callback = $do;
    }

    public function onSchedule(DateTimeImmutable $scheduledTime): void {
        $result = ($this->callback)($scheduledTime);
        if ($result instanceof Action || $result instanceof Event) {
            $this->emit('data', [$result]);
        }
    }
}

