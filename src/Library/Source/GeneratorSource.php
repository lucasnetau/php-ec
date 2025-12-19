<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library\Source;

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Scheduler\SourceFunction;
use Generator;
use React\EventLoop\TimerInterface;
use RuntimeException;

class GeneratorSource extends SourceFunction {
    protected TimerInterface $timer;

    public function __construct(private readonly Generator $generator, private readonly float|int $delay = 0) {
        parent::__construct();
        if ($this->delay < 0) {
            throw new RuntimeException('Delay must be greater than 0');
        }
    }

    public function functionStart(mixed $checkpoint): void {
        $this->loop->futureTick($this->tick(...));
    }

    private function tick(): void {
        if ($this->generator->valid()) {
            $event = $this->generator->current();
            if ($event instanceof Event) {
                $this->emitEvent($event);
            }

            $this->timer = $this->loop->addTimer($this->delay, function() {
                try {
                    $this->generator->next();
                    $this->tick();
                } catch (\Throwable $e) {
                    $this->logger?->critical('Generator threw exception', ['exception' => $e]);
                }
            });
        } else {
            $this->exit();
        }
    }

    public function exit(int $code = 0): void {
        $this->running = false;
        if (isset($this->timer)) {
            $this->loop->cancelTimer($this->timer);
            unset($this->timer);
        }
        $this->emit('exit', [$code]);
    }

    public function functionStop(): void {
        $this->exit();
    }
}
