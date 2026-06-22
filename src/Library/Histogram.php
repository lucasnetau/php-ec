<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library;

/**
 * @internal
 */
class Histogram {

    /** @var array<int, int> */
    private array $counts;

    /** @var array<int, int|float> */
    private array $buckets;

    /**
     * Bucket upper-bounds are inclusive (except for the case where the upper-bound is +Inf) while bucket lower-bounds are exclusive.
     */
    public function __construct(int|float ...$buckets)
    {
        $this->buckets = $buckets;
        sort($this->buckets);
        $this->counts = array_fill(0, count($buckets) + 1, 0);
    }

    public function add(int|float $value): void {
        foreach ($this->buckets as $i => $bound) {
            if ($value <= $bound) {
                $this->counts[$i]++;
                return;
            }
        }
        $this->counts[count($this->buckets)]++;
    }

    public function getHistogram(): array {
        return array_values(array_filter(array_map(
            fn($b, $c) => ['upper_bound' => $b === INF ? '+Inf' : $b, 'count' => $c],
            [...$this->buckets, INF],
            $this->counts
        ), function($entry) { return $entry['count'] > 0; }));
    }

}