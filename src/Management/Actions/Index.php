<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Management\Actions;

use DateTimeImmutable;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use Psr\Http\Message\ServerRequestInterface;
use React\Http\Message\Response;
use function array_keys;
use function array_map;
use function array_reverse;
use function array_values;
use function count;
use function implode;
use function sprintf;

class Index {

    public function __invoke(ServerRequestInterface $request): Response
    {
        global $argv;
        /** @var Scheduler $scheduler */
        $scheduler = $request->getAttribute('scheduler');

        $state = $scheduler->buildState();

        $running_mode = $state['engine']['eventstream_live'] ? 'Live Mode' : 'Batch Mode';
        $start_time = new DateTimeImmutable('@' . $_SERVER['REQUEST_TIME']);

        $input_process_list = '<li>' . implode('</li><li>', $state['scheduler']['input']['running']) . '</li>';

        $opcache_status = opcache_get_status(false);

        if ($opcache_status === false) {
            $opcache_html = '<p style="color: red">Disabled</p>';
        } else {
            $opcache_html = <<<EOH
                <dl>
                    <dt>Opcache Enabled</dt>
                    <dd>{$opcache_status['opcache_enabled']}</dd>
                    <dt>Cache Full</dt>
                    <dd>{$this->fn($opcache_status['cache_full'] ? 'Yes' : 'No')}</dd>
                    <dt>Restart Pending</dt>
                    <dd>{$this->fn($opcache_status['restart_pending'] ? 'Yes' : 'No')}</dd>
                    <dt>Restart in Progress</dt>
                    <dd>{$this->fn($opcache_status['restart_in_progress'] ? 'Yes' : 'No')}</dd>
                    <dt>Wasted Memory Percentage</dt>
                    <dd>{$this->fn(round($opcache_status['memory_usage']['current_wasted_percentage'], 2) . '%')}</dd>
                    <dt>Free Memory</dt>
                    <dd>{$this->formatMemory($opcache_status['memory_usage']['free_memory'])}</dd>
                    <dt>Used Memory</dt>
                    <dd>{$this->formatMemory($opcache_status['memory_usage']['used_memory'])}</dd>
                    <dt>Wasted Memory</dt>
                    <dd>{$this->formatMemory($opcache_status['memory_usage']['wasted_memory'])}</dd>
                    <dt>Interned Strings Usage</dt>
                    <dd>{$this->fn(implode('</dd><dd>', array_map(function($v1, $v2) {return sprintf('%s=%s', $v1, $v2); }, array_keys($opcache_status['interned_strings_usage']), array_values($opcache_status['interned_strings_usage']))))}</dd>
                    <dt>Opcache Statistics</dt>
                    <dd>{$this->fn(implode('</dd><dd>', array_map(function($v1, $v2) {return sprintf('%s=%s', $v1, $v2); }, array_keys($opcache_status['opcache_statistics']), array_values($opcache_status['opcache_statistics']))))}</dd>
                    <dt>JIT Statistics</dt>
                    <dd>{$this->fn(implode('</dd><dd>', array_map(function($v1, $v2) {return sprintf('%s=%s', $v1, $v2); }, array_keys($opcache_status['jit']), array_values($opcache_status['jit']))))}</dd>
                </dl>
EOH;

        }

        $body = <<<EOT
<!DOCTYPE HTML>
<html lang="en">
    <head>
        <title>Event Engine Management Console</title>
    </head>
    <body>
        <h1>PHP Event Engine</h1>
        <dl>
          <dt>State</dt>
          <dd>{$this->fn(ucfirst($state['scheduler']['state']))}</dd>
          <dt>Command</dt>
          <dd>{$this->fn(\cli_get_process_title() ?: implode(" ", $argv))}</dd>
          <dt>Running Mode</dt>
          <dd>$running_mode</dd>
          <dt>Start Time</dt>
          <dd>{$start_time->format('c')}</dd>
          <dt>Uptime</dt>
          <dd>{$this->time_ago($state['scheduler']['uptime_msec'])}</dd>
          <dt>Last Processed Event</dt>
          <dd>{$state['engine']['load']['lastEvent']} ({$this->fn(time() - strtotime($state['engine']['load']['lastEvent']))} seconds ago)</dd>
          <dt>Events Per Second</dt>
          <dd>Last Minute {$state['engine']['load']['minute']} eps</dd>
          <dd>Last 15 Minutes {$state['engine']['load']['fifteen']} eps</dd>
          <dd>Last Hour {$state['engine']['load']['hour']} eps</dd>
          <dt>Memory Usage</dt>
          <dd>{$this->formatMemory(memory_get_usage())} ({$state['scheduler']['memoryPercentageUsed']}%)</dd>
          <dt>Save State Size</dt>
          <dd>{$state['scheduler']['saveFileSizeBytes']} bytes</dd>
          <dt>Time taken to write last save state to disk</dt>
          <dd>{$state['scheduler']['saveStateLastDuration']} milliseconds</dd>
        </dl>
        <hr>
        <h2>Input Processes</h2>
        <h3>Running</h3>
        <ul>$input_process_list</ul>
        <hr>
        <h2>Action Processes</h2>
        <dl>
            <dt>Inflight</dt>
            <dd>{$this->fn(count($state['scheduler']['actions']['inflight']))}</dd>
            <dt>Errored since startup</dt>
            <dd>{$this->fn(count($state['scheduler']['actions']['errored']))}</dd>
        </dl>
        <hr>
        <h2>OpCache Status</h2>
        {$opcache_html}
    </body>
</html>
EOT;

        return new Response(200, ['Content-Type' => 'text/html'], $body);
    }

    /**
     * Helper for the template above
     */
    protected function fn($data) : string {
        return (string)$data;
    }

    /**
     * Calculate a human readable time ago string
     */
    protected function time_ago(int $time_msec) : string {
        $num = intdiv($time_msec,1000);
        $calc = [];
        $calc['secs'] = fmod($num, 60);
        $num = intdiv($num, 60);
        $calc['mins']  = $num % 60;
        $num = intdiv($num, 60);
        $calc['hours'] = $num % 24;
        $num = intdiv($num, 24);
        $calc['days']  = $num;

        $calc = array_filter($calc, function($val) { return ($val !== 0); });
        $time_strings = [];
        foreach(array_reverse($calc) as $segment => $value) {
            $time_strings[] = match($segment) {
                'secs' => $value . ' seconds',
                'mins' => $value . ' minutes',
                'hours' => $value . ' hours',
                'days' => $value . ' days',
            };
        }
        return implode(", ", $time_strings);
    }

    protected function formatMemory($size, $precision = 2): string
    {
        if ($size === 0) {
            return '0B';
        }
        $units = ['B', 'KB', 'MB', 'GB'];
        $base = log($size) / log(1024);
        return round(pow(1024, $base - floor($base)), $precision) . $units[(int)floor($base)];
    }



}