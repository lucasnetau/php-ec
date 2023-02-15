<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Management\Actions;

use DateTimeImmutable;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use Psr\Http\Message\ServerRequestInterface;
use React\Http\Message\Response;
use function array_reverse;
use function count;

class Index {

    public function __invoke(ServerRequestInterface $request): Response
    {
        /** @var Scheduler $scheduler */
        $scheduler = $request->getAttribute('scheduler');

        $state = $scheduler->buildState();

        $running_mode = $state['engine']['eventstream_live'] ? 'Live Mode' : 'Batch Mode';
        $start_time = new DateTimeImmutable('@' . $_SERVER['REQUEST_TIME']);

        $input_process_list = '<li>' . implode('</li><li>', $state['scheduler']['input']['running']) . '</li>';

        $body = <<<EOT
<!DOCTYPE HTML>
<html lang="en">
    <head>
        <title>Event Engine Management Console</title>
    </head>
    <body>
        <h1>PHP Event Engine</h1>
        <dl>
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
          <dd>{$this->fn(memory_get_usage())} bytes ({$state['scheduler']['memoryPercentageUsed']}%)</dd>
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
        $time_str = '';
        foreach(array_reverse($calc) as $segment => $value) {
            switch($segment) {
                case 'secs':
                    $time_str .= $value . ' seconds';
                    break;
                case 'mins':
                    $time_str .= $value . ' minutes';
                    break;
                case 'hours':
                    $time_str .= $value . ' hours';
                    break;
                case 'days':
                    $time_str .= $value . ' days';
                    break;
            }
            $time_str .= ',';
        }
        return rtrim($time_str, ',');
    }

}