<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Management\Server;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ResponseInterface;
use React\EventLoop\Loop;
use React\Http\Browser;
use Throwable;
use VStelmakh\PsrTestLogger\TestLogger;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Management\Server
 * @covers \EdgeTelemetrics\EventCorrelation\Management\Router
 * @covers \EdgeTelemetrics\EventCorrelation\Management\Actions\Index
 * @covers \EdgeTelemetrics\EventCorrelation\Management\Actions\Dashboard
 * @covers \EdgeTelemetrics\EventCorrelation\Management\Actions\Graph
 */
class ManagementServerTest extends TestCase {

    public function testManagementRoutesRespond() : void {
        putenv('HOST=127.0.0.1');
        putenv('PORT=0');

        try {
            $scheduler = new ObservableScheduler([]);
            $scheduler->setLogger(new TestLogger());
            $server = new Server($scheduler);
            $url = $server->getListeningAddress();
            $this->assertIsString($url, 'management server must report its listening address');

            $paths = ['/', '/dashboard', '/api/graph'];
            $responses = [];
            $maybeStop = function () use (&$responses, $paths) {
                if (count($responses) === count($paths)) {
                    Loop::stop();
                }
            };

            $browser = new Browser();
            foreach ($paths as $path) {
                $browser->get($url . $path)->then(
                    function (ResponseInterface $response) use (&$responses, $maybeStop, $path) {
                        $responses[$path] = $response;
                        $maybeStop();
                    },
                    function (Throwable $error) use (&$responses, $maybeStop, $path) {
                        $responses[$path] = $error;
                        $maybeStop();
                    }
                );
            }

            Loop::addTimer(5, function () {
                Loop::stop();
            });
            Loop::run();

            foreach ($paths as $path) {
                $this->assertArrayHasKey($path, $responses, "$path did not respond");
                $this->assertInstanceOf(
                    ResponseInterface::class,
                    $responses[$path],
                    "$path failed: " . ($responses[$path] instanceof Throwable ? $responses[$path]->getMessage() : '')
                );
                $this->assertSame(
                    200,
                    $responses[$path]->getStatusCode(),
                    "$path returned {$responses[$path]->getStatusCode()}: " . (string)$responses[$path]->getBody()
                );
            }
        } finally {
            putenv('HOST');
            putenv('PORT');
        }
    }
}
