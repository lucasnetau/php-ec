<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Management;

use EdgeTelemetrics\EventCorrelation\Management\Actions\Index;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use FastRoute\DataGenerator\GroupCountBased;
use FastRoute\RouteCollector;
use FastRoute\RouteParser\Std;
use Psr\Http\Message\ServerRequestInterface;
use React\Http\HttpServer;
use React\Socket\SocketServer;
use function str_replace;

final class Server {

    /**
     * @var HttpServer
     */
    protected HttpServer $httpServer;

    /**
     * @var SocketServer
     */
    protected SocketServer $socketServer;

    /**
     * @var Scheduler
     */
    protected Scheduler $scheduler;

    /**
     * @param Scheduler $scheduler
     */
    public function __construct(Scheduler $scheduler) {
        $this->scheduler = $scheduler;

        $routes = new RouteCollector(new Std(), new GroupCountBased());
        $routes->get('/', Index::class);

        $this->httpServer = new HttpServer(
            function (ServerRequestInterface $request, callable $next) {
                $request = $request->withAttribute('scheduler', $this->scheduler);
                return $next($request);
            },
            new Router($routes)
        );

        $this->socketServer = new SocketServer('127.0.0.1:0');
        $this->httpServer->listen($this->socketServer);
    }

    public function getListeningAddress() {
        return str_replace('tcp://', 'http://', $this->socketServer->getAddress());
    }
}
