<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Management;

use FastRoute\Dispatcher;
use FastRoute\Dispatcher\GroupCountBased;
use FastRoute\RouteCollector;
use LogicException;
use Psr\Http\Message\ServerRequestInterface;
use React\Http\Message\Response;
use function class_exists;
use function is_callable;
use function is_string;

final class Router
{
    private GroupCountBased $dispatcher;

    public function __construct(RouteCollector $routes)
    {
        $this->dispatcher = new GroupCountBased($routes->getData());
    }

    public function __invoke(ServerRequestInterface $request)
    {
        $routeInfo = $this->dispatcher->dispatch($request->getMethod(), $request->getUri()->getPath());

        switch ($routeInfo[0]) {
            case Dispatcher::NOT_FOUND:
                return new Response(404, ['Content-Type' => 'text/plain'], 'Not found');
            case Dispatcher::METHOD_NOT_ALLOWED:
                return new Response(405, ['Content-Type' => 'text/plain'], 'Method not allowed');
            case Dispatcher::FOUND:
                $params = $routeInfo[2];
                $handler = $routeInfo[1];
                if ( !is_callable($handler) && is_string( $handler ) )
                {
                    //Invokable Method
                    if (class_exists($handler)) {
                        $handler = new $handler();
                    }
                }

                return $handler($request, ... array_values($params));
        }

        throw new LogicException('Something wrong with routing');
    }
}
