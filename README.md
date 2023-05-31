# PHP Event Correlator (Event Sourcing)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Flucasnetau%2Fphp-ec.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Flucasnetau%2Fphp-ec?ref=badge_shield) ![GitHub](https://img.shields.io/github/license/lucasnetau/php-ec)

PHP Event Correlator is an event correlation tool to process a stream of events, make decisions based on Rules sets, and perform actions based on these Rules.

Event correlation is useful in many areas. For example, log processing, microservice coordination, IoT event processing.

PHP-EC is being used in production for multiple IoT projects, some processing 10M+ events per day from multiple disparate systems.

## Requirements

The package is compatible with PHP 8.0+

## Installation

You can add the library as project dependency using [Composer](https://getcomposer.org/):

```sh
composer require edgetelemetrics/eventcorrelation
```

Performance can be improved by installing optional PHP extensions (ext-libuv or ext-libev)

## Examples
See [/examples](/examples) directory
* [Online shopping service](/examples/online_shop/online_shop.php)

## Getting Started
TBC.

Components are:
 * Events: An event is made up of a type, date, id, and data
 * Event Stream: A time ordered stream of events
 * Input Processes: programs that provide event streams into PHP-EC. These can be PHP scripts, NodeJS, Bash script etc.
 * Rules: Rules define a group of events to be processed within a defined time period. Rules can call actions when the Rule is matched or if the defined time period is exceeded (timeout)
 * Actions: Programs that perform tasks when requested. This may be writing to a database, sending an email etc.

### Cron Like Rules
PHP-EC supports rules that can call actions based on Cron time expressions, system start and system shutdown.

## License
MIT, see [LICENSE file](LICENSE).

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Flucasnetau%2Fphp-ec.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Flucasnetau%2Fphp-ec?ref=badge_large)

### Contributing

Bug reports (and small patches) can be submitted via the [issue tracker](https://github.com/lucasnetau/php-ec/issues). Forking the repository and submitting a Pull Request is preferred for substantial patches.