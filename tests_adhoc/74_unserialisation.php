<?php

include __DIR__ . '/../vendor/autoload.php';

$serialised = "C:38:\"EdgeTelemetrics\\EventCorrelation\\Event\":82:{{\"id\":null,\"event\":\"Test:Event:Single\",\"datetime\":\"2021-05-31T14:00:00.000+00:00\"}}";

print_r(unserialize($serialised));

echo serialize(unserialize($serialised));

echo PHP_EOL . PHP_EOL . PHP_EOL;

$serialised = "O:38:\"EdgeTelemetrics\\EventCorrelation\\Event\":3:{s:2:\"id\";N;s:5:\"event\";s:17:\"Test:Event:Single\";s:8:\"datetime\";s:29:\"2021-05-31T14:00:00.000+00:00\";}";

print_r(unserialize($serialised));

echo serialize(unserialize($serialised));

echo PHP_EOL . PHP_EOL . PHP_EOL;

$serialised = "C:69:\"EdgeTelemetrics\\EventCorrelation\\tests\\Rules\\MatchOneRuleContinuously\":117:{{\"events\":[\"00000000000000060000000000000000\"],\"id\":\"95051be938\",\"actionFired\":false,\"isTimedOut\":false,\"context\":[]}}";

print_r(unserialize($serialised));

echo serialize(unserialize($serialised));

echo PHP_EOL . PHP_EOL . PHP_EOL;

$serialised = "O:69:\"EdgeTelemetrics\\EventCorrelation\\tests\\Rules\\MatchOneRuleContinuously\":5:{s:6:\"events\";a:1:{i:0;s:32:\"00000000000000060000000000000000\";}s:2:\"id\";s:10:\"675291bc6f\";s:11:\"actionFired\";b:0;s:10:\"isTimedOut\";b:0;s:7:\"context\";a:0:{}}";

print_r(unserialize($serialised));

echo serialize(unserialize($serialised));

