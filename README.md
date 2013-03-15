# MetricTools

## Goal
It's a distributed system monitor toolset.

## Tools
### metric_processor
metric_processor read metric data from mq, then write mongodb and redis.
It will scan `trigger` records in mongodb and dispatch task via rabbitmq.

A `trigger` may looks like `10min.cpu.free.hostA/10min.memory.free.hostA`.

### metric_statistic
metric_statistic read `trigger` from mq, then calculate it.
metric_statistic can read/write calculate result in redis.

### metric_notify
metric_notify read notify from mq, then query notify's action in mongodb.

### metric_web
It's a web api to access metric data in mongodb.
The json data can be processed by [nvd3.js](http://nvd3.org).

## Require
 * collectd v4.8+ (write_http plugin)
 * nsq
 * mongodb(with ttl index support)
 * redis

## data format

collectd's json/command format https://collectd.org/wiki/index.php/Plugin:Write_HTTP

## Current Staff

1. switching to nsq

## Why switch to NSQ

1. no single node fail
1. easy deploy

## Todo

1. support data compress
