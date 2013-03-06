# MetricTools

## Goal
It's a distributed system monitor toolset.

## Tools
### metric_processor
metric_processor read metric data from rabbitmq, then write mongodb and redis.
It will scan `trigger` records in mongodb and dispatch task via rabbitmq.

A `trigger` may looks like `10min.cpu.free.hostA/10min.memory.free.hostA`.

### metric_statistic
metric_statistic read `trigger` from rabbitmq, then calculate it.
metric_statistic can read/write calculate result in redis.

### metric_notify
metric_notify read notify from rabbitmq, then query notify's action in mongodb.

### metric_web
It's a web api to access metric data in mongodb.
The json data can be processed by [nvd3.js](http://nvd3.org).

## Require
 * collectd
 * [collectd-amqp](https://github.com/datastream/collectd-amqp)
 * rabbitmq
 * mongodb(with ttl index support)
 * redis

## data format
 [MetricFormat](http://code.google.com/p/rocksteady/wiki/MetricFormat)

## Todo

1. support data compress
1. data collect agent, something like collectd
1. switch to nsq
