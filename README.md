# MetricTools

## Goal
It's a distributed system monitor toolset.

## Tool Chains
### metric_processor
metric_processor read metric data from rabbitmq, then write to mongodb and redis. It will scan mongodb and dispatch trigger calculate task via rabbitmq.

### metric_statistic
metric_statistic read trigger from rabbitmq, then calculate trigger and write the result to redis. metric_statistic will read calculate result from redis.

### metric_notify
metric_notify read notify from rabbitmq, then query notify's action in mongodb.

### metric_web
It's a web api to access metric data in mongodb. The json data can be processed by [nvd3.js](http://nvd3.org).

### metric_expire
Just a tool to auto clean old data in mongodb. If you are using latest mongodb version, you can use ttl index instead.

## Require
 * collectd
 * [collectd-amqp](https://github.com/datastream/collectd-amqp)
 * rabbitmq
 * mongodb
 * redis

## data format
 [MetricFormat](http://code.google.com/p/rocksteady/wiki/MetricFormat)

## More
