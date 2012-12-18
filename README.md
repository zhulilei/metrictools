# MetricTools

## Goal
It's a distributed system monitor toolset.

## Tool Chains
### metric_processor
metric_processor read metric data from rabbitmq, then write to mongodb and redis. It will scan mongodb and dispatch trigger calculate task via rabbitmq.

### metric_statistic
metric_statistic read trigger from rabbitmq, then calculate trigger and write the result to redis. metric_statistic will read calculate result from redis.

### metric_web
It's a web api to access metric data in mongodb. The json data can be processed by [nvd3.js](http://nvd3.org).

## Requried
 * collectd
 * rabbitmq
 * mongodb
 * redis

## data format
 [MetricFormat](http://code.google.com/p/rocksteady/wiki/MetricFormat)

## More
