# MetricTools

## Goal
It's a distributed system monitor toolset.

## Tools
### metricprocessor
metric_processor read metric data from mq, then write mongodb and redis.
It will scan `trigger` records in mongodb and dispatch task via rabbitmq.

A `trigger` may looks like `10min.cpu.free.hostA/10min.memory.free.hostA`.

### metricstatistic
metric_statistic read `trigger` from mq, then calculate it.
metric_statistic can read/write calculate result in redis.

### metricnotify
metric_notify read notify from mq, then query notify's action in mongodb.

### metricwebservice
It's a web api to access metric data in redis.
The json data can be processed by [nvd3.js](http://nvd3.org).

### metricweb

move to github.com/datastream/metricweb

### metricarchive
delete old data, remove dup data

## Require
 * collectd v4.8+ (write_http plugin)
 * nsq
 * mongodb(with ttl index support)
 * redis

## data format

collectd's json format https://collectd.org/wiki/index.php/Plugin:Write_HTTP

collectd's command format will need addition type.db to genrate record.

## collectd config

    <Plugin write_http>
      <URL "http://nsq_node:4151/put?topic=metric">
        Format "JSON"
        User "instance_name"
        Password "user_token"
      </URL>
    </Plugin>

## Current Staff

1. test trigger

## Why switch to NSQ

1. no single node fail
1. easy deploy

## Todo

1. support data compress
2. try to port etsy/skyline
