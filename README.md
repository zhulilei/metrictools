# MetricTools

## Goal
It's a distributed system monitor toolset.

## Tools
### metricprocessor
metric_processor read metric data from mq, then write redis.
It will scan `trigger` records in redis and dispatch task via nsq.

A `trigger` may looks like `10min.cpu.free.hostA/10min.memory.free.hostA`.

### metricstatistic
metricstatistic read `trigger` from mq, then calculate it.
metricstatistic can read/write calculate result in redis.

### metricnotify
metricnotify read notify from mq, then query notify's action in redis.

### metricwebservice
It's a web api to access metric data in redis.

### metricweb

move to github.com/datastream/metricweb

### metricarchive
delete old data, compress data

## Require
 * collectd v4.8+ (write_http plugin)
 * nsq
 * redis

## data format

collectd's json format https://collectd.org/wiki/index.php/Plugin:Write_HTTP

    {
        "values":[2566625042,2028255604],
        "dstypes":["derive","derive"],
        "dsnames":["rx","tx"],
        "time":1363158128.258,
        "interval":60.000,
        "host":"mon6.photo.163.org",
        "plugin":"interface",
        "plugin_instance":"eth1",
        "type":"if_packets",
        "type_instance":""
    }

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

1. improve metricwebservice
