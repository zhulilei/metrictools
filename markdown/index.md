# Introducing metrictools

It's distributed system monitoring solution. It support collectd's json format data.

## Metrictools

### metricprocessor

`metricprocessor` read collectd json data from nsq, parse json and store data into redis.
it also check metrics' archive time, and send metrics' name to `metricarchive`.

`metricprocessor` scan all records in redis which match `trigger:*`, and send them to `metricstatistic`

    collectd -> nsq -> metricprocessor -> redis

### metricarchive

`metricarchive` read metric name from nsq, compress and remove old data.

    metricprocessor -> nsq -> metricarchive -> redis

### metricstatistic

`metricstatistic` read trigger from nsq, then calculate triggers' expression.
It also use `etsy/skyline` algorithms to check data. If skyline algorithms return true, send data to `metricnotify`.

    # calculate expression
    metricprocessor -> nsq -> metricarchive -> redis
    # skyline
    redis -> metricarchive -> nsq -> metricnotify

### metricwebservice

`metricwebservice` is a web api, it provide data from redis.

    metricwebservice <-> redis

See also (github.com/datastream/metricweb)

### metricnotify

`metricnotify` read data, and send data via email, http etc.

    metricstatistic -> nsq -> metricnotify -> email/http/im etc.

All these tools communicate data via nsq.

## How to Setup

### Require
1. collectd v4.8+ (write_http plugin)
1. nsq
1. redis (optional twemproxy)

#### collectd

collectd runs on every node, and send data to nsq api via http.

    <Plugin write_http>
      <URL "http://nsq_node:4151/put?topic=metric">
        Format "JSON"
        User "instance_name"
        Password "user_token"
      </URL>
    </Plugin>

#### nsq

[Quick Start](http://bitly.github.io/nsq/overview/quick_start.html)


#### redis

on debian

    apt-get install redis-server
    service redis-server start

on freebsd

    cd /usr/ports/databases/redis;make install
    service redis-server start

on mac ox x

    brew install redis-server
    /usr/local/bin/redis-server

#### twemproxy

metrictools support [twemproxy](https://github.com/twitter/twemproxy) now.

#### metrictools

    git clone github.com/datastream/metrictools
    cd metrictools;make
    vim metrictools.json

    {
        "lookupd_addresses":"127.0.0.1:4160",
        "nsqd_addr":"127.0.0.1:4151",
        "maxinflight":"200",
        "metric_topic":"metric",
        "trigger_topic":"trigger",
        "trigger_channel":"trigger_ch",
        "notify_topic":"notify",
        "notify_channel":"notify_ch",
        "redis_conn_count":"10",
        "config_redis_server":"127.0.0.1:6379",
        "config_redis_auth":"admin",
        "data_redis_server":"127.0.0.1:6379",
        "data_redis_auth":"admin",
        "web_bind":"127.0.0.1:1234",
        "notify_email_address":"notify@test.org"
    }

change `nsq lookupd address` redis_server's ip and auth.

##### Run metrictools

1. `cd metricprocessor;./metricprocessor -conf ../metrictools.json`
1. `cd metricstatistic;./metricstatistic -conf ../metrictools.json`
1. `cd metricwebservice;./metricwebservice -conf ../metrictools.json`
1. `cd metricarchive;./metricarchive -conf ../metrictools.json`
1. `cd metricnotify;./metricnotify -conf ../metrictools.json`

## Todo

1. improve metricwebservice
2. improve anomalous metric detect algorithms
