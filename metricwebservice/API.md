# web api

## Show metric data

    GET /metric?metrics=a,b,c&starttime=1&endtime=2

metrics should not be nil.
if starttime & endtime are nil, starttime = Now().Unix(), endtime = starttime -3*3600

## Setting metric TTL

    POST /metric
    # json data
    {
        "assa":1,
        "assb":3,
    }

first part is metric, second is data's ttl.

## Delete metric

    DELETE /metric/{name}

## Get host info

    GET /host/{name}

return:

    {
        "name":"xxxx",
        "metrics_url":"/host/xxxx/metric"
    }

## Delete host

    DELETE /host/{name}

## Get host's all metrics

    GET /host/{host}/metric

return:

    [{
        "name":"xxx_cpu0_user",
        "host":"xxx",
        "plugin":"cpu",
        "plugin_instance":"0",
        "type":"user",
        "type_instance":"",
        "interval":60,
        "dstype":"COUNT",
        "ttl":3600,
        "url":"/api/v1/metric/xxx_cpu0_user"
    }]

## Delete metric and remove metric from host

    DELETE /host/{host}/metric/{name}

## Get statistic result data

    GET /statistic/{name}

## Create trigger

    POST /trigger
    #json data
    {
        "expression":"aaa",
        "role":"test",
    }

## Delete trigger

    DELETE /trigger/{name}

## Get trigger info

    GET /trigger/{name}

## Create trigger's action

    POST /trigger/{trigger}/action
    #json data
    {
        "name":"aaaa",
        "repeat":2,
        "Uri":"http://asdasd",
    }

## Get trigger's action info

    GET /trigger/{t_name}/action

## Delete trigger's action

    DELETE /trigger/{trigger}/action/{name}
