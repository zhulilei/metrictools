# web api

## Show metric data

    GET /metric?metrics=a,b,c&starttime=1&endtime=2

metrics should not be nil.
if starttime & endtime are nil, starttime = Now().Unix(), endtime = starttime -3*3600

## Setting metric archive

    POST /metric
    # json data
    {
        "assa":1,
        "assb":3,
    }

first part is metric, second is data's ttl day.

## Delete metric

    DELETE /metric/{name}


## Get host's archived metrics list

    GET /host/{name}

## Delete host's metric info

    DELETE /host/{name}


## Get host's all metrics

    GET /host/{host}/metric

## Delete hosts's metric

    DELETE /host/{host}/metric/{name}

## Get statistic result data

    GET /statistic/{name}

## Create trigger

    POST /trigger
    #json data
    {
        "expression":"aaa",
        "trigger_type":1,
        "relation":2,
        "interval":30,
        "period":120,
        "name":"asdasdasd",
        "role":"test"
    }

## Delete trigger

    DELETE /trigger/{name}

## Get trigger info

    GET /trigger/{name}

## Create trigger's action

    POST /trigger/{t_name}/action
    #json data
    {
        "name":"aaaa",
        "repeat":2,
        "Uri":"http://asdasd",
    }

## Get trigger's action info

    GET /trigger/{t_name}/action

## Delete trigger's action

    DELETE /trigger/{t_name}/action/{name}
