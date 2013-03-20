* JSON

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

* Command

    PUTVAL mon6.photo.163.org/cpu-0/cpu-interrupt interval=60.000 1363158093.890:3315

    host = mon6.photo.163.org
    plugin + plugin_instance = cpu-0
    type + type_instance = cpu-interrupt
    interval = 60
    time + value = 1363158093.890:3315

* MongoDB

1. K

    host + "_" + strconv.Itoa(int(this.Interval)) + "_" + plugin + "_" + plugin_instance + "." + type + "_" + type_instance

2. V

    value

3. T

    timestamp
