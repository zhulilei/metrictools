# MetricTools

## 目标
>  这个项目的目标是设计一个健壮的分布式的监控系统。在云计算平台环境中，提供必要 可靠的监控手段。

## 工具链
>  整个项目由客户端，消息队列，消息处理,数据存储，web,5部分组成。
> 消息处理: amqp2mongodb, alarm
> web显示: web

## 依赖项目
> 数据收集端: collectd
> 消息队列: rabbitmq
> 数据存储: mongodb

## 数据协议
>  使用[MetricFormat](http://code.google.com/p/rocksteady/wiki/MetricFormat)。

## web
> 当前的设计是 rails + golang写的webserver。
> http 只是提供单纯的 json 数据接口。

## 高可用设计
> 数据收集客户端： 依赖 collectd。
> 客户端到消息队列间的通讯用 tcp 协议。消息队列使用统一的入口，可以考虑在消息队列前 加 haproxy。
> rabbitmq消息队列需要配置成多备份，分布式，持久化到硬盘。
> 消息处理程序本身是无状态的，主要的工作是对消息队列中的数据做二次处理，并且将处理结果推送到后端的 mongodb。
> 数据存储使用 mongodb，同样也设置多备份。
> web程序主要是将 mongodb 里面的数据按照 json 的格式返回。web 展示端，使用 js 库将 json 渲染成图表。
