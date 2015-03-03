# flume-ng-extends-sink
several flume NG sinks: Advanced logger sink, Kafka sink, etc.

* AdvancedLoggerSink:
* KafkaSink:


##AdvancedLoggerSink


* 背景：`LoggerSink`只在log中输出Event的前16 bytes的内容。
* 目标：实现Event输出bytes个数的可配置；
* 思路：基于`org.apache.flume.sink.LoggerSink`扩展，指定bytes个数的参数；


###配置示例

使用AdvancedLoggerSink，指定输出Event的bytes个数为1000，具体配置：

	
	agent.sinks.advancedLogger.type = com.github.ningg.flume.sink.AdavncedLoggerSink
	agent.sinks.advancedLogger.maxBytes = 1000


###参数说明

`com.github.ningg.flume.sink.AdavncedLoggerSink`中可配置参数如下（**必须配置**的属性已加黑）：

|Property Name	|Default	|Description|
|----|----|----|
|**channel**|–	| |
|**type**|–	|The component type name, needs to be `com.github.ningg.flume.sink.AdavncedLoggerSink`|
|maxBytes|`16` | The maximum number of event's bytes, which will be logged at INFO level.|




**备注**：针对`AdvancedLoggerSink`，之前写过[一篇博客][Flume advanced logger sink]。


##KafkaSink

从Flume channel中获取Event，进行ETL之后，将数据发送至Kafka。包括两类需求：

* channel中一个event，抽取后，对应Kafka中一条记录；
* channel中多个event，抽取后，对应Kafka中一条记录；


###OneToOneKafkaSink

`OneToOneKafkaSink`主要用于解决channel中一个event对应一条Kafka记录的问题，具体是从[thilinamb-Kafka Sink][thilinamb-Kafka Sink]获取的代码，修改之处：

* 去掉了`MessagePreprocessor`
* 添加了编码方式：设置OneToOneKafkaSink的`charset`属性，即可指定Event内容的编码方式。

注：向[thilinamb-Kafka Sink][thilinamb-Kafka Sink] 提交PR，添加编码方式设置。


###ManyToOneKafkaSink

`ManyToOneKafkaSink`主要用于解决channel中多个event对应一条Kafka记录的问题，其关键点：

* ETL，将多个channel中获取的event，抽取为一条Kafka中记录；
* 多个channel event组成一个单元的事务管理；


####TODO LIST

* KafkaSink向Kafka集群批量发送数据
	* 背景：`ManyToOneKafkaSink`因为涉及heavy duty ETL，因此从channel中消费event的速度较慢，一定情况下，可能造成channel空间占满，最终导致Flume agent进程终止。
	* 解决思路：查看Kafka Producer API；


















[Flume advanced logger sink]:				http://ningg.github.io/flume-advance-logger-sink/
[thilinamb-Kafka Sink]:						https://github.com/thilinamb/flume-ng-kafka-sink

