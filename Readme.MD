一个kafka 的消息、偏移的小工具

### 解决的问题

1. 获取固定偏移的消息
2. 推送消息
3. 获取消费组的偏移量
4. 修改消费组的偏移量 
5. 输入命令，可以查看帮助

### 获取固定偏移的消息

```shell
        ./kafkahelper message fetch -c config.json -o offset -p partition [ -t topic ] [ -i item ] [-debug]

        -c string  config path
        -debug     print sarama logs
        -i string  item
        -limit int numbers of show message. (default: 1)
        -o int     offset (support special offset: -1 newest, -2 oldest
        -p int     partition id
        -t string  topic
```

### 推送消息

```shell
        ./kafkahelper message send -c Config.json -p partition [ -t topic ] [ -i item ] [-debug] <message>

        -c string config path
        -debug    print sarama logs
        -i string item
        -p int    partition id
        -t string topic
```

### 获取消费组的偏移量

```shell
        ./kafkahelper offset get -c Config.json -i item [ -debug ]

        -c string Config path
        -debug    print sarama logs
        -i string item
```

### 修改消费组的偏移量

修改消费组的偏移量，必须先将消费进程关闭

```shell
        ./kafkahelper offset set -c Config.json -i item -o offset [ -p partitionId ] [ -debug ]

        -c string Config path
        -debug    print sarama logs
        -i string item
        -o int    offset (support special offset: -1 newest, -2 oldest)
        -p int    partition id
```

### config.json

a example:
```
{
    "brokers": [
        "broker-url:9092"
    ],
    "topics": {
        "item1": {
            "topic": "simpleTopic",
            "group": "simpleTopicConsumerGroup"
        }
    }
}
```


### 一些例子

item 为 一个topic,group对，具有唯一性

1. 获取item的偏移量

> ./kafkaHelper offset get -c config.json -i item

输出:
```
+------------+------------+------------+------------+
|index       |oldest      |newest      |current     |
+------------+------------+------------+------------+
|0           |556781      |563419      |563419      |
|1           |517617      |523562      |523562      |
|2           |495743      |501642      |501642      |
+------------+------------+------------+------------+
```

2. 修改item的所有偏移量为最新偏移

> ./kafkaHelper offset set -c config.json -i item -o -1

3. 修改item的partition 为 3 的偏移为12345

> ./kafkaHelper offset set -c config.json -i item -o 12345 -p 3
输出:

partition 1 update to 47
partition 1 update to 1
partition 2 update to 3129129
partition 3 update to 97413
partition 4 update to 252
partition 5 update to 12

4. 获取item在partition 为 3，偏移为12345 的消息

> ./kafkaHelper message fetch -c config.json -o 12345 -p 3 -i item

```
{"hello":"world"}
```

5. 获取topic 在partition 为 3，偏移为12345 的消息

> ./kafkaHelper message fetch -c config.json -o 12345 -p 3 -t topic 

```
{"hello":"world"}
```

6. 发送消息到partition 为3的分片上
> ./kafkaHelper message send -c config.json -t topic -p 3 '{"hello":"world"}' '{"hello": "word2"}'
