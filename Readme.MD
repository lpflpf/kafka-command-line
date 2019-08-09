一个kafka 的消息、偏移的小工具

### 解决的问题

1. 获取固定偏移的消息
2. 推送消息
3. 获取消费组的偏移量
4. 修改消费组的偏移量 

### 获取固定偏移的消息

```shell
        ./kafkahelper message fetch -c Config.json -o offset -p partition [ -t topic ] [ -i item ] [-debug]

        -c string  config path
        -debug     print sarama logs
        -i string  item
        -limit int numbers of show message. (default: 1)
        -o int     offset (support special offset: -1 newest, -2 oldest
        -p int     partition id
        -t string  topic
```

### 推送消息

```sbtshell
        ./kafkahelper message send -c Config.json -p partition [ -t topic ] [ -i item ] [-debug] <message>

        -c string config path
        -debug    print sarama logs
        -i string item
        -p int    partition id
        -t string topic
```

### 获取消费组的偏移量

```sbtshell
        ./kafkahelper offset get -c Config.json -i item [ -debug ]

        -c string Config path
        -debug    print sarama logs
        -i string item
```

### 修改消费组的偏移量

修改消费组的偏移量，必须先将消费进程关闭

```sbtshell
        ./kafkahelper offset set -c Config.json -i item -o offset [ -p partitionId ] [ -debug ]

        -c string Config path
        -debug    print sarama logs
        -i string item
        -o int    offset (support special offset: -1 newest, -2 oldest)
        -p int    partition id
```

### config.json

a Example:
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
