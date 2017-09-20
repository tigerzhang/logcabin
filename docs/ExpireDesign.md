#Expire Command Design

Logcabin的expire命令设计。设计遵从一下几个原则：
1. 重放日志的时候能保证每次执行的结果都相同
2. 符合Redis的expire命令设计，输入为秒级保留时间，并且任何写命令都将移除expire时间

## 内部实现设计

### 总体思路
通过给每个写请求设置一个接收的时间，保证写请求在有expire的时候apply的一致性。设置定时器，定时清理部分已经超时的key（TODO)。每次读／写请求的时候，都主动检查expire，保证每次读写都是干净的。

### 修改Tree请求结构
1. 增加 request_time 字段，任何写命令，在服务器收到之后都将设置这个字段为当前时间（单位：秒）
2. 增加 expire 字段，用于声明需要expire的key，以及对应的expire时间。

### 修改Tree对请求的处理
1. 请求设置expire的时候，将 ${key}:e:meta 设置为对应的expire时间，该时间为从UTC时间，非相对时间。
2. 每次请求写的时候，都会根据request_time检查是否有expire设定，如果有，是否已经expire，已经expire的时候，会清理掉expire的内容。注意这里是用request_time进行检查，而非当前时间，这样才能保证写请求不会因为apply日志的时间不同而出现差异。
3. 每次请求读的时候，使用当前时间进行expire检查，检查内容与写请求类似。但是由于读请求没有时间戳，所以理论来说读请求不应该修改任何本地树结构，以防止因为脑裂带来的状态机不一致问题。故而在做读请求的时候，如果已经expire，但是还没有清理，应该返回一个key expire，并且通过raft，append一个expire操作，进行清理。
4. 由于Tree结构体会被log拷贝，并且dump到硬盘，使得reqeust_time也保留下来，保证了在重放的时候的一致性。
5. 为保证expire的一致性，除了写的时候主动清理expire之外，任何对expire的清理都应该有明确的日志，以日志作为标准，进行expire清理。

