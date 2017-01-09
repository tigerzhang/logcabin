Description
===========

This is a c++11 redis client using boost.
Make sure to download the boost library before compiling the code.

DISCLAIMER: For personal use on a local redis server. Did not try with a bad
tcp connection so the approach might be still naive.

To do
=====

* Add a timeout for reading from server.
* More API

Usage
=====

Base API
--------

**Initialize a redis connection**

Will raise an exception if cannot connect to the server. Indeed, the RedisInterface
object is useless in this case.

```c++
try {
    RedisInterface redis("localhost", "6379");
} catch {
    std::cerr << "Couldn't connect to redis server\n";
}
```

**Get a key value**

```c++
auto reply = redis.Get("mykey");
```

**Set a value**

```c++
auto reply = redis.Set("mykey", "myvalue");
```

**Push to the left or to the right of a list**

Use Lpush or Rpush.

```c++
auto reply = redis.Lpush("mylist", "value1");
```

or

```c++
auto reply = redis.Lpush("mylist", "value1", "value2", "value3");
```

**Pop from the left or the right of a list**

Use Lpop or Rpop

```c++
auto reply = redis.Lpop("mylist");
```

**Send a command without the simple API**

You can create your command and send it to the server by doing the following:

```c++
std::vector<std::string> command_tokens = {"LRANGE", "mylist", "0", "-1"};
auto reply = redis.SendCommand(command_tokens);
```

The reply object
----------------

All the redis interface API return a unique pointer to a reply object. This reply
object has the following fields:

* type [RedisDataType] -> Will indicate the type of the reply
* string_value [std::string] -> if type is STRING, it will contains the string message.
If type is ERROR, it will contain the error message.
* integer_value [int] -> Will contain the integer value if type is INTEGER.
* elements [std::vector<RedisReplyPtr>]: If reply is an ARRAY, contains a vector of
pointer to the elements which constitute the array.

You can parse a reply using the following:

```c++
void PrintReply(rediscpp::protocol::RedisReply* reply)
{
    switch (reply->type) {
    case rediscpp::protocol::STRING:
        std::cout << "STRING: " << reply->string_value << std::endl;
        break;
    case rediscpp::protocol::INTEGER:
        std::cout << "INTEGER: " << reply->integer_value << std::endl;
        break;
    case rediscpp::protocol::ERROR:
        std::cout << "ERROR: " << reply->string_value << std::endl;
        break;
    case rediscpp::protocol::NIL_VALUE:
        std::cout << "NIL VALUE\n";
        break;
    case rediscpp::protocol::ARRAY:
        std::cout << "Got an array of " << reply->elements.size() << " elements\n";
        for (auto& el : reply->elements) {
            PrintReply(el.get());
        }
        break;
    }
}
```
