#!/usr/bin/env bash
echo "struct redisCommands {
    const char *name;
    const char *flags;
};

struct redisCommands redisCommandTable[] = {" > redisCommands.cc

awk '/struct redisCommand redisCommandTabl/,/};/' ../redis/src/redis.c | grep "^    {" | awk -F, '{printf "%s,%s},\n", $1, $4}' >> redisCommands.cc

echo "    {0, 0}
};" >> redisCommands.cc
