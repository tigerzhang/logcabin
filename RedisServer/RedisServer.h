/*
* ----------------------------------------------------------------------------
* Copyright (c) 2015-2016, xSky <guozhw@gmail.com>
* All rights reserved.
* Distributed under GPL license.
* ----------------------------------------------------------------------------
*/

#include <unistd.h>

#include "../Server/Globals.h"

#include "RedisServerLib.h"

class RedisConnect :public RedisConnectorBase
{
public:
    RedisConnect();
    ~RedisConnect();
private:
};

std::map<std::string, std::string> kvmap;


//
//int main(int argc, char **argv)
//{
//    RedisServer redis;
//    redis.Init();
//    string pass = "shahe22f";
//    redis.SetPassword(pass);
//    redis.Start("127.0.0.1", 6479);
//
//    while (1) {
//        usleep(1000);
//    }
//
//    return 0;
//}

