//
//  test.cpp
//  BGPGeopol
//
//  Created by Kave Salamatian on 21/08/2019.
//  Copyright © 2019 Kave Salamatian. All rights reserved.
//

#include <stdio.h>
#include <map>
#include <iostream>
#include "BGPRedis.hpp"
#include "BGPTables.h"
//#include "BGPPopulate.h"


extern "C" {
#include "bgpstream.h"
}

using namespace std;

class Record{
public:
    unsigned int time;
    string collector;
    Record(unsigned int time, string collector): time(time), collector(collector){};
};

BGPCache *cache;

Redis *bgpRedisConnect(string host, int port){
    try {
        ConnectionOptions connection_options;
        connection_options.host = host;  // Required.
        connection_options.port = port; // Optional. The default port is 6379.
        connection_options.db = 1;  // Optional. Use the 0th database by default.

        // Optional. Timeout before we successfully send request to or receive response from redis.
        // By default, the timeout is 0ms, i.e. never timeout and block until we send or receive successfuly.
        // NOTE: if any command is timed out, we throw a TimeoutError exception.
//        connection_options.socket_timeout = std::chrono::milliseconds(500);

        ConnectionPoolOptions pool_options;
        pool_options.size = 10;  // Pool size, i.e. max number of connections.

        // Create an Redis object, which is movable but NOT copyable.
        return new Redis(connection_options, pool_options);
    } catch (const Error &e) {
        std::cout<<"Redis connection error:"<<e.what()<<endl;
        return NULL;            // Error handling.
    }
}


int main(int argc, char **argv) {    
    Redis *_redis = bgpRedisConnect("127.0.0.1", 6379);
    BlockingCollection<TraceElement *> recoverQueue(100000);
    BlockingCollection<BGPEvent *> bgpEvents(1000000);
    std::map<std::string, unsigned short int > collectors;
    collectors.insert(pair<string, unsigned short int >("rrc00",0));
    collectors.insert(pair<string, unsigned short int >("rrc01",1));
    collectors.insert(pair<string, unsigned short int >("rrc02",2));
    collectors.insert(pair<string, unsigned short int >("rrc03",3));
    collectors.insert(pair<string, unsigned short int >("rrc04",4));
    collectors.insert(pair<string, unsigned short int >("rrc05",5));
    collectors.insert(pair<string, unsigned short int >("rrc06",6));
    collectors.insert(pair<string, unsigned short int >("rrc09",7));
    collectors.insert(pair<string, unsigned short int >("rrc10",8));
    collectors.insert(pair<string, unsigned short int >("rrc11",9));
    collectors.insert(pair<string, unsigned short int >("rrc12",10));
    collectors.insert(pair<string, unsigned short int >("rrc13",11));
    collectors.insert(pair<string, unsigned short int >("rrc14",12));
    collectors.insert(pair<string, unsigned short int >("rrc15",13));
    collectors.insert(pair<string, unsigned short int >("rrc16",14));
    collectors.insert(pair<string, unsigned short int >("rrc17",15));
    collectors.insert(pair<string, unsigned short int >("rrc18",16));
    collectors.insert(pair<string, unsigned short int >("rrc19",17));
    collectors.insert(pair<string, unsigned short int >("rrc20",18));
    collectors.insert(pair<string, unsigned short int >("rrc21",19));
    BGPGraph g;
    BGPCache bgpCache("resources/as.sqlite", &g, bgpEvents, collectors);
    cache= &bgpCache;
    BGPRedis *eventHandler= new BGPRedis(&(cache->bgpEvents),_redis);
    
    vector<std::thread> redis(10), pop(10);
    int t_start =1444412200;
    int end = t_start+1*24*60*60;
    unsigned int dumpDuration = 600, val, vval;
    string str;

    RIBTable *bgpTable = new RIBTable(t_start, dumpDuration);
    BGPRecover *recover = new BGPRecover(recoverQueue,*_redis);
    BGPPopulate *populate= new BGPPopulate(recoverQueue, bgpTable, 4);
    for (int i=0;i<10;i++){
        redis[i]= std::thread(&BGPRedis::run,eventHandler);
    }
    for (int i=0;i<4;i++){
        pop[i]= std::thread(&BGPPopulate::run, populate);
    }
    recover->getPrefixes();
    recover->getASes();
    recover->getPaths();
    recover->getRoutingTable();
    TraceElement *event = new TraceElement(ENDE);
    recoverQueue.add(event);
}

    
