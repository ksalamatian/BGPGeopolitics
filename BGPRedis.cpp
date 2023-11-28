//
//  BGPRedis.cpp
//  BGPGeo
//
//  Created by zhangxinyi on 2019/3/19.
//  Copyright © 2019年 zhangxinyi. All rights reserved.
//

#include "BGPRedis.hpp"
#include "BGPEvent.h"
#include "BGPTables.h"
#include "cache.h"
#include "tbb/parallel_for.h"
#include "tbb/concurrent_vector.h"
#ifdef __linux
    #include <sys/prctl.h>
#endif
#include <algorithm>
#include <boost/algorithm/string.hpp>


using namespace sw::redis;
using namespace std;
using namespace boost;
extern BGPCache *cache;
extern RIBTable *bgpTable;



ShardedBGPRedis::ShardedBGPRedis(string host, int basePort, int dbase, int numShards, int poolSize):numShards(numShards), host(host), basePort(basePort), dbase(dbase), poolSize(poolSize){
    Redis *_redis;
    BGPRedis *redisHandler;
    BlockingCollection<BGPEvent *> *queue;
    //we open numshards REDIS database and each have 4 handlers
    for (int i=0;i<numShards;i++){
        _redis=bgpRedisConnect(host, basePort+i, dbase, poolSize);
        _redisVect.push_back(_redis);
        queue=new BlockingCollection<BGPEvent *>(1250000);
        queues.push_back(queue);
        for (int j=0;j<poolSize;j++){
            redisHandler=new BGPRedis(queue,_redis,i*poolSize+j);
            redisShards.push_back(redisHandler);
        }
    }
}

void ShardedBGPRedis::run(){
    for (int i=0;i<numShards;i++){
        for (int j=0;j<poolSize;j++){
            threads.push_back(std::thread(&BGPRedis::run,redisShards[i*poolSize+j]));
        }
    }
    for (int i=0;i<numShards;i++){
        for (int j=0;j<poolSize;j++){
            threads[i*poolSize+j].join();
        }
    }
}

void ShardedBGPRedis::end(unsigned int timestamp){
    int i=0;
    for (auto q:queues){
        BGPEvent *event = new BGPEvent(timestamp, ENDE);
        q->add(event);
    }
    int kkk=0;
}
                          
BlockingCollection<BGPEvent *> *ShardedBGPRedis::getQueue(unsigned int hash){
    int val=hash % numShards;
    return queues[val];
}

Redis *ShardedBGPRedis::getRedis(unsigned long hash){
    int val=hash % numShards;
    return _redisVect[val];
}

Redis *ShardedBGPRedis::bgpRedisConnect(string host, int port, int dbase, int poolSize){
    try {
        ConnectionOptions connection_options;
        connection_options.host = host;  // Required.
        connection_options.port = port; // Optional. The default port is 6379.
        connection_options.db = dbase;  // Optional. Use the 0th database by default.

        // Optional. Timeout before we successfully send request to or receive response from redis.
        // By default, the timeout is 0ms, i.e. never timeout and block until we send or receive successfuly.
        // NOTE: if any command is timed out, we throw a TimeoutError exception.
        //        connection_options.socket_timeout = std::chrono::milliseconds(500);

        ConnectionPoolOptions pool_options;
        pool_options.size = poolSize;  // Pool size, i.e. max number of connections.

        // Create an Redis object, which is movable but NOT copyable.
        return new Redis(connection_options, pool_options);
    } catch (const Error &e) {
        std::cout<<"Redis connection error:"<<e.what()<<endl;
        return NULL;            // Error handling.
    }

}

void ShardedBGPRedis::setSavingMode(){
    for (auto shards:redisShards){
        shards->setSavingMode();
    }
}

void ShardedBGPRedis::resetSavingMode(){
    for (auto shards:redisShards){
        shards->setSavingMode();
    }
}

void ShardedBGPRedis::getPrefixes(){
    Redis *_redis;
    concurrent_vector<string> keys;
    cout<<"Begin Getting Prefixes"<<endl;
    tbb::parallel_for( tbb::blocked_range<unsigned long >(0,numShards),
                       [&](tbb::blocked_range<unsigned long> range)
    {
        for (int i=range.begin(); i<range.end(); ++i){
            _redis=getRedis(i);
            _redis->zrangebyscore("PREFIXES", UnboundedInterval<double>{},std::back_inserter(keys));
        }
    });
    tbb::parallel_for( tbb::blocked_range<unsigned long >(0,keys.size()),
                       [&](tbb::blocked_range<unsigned long> range)
    {
        bgpstream_pfx_t pfx;
        string pfxStr;
        vector<string> keysID(range.end()-range.begin());
        for (int i=range.begin(); i<range.end(); ++i){
//            bgpstream_str2pfx(keys[i].substr(4).c_str(),&pfx);
            bgpstream_str2pfx(keys[i].c_str(),&pfx);
            bgpTable->ribTrie->checkinsert(&pfx);
            keysID.clear();
        }
    });
    cout<<"Finish Getting "<<keys.size()<<" Prefixes"<<endl;
    keys.clear();
}


bool myComp(pair<string, string> p1, pair<string, string>p2){ return from_myencoding(p2.second) < from_myencoding(p1.second); }

void ShardedBGPRedis::getPaths(){
    concurrent_vector<pair<string, string>> keys;
    Redis *_redis;
    
    cout << "Begin Getting Paths" << endl;
    tbb::parallel_for( tbb::blocked_range<unsigned long >(0,numShards),
                       [&](tbb::blocked_range<unsigned long> range)
    {
        for (int i=range.begin(); i<range.end(); ++i){
            _redis=getRedis(i);
            _redis->hgetall("PATH2ID",std::back_inserter(keys));
        }
    });
    unsigned max=from_myencoding(std::max_element(keys.begin(),keys.end(), myComp)->first);
    cache->pathsMap.setID(max+1);
    tbb::parallel_for( tbb::blocked_range<unsigned long >(0,keys.size()), [&](tbb::blocked_range<unsigned long> range)
    {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            cache->pathsBF.insert(keys[i].first);
        }
    });
    cout<<"Finish Getting "<<keys.size()<<" Paths"<<endl;
    keys.clear();
    return;
}

void ShardedBGPRedis::getLinks(){
    concurrent_vector<pair<string, string>> keys;
    Redis *_redis;
    
    cout<<"Begin Getting Links"<<endl;
    tbb::parallel_for( tbb::blocked_range<unsigned long >(0,numShards),
                       [&](tbb::blocked_range<unsigned long> range)
    {
        for (int i=range.begin(); i<range.end(); ++i){
            _redis=getRedis(i);
            _redis->hgetall("LINKS", std::back_inserter(keys));
        }
    });
    tbb::parallel_for( tbb::blocked_range<unsigned long >(0,keys.size()),
                       [&](tbb::blocked_range<unsigned long> range)
    {
        std::unordered_map<string, string> linkMap;
        string linkID;
        for (int i=range.begin(); i<range.end(); ++i) {
            linkID=keys[i].first;
            linkMap["STR"]=keys[i].second;
            Link *lnk=new Link(linkMap);
            linkMap.clear();
            cache->linksMap.insert(make_pair(lnk->linkID(),lnk));
            if (lnk->isActive()) {
                lnk->addLinks(0);
            }
        }
    });
    cout<<"Finish Getting "<< cache->linksMap.size()<<" Links"<<endl;
    return;
}

void ShardedBGPRedis::getRoutingTable(){
    concurrent_vector<string> keys;
    Redis *_redis;
    cout << "Begin Getting Routing Entries" << endl;
    tbb::parallel_for( tbb::blocked_range<unsigned long >(0,numShards),
                       [&](tbb::blocked_range<unsigned long> range)
    {
        for (int i=range.begin(); i<range.end(); ++i){
            _redis=getRedis(i);
            _redis->zrangebyscore("ROUTINGENTRIES", UnboundedInterval<double>{},std::back_inserter(keys));
            _redis->zrangebyscore("INACTIVEROUTINGENTRIES", UnboundedInterval<double>{},std::back_inserter(keys));
        }
    });
    tbb::parallel_for( tbb::blocked_range<unsigned long >(0,keys.size()),[&](tbb::blocked_range<unsigned long> range)
    {
       for (size_t i = range.begin(); i < range.end(); ++i) {
           cache->routingBF.insert(keys[i]);
        }
    });
    cout<<"Finish Getting "<<keys.size()<<" active routing entries"<<endl;
    keys.clear();
    return;
}

void  ShardedBGPRedis::populate(){
    getASes();
    getPrefixes();
    getLinks();
    getPaths();
    getRoutingTable();
}


void ShardedBGPRedis::add(BGPEvent *event){
    getQueue(event->hash)->add(event);
}


pair<long,long> ShardedBGPRedis::getPathsStat(){
    Redis *_redis;
    long all=0;
    long active=0;
    for (int i=0; i<numShards; ++i){
        _redis=getRedis(i);
        all +=_redis->hlen("PATHS");
        active += _redis->zcard("APATHS");
    }
    return make_pair(all,active);
}

pair<long,long> ShardedBGPRedis::getRoutingStat(){
    Redis *_redis;
    long active=0, inactive=0;
    for (int i=0; i<numShards; ++i){
        _redis=getRedis(i);
        active +=_redis->zcard("ROUTINGENTRIES");
        inactive +=_redis->zcard("INACTIVEROUTINGENTRIES");
    }
    return make_pair(active, inactive);
}

int ShardedBGPRedis::getNumShards(){
    return numShards;
}


void ShardedBGPRedis::getASes(){
    Redis *_redis;
    concurrent_vector<pair<string,string>> keys;
    cout<<"Begin Getting ASes"<<endl;
    tbb::parallel_for( tbb::blocked_range<unsigned long >(0,numShards),
                       [&](tbb::blocked_range<unsigned long> range)
                       {
        for (int i=range.begin(); i<range.end(); ++i){
            _redis=getRedis(i);
            _redis->hgetall("ASN", std::back_inserter(keys));
        }
                       });

    tbb::parallel_for( tbb::blocked_range<unsigned long >(0,keys.size()),
                       [&](tbb::blocked_range<unsigned long> range)
                       {
        SAS as;
        std::unordered_map<string, string> asMap;
        for (size_t i=range.begin(); i<range.end(); ++i) {
            try{
                asMap.clear();
                asMap["ASN"]=keys[i].first;
                asMap["STR"]=keys[i].second;
                as=std::make_shared<AS>(asMap);
                as->touch();
                cache->asCache.insert(make_pair(as->getNum(), as));
            } catch (const Error &err) {
                cout << err.what() << endl;
                // other errors
            }
        }
                       });
    cout<<"Finish Getting "<<keys.size()<<" ASes"<<endl;
    return;
}


BGPRedis::BGPRedis(BlockingCollection<BGPEvent *> *queue, Redis *_redis, int index):queue(queue), _redis(_redis), _index(index){}

BGPRedis:: ~BGPRedis(){}

void BGPRedis::run(){
    BGPEvent *event;
    bool cont=true;
    Pipeline pipe=_redis->pipeline();
#ifdef __linux
    string mess("BGPREDIS");
    mess += to_string(_index);
    prctl(PR_SET_NAME,mess.c_str());
#endif
    try {
        while(cont){
            queue->take(event);
            if (savingMode){
                switch (event->eventType) {
                    case NEWAS:{
                        string asNum=event->map["ASN"];
//                        event->map.erase("ASN");
//                        sem.acquire();
                        pipe.hset("ASN", event->map["ASN"],event->map["STR"]);
//                        pipe.hmset("ASN:"+asNum, event->map.begin(), event->map.end());
//                        pipe.sadd("ASES",asNum );
//                        sem.release();
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case NEWLINK:{
                        string linkstr=event->map["LID"];
                        event->map.erase("LID");
//                        sem.acquire();
                        pipe.hset("LINKS",linkstr,event->map["STR"]);
//                        pipe.hmset("LNK:"+linkstr, event->map.begin(), event->map.end());
//                        pipe.sadd("LINKS",linkstr);
//                        sem.release();
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case NEWPREFIX:{
 //                       sem.acquire();
                        pipe.zadd("PREFIXES", event->map["PFX"],event->timestamp*1.0);
//                        pipe.sadd("PREFIXES",event->map["PFX"]);
 //                       sem.release();
                        event->map.clear();
    //                    _redis->get(PREF:"+event->map["pfx"])
                        delete event;
                        break;
                    }
                    case NEWPATH:{
                        string hashStr=event->map["HSH"];
                        string pathHash=event->map["PSTR"];
                        pipe.hsetnx("PATHS",hashStr,event->map["STR"]);
                        pipe.hsetnx("PATH2ID",pathHash,hashStr);
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case LINKDROP:{
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case ASDROP:{
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case PATHACT:{
                        string hashStr=event->map["HSH"];
                        pipe.hset("PATHS",hashStr,event->map["STR"]);
                        pipe.zrem("INACTIVEPATHS",hashStr);
                        pipe.zadd("APATHS", hashStr ,event->timestamp*1.0);
//                        pipe.sadd("APATHS", hashStr);
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case PATHNACT:{
                        string hashStr=event->map["HSH"];
                        pipe.hset("PATHS",hashStr,event->map["STR"]);
                        pipe.zrem("APATHS",hashStr);
                        pipe.zadd("INACTIVEPATHS", hashStr, event->timestamp*1.0);
//                        pipe.srem("APATHS", hashStr);
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case PATHA:{
                        vector<string> routingEntries, results, type;
                        string entry,pathHashStr, peerStr, pfxID;
                        pipe.zrem("INACTIVEROUTING",event->map["pfxID"]+":"+event->map["peer"]);
                        pipe.zadd("ROUTINGENTRIES",event->map["pfxID"]+":"+event->map["peer"],event->timestamp*1.0);
                        pipe.lpush("PRE:"+event->map["pfxID"]+":"+event->map["peer"],{event->map["pathHash"]+ ":A:"+to_myencoding(event->timestamp)+":"+event->map["status"]});
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case ASPREFA:{
                        pipe.lpush("ASR:"+event->map["dstAS"],{event->map["pfxID"]+
                            ":A:"+to_myencoding(event->timestamp)});
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case ASPREFW:{
                        pipe.lpush("ASR:"+event->map["dstAS"],{event->map["pfxID"]+
                            ":W:"+to_myencoding(event->timestamp)});
                        event->map.clear();
                        delete event;
                        break;

                    }
                    case CAPTBEGIN:{
                        pipe.lpush("CAPT",to_myencoding(event->timestamp));
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case CAPTTIME:{
                        pipe.lpush("CAPT",to_myencoding(event->timestamp));
                        event->map.clear();
                        delete event;
                        break;

                    }
                    case ENDE:{
                        cout<<"END BGP REDIS " <<_index<<endl;
                        pipe.exec();
                        cont=false;
                        queue->add(event);
                        break;
                    }
                    case WITHDRAW:{
                        pipe.lpush("PRE:"+event->map["pfxID"]+":"+event->map["peer"],{":W:"+to_myencoding(event->timestamp)+":"});
                        pipe.zadd("INACTIVEROUTINGENTRIES", event->map["pfxID"]+":"+event->map["peer"], event->timestamp*1.0);
                        pipe.zrem("ROUTINGENTRIES",event->map["pfxID"]+":"+event->map["peer"]);
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case TRIM:{
                        int index=from_myencoding(event->map["Indx"]);
                        string key="PRE:"+event->map["pfxID"]+":"+event->map["peer"];
                        _redis->ltrim(key, 0, index);
                        break;
                    }
                    case ASUPD:{
                        string asNum=event->map["ASN"];
//                        event->map.erase("ASN");
//                        pipe.hmset("ASN:"+asNum, event->map.begin(), event->map.end());
                        pipe.hset("ASN", event->map["ASN"],event->map["STR"]);
                        event->map.clear();
                        delete event;
                        break;
                        
                    }
                    case LNKUPD:{
                        string linkstr=event->map["LID"];
                        event->map.erase("LID");
                        pipe.hset("LINKS",linkstr, event->map["STR"]);
                        event->map.clear();
                        delete event;
                        break;
                    }
                    case PTHUPD:{
                        string hashStr=event->map["HSH"];
                        pipe.hset("PATHS",hashStr,event->map["STR"]);
                        pipe.zadd("APATHS", hashStr, event->timestamp*1.0);
                        event->map.clear();
                        delete event;
                        break;
                    }

                    case COMMADD:{
                        string asStr=event->map["ASN"];
                        string str=event->map["STR"];
                        asStr="CO"+asStr;
                        pipe.sadd(asStr,str);
                        event->map.clear();
                        delete event;
                        break;
                    }
                        
                    default:
                        event->map.clear();
                        delete event;
                        break;
                }
                if (cnt%100==0){
                    pipe.exec();
                }
            } else {
                event->map.clear();
                delete event;
            }
            cnt++;
        }
    } catch (const ReplyError &err) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        cout << err.what() << endl;
    }    catch (const TimeoutError &err) {
        // reading or writing timeout
        cout << err.what() << endl;
    } catch (const ClosedError &err) {
        // the connection has been closed.
        cout << err.what() << endl;
    } catch (const IoError &err) {
        cout << err.what() << endl;
        // there's an IO error on the connection.
    } catch (const Error &err) {
        cout << err.what() << endl;
        // other errors
    }
    return;
}

void BGPRedis::setSavingMode(){
    savingMode = true;
}

void BGPRedis::resetSavingMode(){
    savingMode= false;
}





