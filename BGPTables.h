//
// Created by Kave Salamatian on 2018-12-01.
//

#ifndef BGPGEOPOLITICS_BGPTABLES_H
#define BGPGEOPOLITICS_BGPTABLES_H

#include "cache.h"
#include "BGPGeopolitics.h"
#include "BlockingQueue.h"
#include "BGPEvent.h"
#include "bgpstream_utils_patricia.h"
//#include "cache.h"
#include <sw/redis++/redis++.h>
#include <map>



class BGPSource;
//class semaphore;
using namespace boost;

extern BGPCache *cache;

class RIBElement{
protected:
//    boost::shared_mutex mutex_;
private:
//    MyThreadSafeMap<char, RIBCollectorElement*> collectors;
    MyThreadSafeSet<char> collectorsSet;
    concurrent_unordered_map<string, unsigned int> routingEntries;
    MyThreadSafeSet<char> OutageCollectors;
    MyThreadSafeSet<unsigned int> asSet;
    unsigned int cTime;
    bool globalOutage=true;
    int visibleCollectorsNum=0;
    int visiblePeerNum=0;
    bgpstream_pfx_t pfx;
    string pfxStr;
    bool hijack= false;
public:
    RIBElement(bgpstream_pfx_t *inpfx);
    Category addPath(SPrefixPath prefixPath, unsigned int time);
    Category addRIBPath(SPrefixPath prefixPath, unsigned int time);
    pair<bool, Category> erasePath(char collector,  unsigned int peer, unsigned int time);
    SPrefixPath getPath(unsigned int hash, unsigned int peer, unsigned int timestamp);
    pair<bool,SRoutingEntry> getRoutingEntry(bgpstream_pfx_t *pfx,unsigned int peer);
    bool addAS(unsigned int asn);
    bool checkGlobalOutage(unsigned int time);
    bool removeAS(unsigned int asn);
    bool checkHijack(unsigned int ans);
    long size_of();
    string str();
    int AADiff=0, AADup=0, WADup=0, WWDup=0, Flap=0, Withdraw=0;
};


class Trie{
private:
    mutable boost::shared_mutex mutex_;
public:
    bgpstream_patricia_tree_t *pt;
    Trie();
    bool insert(bgpstream_pfx_t *pfx, void *data);
    pair<bool, void*> search(bgpstream_pfx_t *pfx);
    pair<bool, void*> checkinsert(bgpstream_pfx_t *pfx);
    bool remove(bgpstream_pfx_t *pfx);
    void save();
    long prefixNum();
    long prefix24Num();
    void savePrefixes(SPrefixPath prefixPath);
    void clear();
    long size_of();
};

class BGPMessage;
class BGPCache;

class RIBTable{
public:
    mutable boost::shared_mutex mutex_;
    Trie *ribTrie;
    unsigned int basetime;
    unsigned int windowtime;
    unsigned int duration;
    RIBTable(unsigned int time, unsigned int duration);
    BGPMessage *update(BGPMessage *bgpMessage);
    long size_of();
    void clear();
    void save(BGPGraph* g, unsigned int time, unsigned int dumpDuration);
};

typedef std::shared_ptr<PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>>> BGPProcessorQueue;

class BGPProcessor{
public:
    BGPProcessor(BGPProcessorQueue &infifo, BlockingCollection<BGPMessage *> &outfifo, RIBTable *bgpTable, BGPSource *bgpSource, int version);
    void run();
private:
    BGPSource *bgpSource;
    BGPProcessorQueue &infifo;
    BlockingCollection<BGPMessage *> &outfifo;
    RIBTable *bgpTable;
    int version;
};

class BGPMessageComparer;
class TableFlagger{
public:
    TableFlagger(int numThread, BlockingCollection<BGPMessage *> &outfifo, RIBTable *bgpTable, BGPSource *bgpSource, int version);
    void add(BGPMessage *message);
    void join();
    long qSize();
    void stop();
private:
    int numThread, version;
    vector<BGPProcessorQueue> processQueues;
    vector<std::thread> processThreads;
    BlockingCollection<BGPMessage *> &outfifo;
    RIBTable *bgpTable;
    BGPSource *bgpSource;
};

#endif //BGPGEOPOLITICS_BGPTABLES_H
