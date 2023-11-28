//
// Created by Kave Salamatian on 2018-12-03.
//
#include <algorithm>
#include <random>
#include "BGPTables.h"
#include "BGPGeopolitics.h"
#include "BGPSource.h"
#include "cache.h"
#include "tbb/tbb.h"
#include <boost/algorithm/string.hpp>
#ifdef __linux
    #include <sys/prctl.h>
#endif

using namespace tbb;
using namespace std;

struct bgpstream_patricia_node {
    /* flag if this node used */
    u_int bit;
    
    /* who we are in patricia tree */
    bgpstream_pfx_t prefix;
    
    /* left and right children */
    bgpstream_patricia_node_t *l;
    bgpstream_patricia_node_t *r;
    
    /* parent node */
    bgpstream_patricia_node_t *parent;
    
    /* pointer to user data */
    void *user;
};


struct bgpstream_patricia_tree {
    
    /* IPv4 tree */
    bgpstream_patricia_node_t *head4;
    
    /* IPv6 tree */
    bgpstream_patricia_node_t *head6;
    
    /* Number of nodes per tree */
    uint64_t ipv4_active_nodes;
    uint64_t ipv6_active_nodes;
    
    /** Pointer to a function that destroys the user structure
     *  in the bgpstream_patricia_node_t structure */
    bgpstream_patricia_tree_destroy_user_t *node_user_destructor;
};

struct pathComp {
    bool operator() (const SPrefixPath lhs, const SPrefixPath rhs) const {
        if (lhs->getPeer() == rhs->getPeer()){
            return false;
        }
        if (lhs->getScore()>rhs->getScore()){
            return false;
        } else {
            if (lhs->getScore()<rhs->getScore()){
                return true;
            } else {
                for (int i=0; i<lhs->shortPathLength;i++){
                    if (lhs->shortPath[i]<rhs->shortPath[i])
                        return true;
                }
                return false;
            }
        }
    }
};


RIBTable::RIBTable(unsigned int time, unsigned int duration): basetime(time), windowtime(time), duration(duration){
    ribTrie = new Trie();
}

BGPMessage *RIBTable::update(BGPMessage *bgpMessage){
    bgpstream_pfx_t *pfx;
    Peer* peer;
    SAS dest;
    bool globalOutage=false;
    char collectorId =cache->collectors.find(bgpMessage->collector)->second;
    SPrefixPath path;
    unsigned int time, pathHash;
    BGPEvent *event;
    bool pathAdded=false, pathwithdrawn=false;
    Category cat;
    
    peer=bgpMessage->peer;
    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    pfx=  (bgpstream_pfx_t *)&bgpMessage->pfx;
    peer = bgpMessage->peer;
    time = bgpMessage->timestamp;
    RIBElement *ribElement = (RIBElement *)bgpMessage->trieElement;
    switch(bgpMessage->type) {
        case BGPSTREAM_ELEM_TYPE_RIB:{
            if (!bgpMessage->setPath(time)){//check if the path is in RAM or Redis. After the check the path is anyway in RAM cache and in Redis
                //the path is new
                cache->numActivePath++;
                cache->pathsBF.insert(bgpMessage->prefixPath->str());
            }
            path = bgpMessage->prefixPath;
            pathHash = bgpMessage->pathHash;
            cat= ribElement->addRIBPath(path, time);
            bgpMessage->dest->update(pfx, time);
            break;
        }
        case BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT:{
            if(!bgpMessage->setPath(time)){
                //the path is new
                cache->numActivePath++;
                cache->pathsBF.insert(bgpMessage->prefixPath->str());
            }
            path = bgpMessage->prefixPath;
            pathHash =bgpMessage->pathHash;
            cat= ribElement->addPath(path, time);
            switch(cat){
                case AADup:{
                    ribElement->AADup++;
                    bgpMessage->category = AADup;
                    break;
                }
                case AADiff: {
                    ribElement->AADiff++;
                    pathAdded = true;
                    pathwithdrawn = true;// PreviousPath != NULL it is an implicit withdraw
                    bgpMessage->category = AADiff;
 //                   cache->routingFilter.insert(bgpMessage->pfxStr+":"+to_string(path->getPeer()));
                    break;
                }
                case None:{
                    pathAdded = true;
                    bgpMessage->dest->update(pfx,time);
                    bgpMessage->category=None;
      //              cache->routingFilter.insert(bgpMessage->pfxStr+":"+to_string(path->getPeer()));
                    break;
                }
                default:{
                    break;
                }
            }
            break;
        }
        case BGPSTREAM_ELEM_TYPE_WITHDRAWAL: {
            auto ret = ribElement->erasePath(collectorId,peer->getAsn(), time);
            globalOutage = ret.first;
            switch(ret.second){
                case WWDup: {
                    ribElement->WWDup++;
                    bgpMessage->category = WWDup;
                    break;
                }
                case Withdrawn: {
                    ribElement->Withdraw++;
                    pathwithdrawn = true; // PreviousPath != NULL it is an implicit withdraw
                    bgpMessage->category = Withdrawn; // implicit withdrawal and replacement with different
                    event= new BGPEvent(time,WITHDRAW);
                    event->map["pfxID"]=to_myencodingPref(&bgpMessage->pfx);
                    event->map["peer"]=to_myencoding(peer->getAsn());
                    event->hash= std::hash<std::string>{}(event->map["pfxID"]+":"+ event->map["peer"]);
                    event->timestamp=bgpMessage->timestamp;
                    cache->bgpRedis->add(event);
                    break;
                }
                default:{
                    break;
                }
            }
            if (globalOutage){
                    //There is a global prefix withdraw
            //        for (auto it= trieElement->asSet.begin(); it != trieElement->asSet.end();it++){
            //            if((*it)->activePrefixTrie->search(pfx).second != NULL){
            //                cout<<"ERROR"<<endl;
            //            }
            //        }

            }
            break;
        }
        default: {
            break;
        }
    }
    return bgpMessage;
    //updateEventTable(bgpMessage);
}



void RIBTable::save(BGPGraph* g, unsigned int time, unsigned int dumpDuration){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    vector<pair<unsigned int, Link *>> linkVect;
//    cout<<"FULL FEED PEERS"<<endl;
//    for(auto p:cache->peersMap){
//        if (p.second->isFullFeed){
//            cout<<p.second->asNum<<endl;
//        }
//    }
    windowtime +=duration;
    cache->makeGraph(g, time, dumpDuration);
}

long RIBTable::size_of(){
    long size=4+4*8;
    return size;
}


RIBElement::RIBElement(bgpstream_pfx_t *inpfx) {
    bgpstream_pfx_copy(&pfx, inpfx);
    pfxStr=to_myencodingPref(&pfx);
}

SPrefixPath RIBElement::getPath(unsigned int hash, unsigned int peer, unsigned int timestamp) {
    string hashStr=to_myencoding(hash);
    unsigned int hash1=std::hash<std::string>{}(hashStr);
    Redis *_redis=cache->bgpRedis->getRedis(hash1);
    auto str = _redis->hget("PATHS", hashStr);
    if (str) {
        cache->pathsMap.idCacheMissed();
        std::unordered_map<string, string> pathMap;
        SPrefixPath path = std::make_shared<PrefixPath>(*str);
        auto ret=cache->pathsMap.insert(hash, path, timestamp);
        if(ret.first){
            return ret.second;
        }
    }
    return NULL;
}


pair<bool,SRoutingEntry> RIBElement::getRoutingEntry(bgpstream_pfx_t *pfx,unsigned int peer){
    string pfxStr=to_myencodingPref(pfx);
    string peerStr= to_myencoding(peer);
    string str=pfxStr+":"+peerStr;
    SRoutingEntry r;
    if(cache->routingBF.contains(str)){
        vector<string> vec, results(3);
        unsigned int hash=std::hash<std::string>{}(str);
        Redis *_redis=cache->bgpRedis->getRedis(hash);
        _redis->lrange("PRE:"+str,-1,-1, std::back_inserter(vec));
        if (vec.size()>0){
            cache->routingentries.cacheMissed();
            boost::split(results, vec[0], [](char c){return c == ':';});
            if (results[1]=="A") {
                r=std::make_shared<RoutingEntry>(from_myencoding(results[0]),static_cast<Category>(from_myencoding(results[3])),from_myencoding(results[2]));
            } else {
                r=std::make_shared<RoutingEntry>(from_myencoding(results[0]),Category::Withdrawn,from_myencoding(results[2]));
            }
            
            cache->routingBF.insert(str);
            auto p=cache->routingentries.insert(str,r);
            return make_pair(true, p.second);
        }
    }
    return make_pair(false, r);
}


Category RIBElement::addRIBPath(SPrefixPath prefixPath, unsigned int time){
    BGPEvent *event;
    unsigned int previousHash;
    ThreadSafeScalableCache<string, SRoutingEntry>::Accessor accessor;
    Category status;
    SRoutingEntry r;
    unsigned int lastChange;
    SPrefixPath previous=NULL;
    char collector=prefixPath->collector;
    unsigned int peer=prefixPath->getPeer(), pathHash=prefixPath->hash;
    
    if (collectorsSet.insert(collector)){
        //new collector
        visibleCollectorsNum++;
    }
    OutageCollectors.erase(collector);
    if (addAS(prefixPath->getDest())){
        cache->asCache[prefixPath->getDest()]->update(&pfx,time);
    }
    string str=pfxStr+":"+to_myencoding(peer);
    visiblePeerNum++;
    cache->routingBF.insert(str);
    r=std::make_shared<RoutingEntry>(pathHash,Category::None,time);
    auto p=cache->routingentries.insert(str,r);
    prefixPath->addPrefix(time);
    if (!cache->peersMap.find(peer).first){
        Peer *peerP = new Peer(peer);
        auto res=cache->peersMap.insert(make_pair(peer,peerP));
        if (!res.first){
            delete peerP;
        }
    }
    previousHash=r->pathHash;
    lastChange=r->lastChange;
    r->pathHash=pathHash;
    r->lastChange=time;
    cache->peersMap[peer]->addPref();
    r->status=Category::None;
    event = new BGPEvent(time, PATHA);
    event->map["T"]=to_string(time);
    event->map["peer"] = to_myencoding(peer);
    event->map["pfxID"] = to_myencodingPref(&pfx);
    event->map["pathHash"]=to_myencoding(pathHash);
    event->map["status"]=to_myencoding(status);
    event->hash= std::hash<std::string>{}(event->map["pfxID"]+":"+ event->map["peer"]);
    event->timestamp=time;
    cache->bgpRedis->add(event);
    cTime= time;
    return r->status;
}


Category RIBElement::addPath(SPrefixPath prefixPath, unsigned int time){
    BGPEvent *event;
    unsigned int previousHash;
    ThreadSafeScalableCache<string, SRoutingEntry>::Accessor accessor;
    Category status;
    SRoutingEntry r;
    unsigned int lastChange;
    SPrefixPath previous=NULL;
    char collector=prefixPath->collector;
    unsigned int peer=prefixPath->getPeer(), pathHash=prefixPath->hash;
    
    if (collectorsSet.insert(collector)){
        //new collector
        visibleCollectorsNum++;
    }
    OutageCollectors.erase(collector);
    if (addAS(prefixPath->getDest())){
//        checkHijack(prefixPath->dest);
//TODO checkHijack
        cache->asCache[prefixPath->getDest()]->update(&pfx,time);
    }
    string str=pfxStr+":"+to_myencoding(peer);
    if (!cache->routingentries.find(accessor,str)){
        // CheckRedis
        auto ret=getRoutingEntry(&pfx, peer);
        if (!ret.first){
            //New visible peer
            visiblePeerNum++;
            cache->routingBF.insert(str);
            r=std::make_shared<RoutingEntry>(pathHash,Category::None,time);
            auto p=cache->routingentries.insert(str,r);
            if (p.first) {
                prefixPath->addPrefix(time);
                if (!cache->peersMap.find(peer).first){
                    Peer *peerP = new Peer(peer);
                    auto res=cache->peersMap.insert(make_pair(peer,peerP));
                    if (!res.first){
                        delete peerP;
                    }
                }
            }
        } else {
            r=ret.second;
        }
    } else {
        // the routing entry is in the cache
        // the peer has already a path!
        r=*accessor;
    }
    previousHash=r->pathHash;
    status=static_cast<Category>(r->status);
    lastChange=r->lastChange;
    r->pathHash=pathHash;
    r->lastChange=time;
    if (!cache->peersMap.find(peer).first){
        Peer *peerP = new Peer(peer);
        auto res=cache->peersMap.insert(make_pair(peer,peerP));
        if (!res.first){
            delete peerP;
        }
    }
    cache->peersMap[peer]->addPref();
    if ((status=Withdrawn) || (status==IWithdrawn)) {
        //New visible peer
        visiblePeerNum++;
        //check for Flap
        if (previousHash==pathHash){
            r->status=Category::None;
            if (time-lastChange<300){
                // if less than 5 mins between the two change it is a flap
                r->status=Category::Flap;
            }
        } else {
            r->status=Category::None;
        }
    } else {
        //this an implicit withdraw or a AADUP.
        if (previousHash==pathHash){
            r->status=Category::AADup;
        } else {
            r->status=Category::AADiff;
        }
    }
    if (r->status != AADup){
        auto p=cache->pathsMap.find(previousHash);
        if (p.first){
            //the path is in the cache memory
            previous=p.second;
        } else {
            // The previous Path for the peer is not in cache memory ask from redis
            previous=getPath(previousHash,peer, time);
        }
        if (previous){
            //the previous path exists
            prefixPath->addPrefix(time);
        } //this not possible that the previous path does not exist in this situation as its hash exists
    }
    event = new BGPEvent(time, PATHA);
    event->map["T"]=to_string(time);
    event->map["peer"] = to_myencoding(peer);
    event->map["pfxID"] = to_myencodingPref(&pfx);
    event->map["pathHash"]=to_myencoding(pathHash);
    event->map["status"]=to_myencoding(status);
    event->hash= std::hash<std::string>{}(event->map["pfxID"]+":"+ event->map["peer"]);
    event->timestamp=time;
    cache->bgpRedis->add(event);
    cTime= time;
    return r->status;
}

pair<bool, Category> RIBElement::erasePath(char collector, unsigned int peer, unsigned int time){

    ThreadSafeScalableCache<string,SRoutingEntry>::Accessor accessor;
    SRoutingEntry r;
    // We have to remove all paths in the peer
    string str=pfxStr+":"+to_myencoding(peer);
    if(cache->routingentries.find(accessor,str)){ // the routing entry is in the RAM
        r=*accessor;
    } else {
        auto ret=getRoutingEntry(&pfx, peer); //check in Redis
        if (!ret.first){
            //Not exist or already withdrawn
            return make_pair(false, Category::WWDup);
        } else {
            r=ret.second;
        }
    }
    if (r->status==Category::Withdrawn){
        //already withdrawn
        return make_pair(false, Category::WWDup);
    }
    cTime = time;
    r->status=Category::Withdrawn;
    visiblePeerNum--;
    if (checkGlobalOutage(time)) {
        globalOutage= true;
        return make_pair(true, Category::Withdrawn);
    } else {
        return make_pair(false, Category::Withdrawn);
    }
}

bool RIBElement::checkGlobalOutage(unsigned int time){
    if (visibleCollectorsNum==0){
        for(auto as:asSet)
            cache->asCache[as]->withdraw(&pfx, time);
        return true;
    } else{
        return false;
    }
}

bool RIBElement::addAS(unsigned int asn){
//    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    return asSet.insert(asn);
}

string RIBElement::str(){
    string pfxStr;
    char buffer[20];
    bgpstream_pfx_snprintf(buffer, 20, &pfx);
    pfxStr=string(buffer);
    return pfxStr;
}

long RIBElement::size_of(){
    long sum=0;
    return sum;
}


bgpstream_patricia_walk_cb_result_t pathProcess(const bgpstream_patricia_tree_t *pt,const bgpstream_patricia_node_t *node, void *data){
    RIBElement *rib=(RIBElement *)node->user;
    long *sum=(long *)data;
    *sum +=rib->size_of();
    return BGPSTREAM_PATRICIA_WALK_CONTINUE;
    
/*    SPrefixPath path=(SPrefixPath )data;
    unsigned int src,dst,tmp;
    string linkId;
    if ((node->prefix.address.version != BGPSTREAM_ADDR_VERSION_UNKNOWN)){
        path->dest->activePrefixTrie->insert(&node->prefix,NULL);
        for (int i=0;i<path->shortPathLength-1;i++){
            src = path->shortPath[i];
            dst = path->shortPath[i+1];
            if (src>dst){
                tmp = src;
                src = dst;
                dst = tmp;
            }
            linkId=to_string(src) + "|" + to_string(dst);
            link=cache->linksMap.find(linkId).second;
            
            path->linkVect[i]->prefixTrie->insert(&node->prefix, NULL);
        }
    }*/
}

Trie::Trie(){
    /* Create a Patricia Tree */
    pt = bgpstream_patricia_tree_create(NULL);
}

bool Trie::insert(bgpstream_pfx_t *pfx, void *data){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    long prevCount=prefixNum();
    bgpstream_patricia_node_t *node = bgpstream_patricia_tree_insert(pt,pfx);
    long nextCount=prefixNum();
    bgpstream_patricia_tree_set_user(pt, node, data);
    return (nextCount == prevCount);
}

pair<bool, void*> Trie::search(bgpstream_pfx_t *pfx){
    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    bgpstream_patricia_node_t *node = bgpstream_patricia_tree_search_exact(pt, pfx);
    if (node == NULL) {
        return make_pair(false,(void*) NULL);
    } else {
        return make_pair(true, bgpstream_patricia_tree_get_user(node));
    }
}

pair<bool, void*> Trie::checkinsert(bgpstream_pfx_t *pfx){
    boost::upgrade_lock<boost::upgrade_mutex> lock(mutex_);
    RIBElement *trieElement;
    bgpstream_patricia_node_t *node = bgpstream_patricia_tree_search_exact(pt, pfx);
    if (node ==NULL){
        trieElement= new RIBElement(pfx);
//        trieElement = NULL;
        boost::upgrade_to_unique_lock<boost::shared_mutex> writeLock(lock);
        bgpstream_patricia_node_t *node = bgpstream_patricia_tree_insert(pt,pfx);
        bgpstream_patricia_tree_set_user(pt, node, trieElement);
        return make_pair(true,trieElement);
    }
    return make_pair(false,bgpstream_patricia_tree_get_user(node));
}

bool Trie::remove(bgpstream_pfx_t *pfx){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    bgpstream_patricia_node_t *node;
    if ((node=bgpstream_patricia_tree_search_exact(pt, pfx))!=NULL){
        bgpstream_patricia_tree_remove_node(pt, node);
        return true;
    } else
        return false;
}

long Trie::prefixNum(){
    return bgpstream_patricia_prefix_count(pt, BGPSTREAM_ADDR_VERSION_IPV4)+bgpstream_patricia_prefix_count(pt, BGPSTREAM_ADDR_VERSION_IPV6);
}
long Trie::prefix24Num(){
    return bgpstream_patricia_tree_count_24subnets(pt)+bgpstream_patricia_tree_count_64subnets(pt);
}

void Trie::savePrefixes(SPrefixPath prefixPath){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
//    bgpstream_patricia_tree_walk(pt,pathProcess , prefixPath);
}
    

void Trie::clear(){
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    bgpstream_patricia_tree_clear(pt);
}

long Trie::size_of(){
    long sum=0;
    bgpstream_patricia_tree_walk(pt,pathProcess, &sum);
    return sum;
}

TableFlagger::TableFlagger(int numThread, BlockingCollection<BGPMessage *> &outfifo, RIBTable *bgpTable, BGPSource *bgpSource, int version):numThread(numThread), version(version),outfifo(outfifo), bgpTable(bgpTable),bgpSource(bgpSource), processQueues(numThread),processThreads(numThread){
    BGPProcessor *bgpProcessor;
    for(int i=0;i<numThread;i++){
        processQueues[i]=std::make_shared<PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>>>(10000);
        bgpProcessor=new BGPProcessor(processQueues[i], outfifo, bgpTable, bgpSource, version);
        processThreads[i]=std::thread(&BGPProcessor::run,bgpProcessor);

    }
}

void TableFlagger::stop(){
    BGPMessage *bgpMessage = new BGPMessage(0);
    bgpMessage->category = STOP;
    for (auto queue:processQueues){
        queue->add(bgpMessage);
    }
}

void TableFlagger::join(){
    for (int i=0;i<numThread;i++){
        processThreads[i].join();
    }
}


void TableFlagger::add(BGPMessage *message){
    unsigned long hash=bgpstream_pfx_hash(&message->pfx);
    processQueues[hash%numThread]->add(message);
}

long TableFlagger::qSize(){
    long sum=0;
    for(int i=0;i<numThread;i++){
        sum +=processQueues[i]->size();
    }
    return sum;
}


BGPProcessor::BGPProcessor(BGPProcessorQueue &infifo, BlockingCollection<BGPMessage *> &outfifo, RIBTable *bgpTable, BGPSource *bgpSource, int version):infifo(infifo),outfifo(outfifo), bgpTable(bgpTable), bgpSource(bgpSource), version(version){}

void BGPProcessor::run(){
    BGPMessage *bgpMessage;
    RIBElement *trieElement;
    BGPEvent *event;
    bgpstream_pfx_t *pfx;
    pair<bool, void *> ret;
#ifdef __linux
    prctl(PR_SET_NAME,"TABLEFLAGGER");
#endif
    while(true){
        if (infifo->try_take(bgpMessage, std::chrono::milliseconds(1200000))==BlockingCollectionStatus::TimedOut){
            cout<< "Data Famine Table"<<endl;
            break;
        } else {
            if (bgpMessage->category != STOP)  {
                pfx = (bgpstream_pfx_t *)&(bgpMessage->pfx);
                ret =bgpTable->ribTrie->checkinsert(pfx);
                if (ret.first){
                    trieElement = (RIBElement *) ret.second;
                    event = new BGPEvent(bgpMessage->timestamp, NEWPREFIX);
                    event->map["PFX"]=bgpMessage->pfxStr;
                    event->map["PID"]=to_myencodingPref(&bgpMessage->pfx);
                    event->hash= std::hash<std::string>{}(event->map["PFX"]);
                    event->timestamp=bgpMessage->timestamp;
                    cache->bgpRedis->add(event);
                } else {
                    trieElement=(RIBElement *) ret.second;
                }
                bgpMessage->trieElement = trieElement;
                bgpMessage = bgpTable->update(bgpMessage);
                bgpSource->bgpMessagePool->returnBGPMessage(bgpMessage);
                outfifo.add(bgpMessage);
            } else {
                infifo->add(bgpMessage);
                cache->bgpRedis->end(bgpMessage->timestamp);
                outfifo.add(bgpMessage);
                SBGPAPI data= new BGPAPI(NULL,0);
                cache->toAPIbgpbiew.add(data);
                break;
            }
        }
    }
    cout<< "Table Processing end"<<endl;
}

