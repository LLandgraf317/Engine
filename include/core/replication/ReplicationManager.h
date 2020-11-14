#pragma once

#include <core/access/RootManager.h>
#include <core/access/NVMStorageManager.h>
#include <core/storage/PersistentColumn.h>
#include <core/storage/column_gen.h>
#include <core/storage/column.h>

#include <core/index/index_gen.h>
#include <core/index/IndexDef.h>
#include <core/tracing/trace.h>
#include <core/operators/scalar/group_uncompr.h>

#include <libpmempool.h>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <numaif.h>

namespace morphstore {

enum DataStructure {
    PCOLUMN,
    VCOLUMN,

    PTREE,
    VTREE,

    PSKIPLIST,
    VSKIPLIST,

    PHASHMAP,
    VHASHMAP,

    CLPTREE,
    CLVTREE,
    CLPSKIPLIST,
    CLVSKIPLIST,
    CLPHASHMAP,
    CLVHASHMAP

};


class ReplTuple {
public:
    pptr<void> m_PPtr;
    void* m_VPtr;

    DataStructure m_Kind;
    uint64_t m_NumaNode;

    ReplTuple(persistent_ptr<void> ptr, DataStructure ds, uint64_t node)
    {
        m_PPtr = ptr;
        m_Kind = ds;
        m_NumaNode = node;
    }

    ReplTuple(void* ptr, DataStructure ds, uint64_t node) {
        switch (ds) {
            default:
                m_VPtr = ptr;
                break;
        };

        m_Kind = ds;
        m_NumaNode = node;
    }

    bool isIndex()
    {
        switch (m_Kind) {
            case PTREE:
            case PSKIPLIST:
            case PHASHMAP:
            case VSKIPLIST:
            case VTREE:
            case VHASHMAP:
                return true;
            default:
                return false;
        };
    }

    void destruct()
    {
        switch (m_Kind) {
            case PTREE: {
                auto ptr = static_cast<persistent_ptr<MultiValTreeIndex>>(m_PPtr);
                ptr->prepareDest();
                delete_persistent<MultiValTreeIndex>(ptr);
                break;
            }
            case CLPTREE: {
                auto ptr = static_cast<persistent_ptr<MultiValTreeIndex>>(m_PPtr);
                ptr->prepareDest();
                delete_persistent<MultiValTreeIndex>(ptr);
                break;
            }
            case PSKIPLIST: {
                auto ptr = static_cast<persistent_ptr<SkipListIndex>>(m_PPtr);
                ptr->prepareDest();
                delete_persistent<SkipListIndex>(ptr);
                break;
            }
            case CLPSKIPLIST: {
                auto ptr = static_cast<persistent_ptr<SkipListIndex>>(m_PPtr);
                ptr->prepareDest();
                delete_persistent<SkipListIndex>(ptr);
                break;
            }
            case PHASHMAP: {
                auto ptr = static_cast<persistent_ptr<HashMapIndex>>(m_PPtr);
                ptr->prepareDest();
                delete_persistent<HashMapIndex>(ptr);
                break;
            }
            case CLPHASHMAP: {
                auto ptr = static_cast<persistent_ptr<CLHashMapIndex>>(m_PPtr);
                ptr->prepareDest();
                delete_persistent<CLHashMapIndex>(ptr);
                break;
            }
            case PCOLUMN: {
                auto ptr = static_cast<persistent_ptr<PersistentColumn>>(m_PPtr);
                ptr->prepareDest();
                delete_persistent<PersistentColumn>(ptr);
            }
            case VSKIPLIST:
            case VTREE:
            case VCOLUMN:
            case VHASHMAP: {
                //TODO: check for all allocations of columns and check whether malloc or numa alloc is used
                break;
            }
            default:
                break;
        };
    }
};

class ReplicationStatus {
    std::string m_Relation;
    std::string m_Table;
    std::string m_Attribute;

    std::vector<ReplTuple> replication;

    //ReplicationStatus(ReplicationStatus const&) = delete;
    //void operator=(ReplicationStatus const&) = delete;

public:
    ReplicationStatus(std::string relation, std::string table, std::string attribute)
        : m_Relation(relation), m_Table(table), m_Attribute(attribute)
    {
    }

    ~ReplicationStatus()
    {
        trace_l(T_DEBUG, "Destroying replication status ", m_Relation, ", ", m_Table, ", ", m_Attribute);
    }

    std::string getRelation() { return m_Relation; }

    std::string getTable() { return m_Table; }

    std::string getAttribute() { return m_Attribute; }

    bool compare(std::string relation, std::string table, std::string attribute)
    {
        /*trace_l(T_DEBUG, "Comparing ", relation, ", ", table, ", ", attribute);
        trace_l(T_DEBUG, "and ", m_Relation, ", ", m_Table, ", ", m_Attribute);
        trace_l(T_DEBUG, "Results: ", relation.compare(m_Relation), ", ", table.compare(m_Table), ", ", attribute.compare(m_Attribute));*/

        return relation.compare(m_Relation) == 0 && table.compare(m_Table) == 0 && attribute.compare(m_Attribute) == 0;
    }

    bool contains(DataStructure kind)
    {
        for (auto i : replication)
            if (i.m_Kind == kind)
                return true;

        return false;
    }

    using VColumn = morphstore::column<uncompr_f>;

#define PGET(index_structure, structure_enum) \
    persistent_ptr<index_structure> get##index_structure(size_t numa_node) \
    { \
        for (auto i : replication) \
            if (i.m_Kind == structure_enum && i.m_NumaNode == numa_node) { \
                return static_cast<persistent_ptr<index_structure>>(i.m_PPtr); \
            } \
        return nullptr; \
    } \

#define VGET(index_structure, structure_enum) \
    index_structure* get##index_structure(size_t numa_node) \
    { \
        for (auto i : replication) \
            if (i.m_Kind == structure_enum && i.m_NumaNode == numa_node) { \
                return reinterpret_cast<index_structure*>(i.m_VPtr); \
            } \
        return nullptr; \
    } 

    VGET(VColumn, VCOLUMN)

    PGET(MultiValTreeIndex, PTREE);
    PGET(HashMapIndex, PHASHMAP);
    PGET(SkipListIndex, PSKIPLIST);
    PGET(PersistentColumn, PCOLUMN);

    PGET(CLTreeIndex, CLPTREE);
    PGET(CLHashMapIndex, CLPHASHMAP);
    PGET(CLSkipListIndex, CLPSKIPLIST);

    bool containsIndex()
    {
        for (auto i : replication) {
            if (i.isIndex())
                return true;
        }
        return false;
    }
    

    template<typename t_index_structure_ptr>
    void add(t_index_structure_ptr ptr, DataStructure kind, size_t pmemNode)
    {
        //trace_l(T_INFO, "Adding data structure of kind ", kind, " on node ", pmemNode, " concerning r: ", ptr->getRelation(), ", t: ", ptr->getTable(), ", a: ", ptr->getAttribute());
        replication.emplace_back(ptr, kind, pmemNode);
    }

    void destructAll()
    {
        for (auto i : replication)
            i.destruct();
    }

};


template<class index_structure_ptr>
struct ReplCreateIndexArgs {
    uint64_t node;
    index_structure_ptr index;
    pptr<PersistentColumn> valCol;
};

struct repl_thread_info {
    pthread_t thread_id;
    int thread_num;
};

// Threading infrastructure of replication manager
template< class index_structure_ptr, size_t bucket_size>
void * generateIndex( void * argPtr )
{
    ReplCreateIndexArgs<index_structure_ptr> * indexArgs = (ReplCreateIndexArgs<index_structure_ptr>*) argPtr;

    numa_run_on_node(indexArgs->node);
    IndexGen::generateFast<index_structure_ptr, bucket_size>(indexArgs->index, indexArgs->valCol);

    RootManager& root_mgr = RootManager::getInstance();
    root_mgr.drainAll();

    free(argPtr);

    return nullptr;
}

void * generateVColumn( void * argPtr )
{
    ReplCreateIndexArgs<column<uncompr_f>*> * indexArgs = (ReplCreateIndexArgs<column<uncompr_f>*>*) argPtr;

    numa_run_on_node(indexArgs->node);
    copy_pers_column_to_vol(indexArgs->index, indexArgs->valCol, indexArgs->node);

    free(argPtr);
    return nullptr;
}

class ReplicationManager {
private:
    uint64_t m_NumaNodeCount;
    std::vector<ReplicationStatus> state;

    std::list<repl_thread_info*> thread_infos;
    pobj_alloc_class_desc alloc_class;
    bool m_Wait = false;

    ReplicationManager() {}

public:
    ReplicationManager(ReplicationManager const&)               = delete;
    void operator=(ReplicationManager const&)  = delete;

    static ReplicationManager& getInstance()
    {
        static ReplicationManager instance;

        return instance;
    }

    void setWait(bool val)
    {
        m_Wait = val;
    }

    ~ReplicationManager()
    {
        trace_l(T_DEBUG, "Destroying ReplicationManager");
    }

    void joinAllThreads()
    {
        if (!m_Wait)
            while (thread_infos.begin() != thread_infos.end()) {
                auto iter = thread_infos.begin();
                pthread_join( (*iter)->thread_id, nullptr);
                thread_infos.pop_front();
            }
    }

    void traceAll()
    {
        trace_l(T_DEBUG, "States of replication:");
        for (auto & i : state) {
            trace_l(T_DEBUG, i.getRelation(), ", ",  i.getTable(), ", ", i.getAttribute());
        }
    }


#define PINSERT_AND_CONSTRUCT(index_structure, structure_enum, bucket_size) \
    void insert(persistent_ptr<index_structure> index) \
    { \
        auto status = getStatusOrNew(index->getRelation(), index->getTable(), index->getAttribute()); \
        status->add< persistent_ptr<index_structure> >(index, structure_enum, index->getNumaNode()); \
    } \
    \
    template<typename ...Args> \
    persistent_ptr<index_structure> construct##index_structure##Async(size_t numa_node, persistent_ptr<PersistentColumn> valCol, Args... args) \
    { \
        persistent_ptr<index_structure> index; \
      \
        auto pop = RootManager::getInstance().getPop(numa_node); \
        transaction::run(pop, [&]() { \
            index = make_persistent<index_structure>(args... ); \
        }); \
      \
        ReplCreateIndexArgs<persistent_ptr<index_structure>>* threadArgs = new ReplCreateIndexArgs<persistent_ptr<index_structure>>(); \
        threadArgs->node = numa_node; \
        threadArgs->index = index; \
        threadArgs->valCol = valCol; \
      \
        repl_thread_info * info = new repl_thread_info(); \
        pthread_create(&info->thread_id, nullptr, generateIndex<persistent_ptr<index_structure>, bucket_size>, threadArgs); \
        thread_infos.push_back(info); \
       \
        if (m_Wait) \
            pthread_join( info->thread_id, nullptr); \
      \
        return index; \
    } 

#define VINSERT_AND_CONSTRUCT(index_structure, structure_enum, bucket_size) \
    void insert(index_structure * index) \
    { \
        auto status = getStatusOrNew(index->getRelation(), index->getTable(), index->getAttribute()); \
        status->add< index_structure* >(index, structure_enum, index->getNumaNode()); \
    } \
    \
    template<typename ...Args> \
    index_structure* construct##index_structure##Async(size_t numa_node, persistent_ptr<PersistentColumn> valCol, Args... args) \
    { \
        index_structure* index; \
      \
        auto pop = RootManager::getInstance().getPop(numa_node); \
        index = new (general_memory_manager::get_instance().allocateNuma(sizeof(index_structure), numa_node)) index_structure(args... ); \
      \
        ReplCreateIndexArgs<index_structure*>* threadArgs = new ReplCreateIndexArgs<index_structure*>(); \
        threadArgs->node = numa_node; \
        threadArgs->index = index; \
        threadArgs->valCol = valCol; \
      \
        repl_thread_info * info = new repl_thread_info(); \
        pthread_create(&info->thread_id, nullptr, generateIndex<index_structure*, bucket_size>, threadArgs); \
        thread_infos.push_back(info); \
        if (m_Wait) \
            pthread_join( info->thread_id, nullptr); \
      \
        return index; \
    } 

    using VColumn = column<uncompr_f>;
    //VINSERT_AND_CONSTRUCT(VColumn, DataStructure::VCOLUMN)
    void insert(column<uncompr_f> * index)
    {
        trace_l(T_INFO, "Got volatile column with r: ", index->getRelation(), ", t: ", index->getTable(), ", a: ", index->getAttribute());
        auto status = getStatus(index->getRelation(), index->getTable(), index->getAttribute());
        status->add< column<uncompr_f>* >(index, DataStructure::VCOLUMN, index->getNumaNode());
    }

    column<uncompr_f> * constructVColumnAsync(size_t numa_node, persistent_ptr<PersistentColumn> valCol, size_t p_ByteSize, size_t constrNumaNode) \
    {
        column<uncompr_f> * index;

        auto pop = RootManager::getInstance().getPop(numa_node);
        index = new (general_memory_manager::get_instance().allocateNuma(sizeof(column<uncompr_f>), numa_node)) column<uncompr_f>(p_ByteSize, constrNumaNode);
        index->setRelation(valCol->getRelation());
        index->setTable(valCol->getTable());
        index->setAttribute(valCol->getAttribute());

        ReplCreateIndexArgs<column<uncompr_f>*>* threadArgs = new ReplCreateIndexArgs<column<uncompr_f>*>();
        threadArgs->node = numa_node;
        threadArgs->index = index;
        threadArgs->valCol = valCol;

        repl_thread_info * info = new repl_thread_info();
        pthread_create(&info->thread_id, nullptr, generateVColumn, threadArgs);
        thread_infos.push_back(info);
        if (m_Wait)
            pthread_join( info->thread_id, nullptr);

        return index;
    } 

    PINSERT_AND_CONSTRUCT(MultiValTreeIndex, DataStructure::PTREE, OSP_SIZE)
    PINSERT_AND_CONSTRUCT(SkipListIndex, DataStructure::PSKIPLIST, OSP_SIZE)
    PINSERT_AND_CONSTRUCT(HashMapIndex, DataStructure::PHASHMAP, OSP_SIZE)
    PINSERT_AND_CONSTRUCT(PersistentColumn, DataStructure::PCOLUMN, OSP_SIZE)

    PINSERT_AND_CONSTRUCT(CLTreeIndex, DataStructure::CLPTREE, CL_SIZE)
    PINSERT_AND_CONSTRUCT(CLHashMapIndex, DataStructure::CLPHASHMAP, CL_SIZE)
    PINSERT_AND_CONSTRUCT(CLSkipListIndex, DataStructure::CLPSKIPLIST, CL_SIZE)


    using ps = vectorlib::scalar<vectorlib::v64<uint64_t>>;
    void constructAll( persistent_ptr<PersistentColumn> col )
    {
        auto & initializer = RootInitializer::getInstance();
        const auto node_number = initializer.getNumaNodeCount();

        auto status = getStatusOrNew(col->getRelation(), col->getTable(), col->getAttribute());
        assert(status != nullptr);

        const column<uncompr_f> * conv = col->convert();

        auto tuple = group<ps, uncompr_f, uncompr_f, uncompr_f>(nullptr, conv);

        size_t distinct_key_count = std::get<1>(tuple)->get_count_values();

        delete conv;
        delete std::get<0>(tuple);  
        delete std::get<1>(tuple);  

        for (size_t node = 0; node < node_number; node++) {
            if (col->getNumaNode() != node) {
                auto newCol = copy_persistent_column_to_node(col, node);
                status->add(newCol, DataStructure::PCOLUMN, node);
                NVMStorageManager::pushPersistentColumn(newCol);
            }
            else {
                status->add(col, DataStructure::PCOLUMN, node);
                NVMStorageManager::pushPersistentColumn(col);
            }
            auto vcol = copy_volatile_column_to_node(col, node);
            status->add(vcol, DataStructure::VCOLUMN, node);

            auto tree = constructMultiValTreeIndexAsync(node, col, node, alloc_class, col->getRelation(), col->getTable(), col->getAttribute());
            auto hash = constructHashMapIndexAsync(node, col, distinct_key_count, node, col->getRelation(), col->getTable(), col->getAttribute());
            auto skip = constructSkipListIndexAsync(node, col, node, col->getRelation(), col->getTable(), col->getAttribute());

            insert(tree);
            insert(hash);
            insert(skip);

            NVMStorageManager::pushMultiValTreeIndex(tree);
            NVMStorageManager::pushSkipListIndex(skip);
            NVMStorageManager::pushHashMapIndex(hash);
        }

        trace_l(T_DEBUG, "End construct all for ", col->getRelation(), ", ", col->getTable(), ", ", col->getAttribute());
    }

    void constructAllCol( persistent_ptr<PersistentColumn> col)
    {
        auto & initializer = RootInitializer::getInstance();
        const auto node_number = initializer.getNumaNodeCount();

        auto status = getStatusOrNew(col->getRelation(), col->getTable(), col->getAttribute());

        for (size_t node = 0; node < node_number; node++) {
            if (col->getNumaNode() != node) {
                auto newCol = copy_persistent_column_to_node(col, node);
                status->add(newCol, DataStructure::PCOLUMN, node);
                NVMStorageManager::pushPersistentColumn(newCol);
            }
            else {
                status->add(col, DataStructure::PCOLUMN, node);
                NVMStorageManager::pushPersistentColumn(col);
            }
            auto vcol = copy_volatile_column_to_node(col, node);
            status->add(vcol, DataStructure::VCOLUMN, node);
        }
    }


    void constructAllCL( persistent_ptr<PersistentColumn> col )
    {
        auto & initializer = RootInitializer::getInstance();
        const auto node_number = initializer.getNumaNodeCount();

        auto status = getStatusOrNew(col->getRelation(), col->getTable(), col->getAttribute());
        assert(status != nullptr);

        const column<uncompr_f> * conv = col->convert();

        auto tuple = group<ps, uncompr_f, uncompr_f, uncompr_f>(nullptr, conv);

        size_t distinct_key_count = std::get<1>(tuple)->get_count_values();

        delete conv;
        delete std::get<0>(tuple);  
        delete std::get<1>(tuple);  

        for (size_t node = 0; node < node_number; node++) {
            auto tree = constructCLTreeIndexAsync(node, col, node, alloc_class, col->getRelation(), col->getTable(), col->getAttribute());
            auto hash = constructCLHashMapIndexAsync(node, col, distinct_key_count, node, col->getRelation(), col->getTable(), col->getAttribute());
            auto skip = constructCLSkipListIndexAsync(node, col, node, col->getRelation(), col->getTable(), col->getAttribute());

            insert(tree);
            insert(hash);
            insert(skip);

            NVMStorageManager::pushCLTreeIndex(tree);
            NVMStorageManager::pushCLSkipListIndex(skip);
            NVMStorageManager::pushCLHashMapIndex(hash);
        }
    }

    void deleteAll(std::string relation, std::string table, std::string attribute)
    {
        auto status = getStatus(relation, table, attribute);

        if (status != nullptr) {
            trace_l(T_DEBUG, "Status not null, ", relation, ", ", table, ", ", attribute);
            status->destructAll();
            removeStatus(status);
        }
    }

    bool containsAll(size_t count_values, std::string relation, std::string table, std::string attribute)
    {
        for (size_t i = 0; i < m_NumaNodeCount; i++) {
            auto status = getStatus(relation, table, attribute);

            if (status != nullptr) {
                if ( !(status->getPersistentColumn(i)
                        && status->getMultiValTreeIndex(i)
                        && status->getSkipListIndex(i)
                        && status->getHashMapIndex(i))) {
                    trace_l(T_INFO, "status did not contain all demanded persistent datastructures, returning false");
                    return false;
                }

                if (status->getPersistentColumn(i)->get_count_values() != count_values) {
                    trace_l(T_INFO, "count_values found in column is ", status->getPersistentColumn(i)->get_count_values(), ", expected ", count_values);
                    return false;
                }
            }
            else {
                return false;
            }
        } 

        return true;
    }

    bool isLocOnNode(void* loc, size_t pmemNode)
    {
        int ret_numa;
        auto ret = get_mempolicy(&ret_numa, NULL, 0, loc, MPOL_F_NODE | MPOL_F_ADDR);
        //numa_move_pages( 0 /*calling process this*/, 0 /* we dont move pages */, reinterpret_cast<void**>(loc), nullptr, &status, 0);
        trace_l(T_INFO, "location on ", loc, " is located on node ", ret_numa, ", requested is ", pmemNode, ", returned status ", ret);
        return pmemNode == static_cast<size_t>(ret_numa);
    }

    ReplicationStatus * getStatus(std::string relation, std::string table, std::string attribute)
    {
        //trace_l(T_DEBUG, "Containing ", state.size(), " columns");
        for (auto iter = state.begin(); iter != state.end(); iter++) {
            if (iter->compare(relation, table, attribute)) {
                //trace_l(T_DEBUG, "Returning ", &*iter);
                return &*iter;
            }
        }

        //trace_l(T_DEBUG, "Returning nullptr");
        return nullptr;
    }

    ReplicationStatus * getStatusOrNew(std::string relation, std::string table, std::string attribute)
    {
        auto status = getStatus(relation, table, attribute);
        if (status == nullptr) {
            state.emplace_back(relation, table, attribute);
            status = getStatus(relation, table, attribute);
        }
        return status;
    }

    void removeStatus(ReplicationStatus* status)
    {
        for (auto iter = state.begin(); iter != state.end(); iter++) {
            if (iter->compare(status->getRelation(), status->getTable(), status->getAttribute())) {
                state.erase(iter);
                return;
            }
        }
    }

    /*size_t getSelectivity(std::string relation, std::string table, std::string attribute)
    {
        auto status = getStatus(relation, table, attribute);

        if (status == nullptr) {

        }

        return 0;
    }*/

    void init(uint64_t numaNodeCount) {
        m_NumaNodeCount = numaNodeCount;

        for (uint64_t node = 0; node < m_NumaNodeCount; node++) {
            for (auto i : NVMStorageManager::getPersistentColumns(node)) {
                auto status = getStatusOrNew(i->getRelation(), i->getTable(), i->getAttribute());
                status->add(i, DataStructure::PCOLUMN, i->getNumaNode());
            }
            for (auto i : NVMStorageManager::getHashMapIndexs(node)) {
                auto status = getStatusOrNew(i->getRelation(), i->getTable(), i->getAttribute());
                status->add(i, DataStructure::PHASHMAP, i->getNumaNode());
            }
            for (auto i : NVMStorageManager::getSkipListIndexs(node)) {
                auto status = getStatusOrNew(i->getRelation(), i->getTable(), i->getAttribute());
                status->add(i, DataStructure::PSKIPLIST, i->getNumaNode());
            }
            for (auto i : NVMStorageManager::getMultiValTreeIndexs(node)) {
                auto status = getStatusOrNew(i->getRelation(), i->getTable(), i->getAttribute());
                status->add(i, DataStructure::PTREE, i->getNumaNode());
            }
        }
    }
};

}


