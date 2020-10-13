#pragma once

#include <core/access/RootManager.h>
#include <core/access/NVMStorageManager.h>
#include <core/storage/PersistentColumn.h>
#include <core/storage/column_gen.h>

#include <core/index/index_gen.h>
#include <core/index/IndexDef.h>
#include <core/tracing/trace.h>

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
    VHASHMAP
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
            case PCOLUMN:
            case PTREE:
            case PSKIPLIST:
            case PHASHMAP:
                m_PPtr = ptr;
                break;
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
};

class ReplicationStatus {
    std::string m_Relation;
    std::string m_Table;
    std::string m_Attribute;

    std::vector<ReplTuple> replication;

public:
    ReplicationStatus(std::string relation, std::string table, std::string attribute)
        : m_Relation(relation), m_Table(table), m_Attribute(attribute)
    {
    }

    bool compare(std::string relation, std::string table, std::string attribute)
    {
        return relation.compare(m_Relation) == 0 && table.compare(m_Table) == 0 && attribute.compare(m_Attribute);
    }

    bool contains(DataStructure kind)
    {
        for (auto i : replication)
            if (i.m_Kind == kind)
                return true;

        return false;
    }

#define PGET(index_structure, structure_enum) \
    persistent_ptr<index_structure> get##index_structure() \
    { \
        for (auto i : replication) \
            if (i.m_Kind == structure_enum) { \
                return static_cast<persistent_ptr<index_structure>>(i.m_PPtr); \
            } \
        return nullptr; \
    } 

    PGET(MultiValTreeIndex, PTREE);
    PGET(HashMapIndex, PHASHMAP);
    PGET(SkipListIndex, PSKIPLIST);
    PGET(PersistentColumn, PCOLUMN);

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
        replication.emplace_back(ptr, kind, pmemNode);
    }

};


template<class index_structure>
struct ReplCreateIndexArgs {
    uint64_t node;
    pptr<index_structure> index;
    pptr<PersistentColumn> valCol;
};

struct repl_thread_info {
    pthread_t thread_id;
    int thread_num;
};

// Threading infrastructure of replication manager
template< class index_structure>
void * generateIndex( void * argPtr )
{
    ReplCreateIndexArgs<index_structure> * indexArgs = (ReplCreateIndexArgs<index_structure>*) argPtr;

    numa_run_on_node(indexArgs->node);
    IndexGen::generateFast<pptr<index_structure>, OSP_SIZE>(indexArgs->index, indexArgs->valCol);

    RootManager& root_mgr = RootManager::getInstance();
    root_mgr.drainAll();

    free(argPtr);

    return nullptr;
}

class ReplicationManager {

    uint64_t m_NumaNodeCount;
    std::vector<ReplicationStatus> state;

    std::list<repl_thread_info*> thread_infos;

public:
    static ReplicationManager& getInstance()
    {
        static ReplicationManager instance;

        return instance;
    }


    void joinAllThreads()
    {
        while (thread_infos.begin() != thread_infos.end()) {
            auto iter = thread_infos.begin();
            pthread_join( (*iter)->thread_id, nullptr);
            thread_infos.pop_front();
        }
    }


#define PINSERT_AND_CONSTRUCT(index_structure, structure_enum) \
    void insert(persistent_ptr<index_structure> index) \
    { \
        auto status = getStatus(index->getRelation(), index->getTable(), index->getAttribute()); \
        status->add< persistent_ptr<index_structure> >(index, structure_enum, index->getPmemNode()); \
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
        ReplCreateIndexArgs<index_structure>* threadArgs = new ReplCreateIndexArgs<index_structure>(); \
        threadArgs->node = numa_node; \
        threadArgs->index = index; \
        threadArgs->valCol = valCol; \
      \
        repl_thread_info * info = new repl_thread_info(); \
        pthread_create(&info->thread_id, nullptr, generateIndex<index_structure>, threadArgs); \
        thread_infos.push_back(info); \
      \
        return index; \
    } 

    PINSERT_AND_CONSTRUCT(MultiValTreeIndex, DataStructure::PTREE)
    PINSERT_AND_CONSTRUCT(SkipListIndex, DataStructure::PSKIPLIST)
    PINSERT_AND_CONSTRUCT(HashMapIndex, DataStructure::PHASHMAP)
    PINSERT_AND_CONSTRUCT(PersistentColumn, DataStructure::PCOLUMN)


    void constructAll( persistent_ptr<PersistentColumn> col )
    {
        auto initializer = RootInitializer::getInstance();
        const auto node_number = initializer.getNumaNodeCount();

        auto status = getStatusOrNew(col->getRelation(), col->getTable(), col->getAttribute());

        for (size_t node = 0; node < node_number; node++) {
            if (col->getPmemNode() != node) {
                auto newCol = copy_column_to_node(col, node);
                status->add(newCol, DataStructure::PCOLUMN, node);
            }
            else {
                status->add(col, DataStructure::PCOLUMN, node);
            }
            auto tree = constructMultiValTreeIndexAsync(node, col, node, col->getRelation(), col->getTable(), col->getAttribute());
            auto hash = constructHashMapIndexAsync(node, col, /*distict key count*/ tree->getKeyCount(), node, col->getRelation(), col->getTable(), col->getAttribute());
            auto skip = constructSkipListIndexAsync(node, col, node, col->getRelation(), col->getTable(), col->getAttribute());

            insert(tree);
            insert(hash);
            insert(skip);
        }
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
        for (auto iter = state.begin(); iter != state.end(); iter++) {
            if (iter->compare(relation, table, attribute)) {
                return &*iter;
            }
        }

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

    size_t getSelectivity(std::string relation, std::string table, std::string attribute)
    {
        auto status = getStatus(relation, table, attribute);

        if (status == nullptr) {

        }

        return 0;
    }

    void init(uint64_t numaNodeCount) {
        m_NumaNodeCount = numaNodeCount;

        for (uint64_t node = 0; node < m_NumaNodeCount; node++) {
            for (auto i : NVMStorageManager::getPersistentColumns(node)) {
                auto status = getStatusOrNew(i->getRelation(), i->getTable(), i->getAttribute());
                status->add(i, DataStructure::PCOLUMN, i->getPmemNode());
            }
            for (auto i : NVMStorageManager::getHashMapIndexs(node)) {
                auto status = getStatusOrNew(i->getRelation(), i->getTable(), i->getAttribute());
                status->add(i, DataStructure::PHASHMAP, i->getPmemNode());
            }
            for (auto i : NVMStorageManager::getSkipListIndexs(node)) {
                auto status = getStatusOrNew(i->getRelation(), i->getTable(), i->getAttribute());
                status->add(i, DataStructure::PSKIPLIST, i->getPmemNode());
            }
            for (auto i : NVMStorageManager::getMultiValTreeIndexs(node)) {
                auto status = getStatusOrNew(i->getRelation(), i->getTable(), i->getAttribute());
                status->add(i, DataStructure::PTREE, i->getPmemNode());
            }
        }
    }
};

}


