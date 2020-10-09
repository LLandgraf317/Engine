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

class ReplicationManager {

    uint64_t m_NumaNodeCount;
    std::vector<ReplicationStatus> state;

public:
    static ReplicationManager& getInstance()
    {
        static ReplicationManager instance;

        return instance;
    }

#define PINSERT(index_structure, structure_enum) \
    void insert(persistent_ptr<index_structure> index) \
    { \
        auto status = getStatus(index->getRelation(), index->getTable(), index->getAttribute()); \
        status->add< persistent_ptr<index_structure> >(index, structure_enum, index->getPmemNode()); \
    }

    PINSERT(MultiValTreeIndex, DataStructure::PTREE);
    PINSERT(SkipListIndex, DataStructure::PSKIPLIST);
    PINSERT(HashMapIndex, DataStructure::PHASHMAP);
    PINSERT(PersistentColumn, DataStructure::PCOLUMN);

    bool isLocOnNode(void* loc, size_t pmemNode)
    {
        int status;
        numa_move_pages( 0 /*calling process this*/, 0 /* we dont move pages */, reinterpret_cast<void**>(loc), 0, &status, 0);
        return pmemNode == static_cast<size_t>(status);
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


