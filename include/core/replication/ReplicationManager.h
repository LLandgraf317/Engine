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
        return kind; //TODO
    }

    persistent_ptr<MultiValTreeIndex> getTree()
    {
        for (auto i : replication)
            if (i.m_Kind == PTREE)
                return static_cast<persistent_ptr<MultiValTreeIndex>>(i.m_PPtr);

        return nullptr;
    }

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

    void initialize()
    {

    }

    void insert(persistent_ptr<MultiValTreeIndex> index)
    {
        auto status = getStatus(index->getRelation(), index->getTable(), index->getAttribute());
        status->add< persistent_ptr<MultiValTreeIndex> >(index, DataStructure::PTREE, index->getPmemNode());
    }

    void insert(persistent_ptr<SkipListIndex> i)
    {
        auto status = getStatus(i->getRelation(), i->getTable(), i->getAttribute());
        status->add< persistent_ptr<SkipListIndex> >(i, DataStructure::PSKIPLIST, i->getPmemNode());
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

    size_t getSelectivity(std::string relation, std::string table, std::string attribute)
    {
        auto status = getStatus(relation, table, attribute);

        if (status == nullptr) {

        }
        //else if (status.contains(DataStructure

        return 0;
    }

    void init(uint64_t numaNodeCount) {
        m_NumaNodeCount = numaNodeCount;

        for (auto i : NVMStorageManager::getPTrees()) {
            
        }
        for (auto i : NVMStorageManager::getPSkipLists()) {

        }
        for (auto i : NVMStorageManager::getPHashMaps()) {

        }
        for (auto i : NVMStorageManager::getPColumns()) {

        }
    }

    template<typename index_structure_ptr>
    index_structure_ptr getDataStructure()
    {
        return nullptr;
    }

};

}


