#pragma once

namespace morphstore {

enum DataStructure {
    PCOLUMN;
    VCOLUMN;

    PTREE;
    VTREE;

    PSKIPLIST;
    VSKIPLIST;

    PHASHMAP;
    VHASHMAP;
};

using NumaNode = uint64_t;
using ReplTuple = std::tuple<DataStructure, NumaNode>

class ReplTuple {
private:
    pptr<void> m_DSPtr;
    DataStructure m_Kind;
    uint64_t m_NumaNode;
}

class ReplicationStatus {
    std::string m_Relation;
    std::string m_Table;
    std::string m_Attribute;

    std::vector<ReplTuple> replication;

public:
    ReplicationStatus(std::string relation, std::string table, std::string attribute, size_t pmemNode, DataStructure ds)
        : m_Relation(relation), m_Table(table), m_Attribute(attribute)
    {
        replication.emplace( 
    }

    bool compare(std::string relation, std::string table, std::string attribute)
    {
        return relation.compare(m_Relation) == 0 && table.compare(m_Table) == 0 && attribute.compare(m_Attribute);
    }

    bool contains(DataStructure kind, std::string relation, std::string table, std::string attribute)
    {
        return false;
    }

    template<
    void add

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

    void insert(std::vector<MultiValTreeIndex>& trees)
    {
        for (auto i : trees) {
            auto status = getStatus(i->getRelation(), i->getTable(), i->getAttribute());
            if (status == nullptr) {
                state.emplace(i->getRelation(), i->getTable(), i->getAttribute(), i->getPmemNode(), DataStructure::PTREE, persistent_ptr<MultiValTreeIndex> ptr)
            }
        }
        state.insert(    
    }

    ReplicationStatus * getStatus(std::string relation, std::string table, std::string attribute)
    {
        for (auto i : state) {
            if (i.compare(relation, table, attribute)) {
                return &(*i);
            }
        }

        return nullptr;
    }

    void init(uint64_t numaNodeCount) {
        m_NumaNodeCount = numaNodeCount;

        for (auto i : NVMStorageManager::getPTrees()) {
            
        }
        NVMStorageManager::getPSkipLists();
        NVMStorageManager::getPHashMaps();
        NVMStorageManager::getPColumns();
    }

    template<index_structure_ptr>
    index_structure_ptr getDataStructure()
    {
        return nullptr;
    }

}

}
