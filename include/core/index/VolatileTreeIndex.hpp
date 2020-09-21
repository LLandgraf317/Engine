#pragma once

#include <core/index/TreeDef.h>
#include <core/index/VNodeBucketList.h>
#include <core/index/VolatileBPTree.hpp>

namespace morphstore {

class VolatileTreeIndex {

    VolatileBPTree<uint64_t, VNodeBucketList<uint64_t> *, 10, 10>  * m_Tree;
    std::string m_Relation;
    std::string m_Attribute;
    std::string m_Table;

    size_t m_NumaNode;
    bool m_Init;
    size_t m_CountTuples;

public:
    VolatileTreeIndex(uint64_t numaNode, std::string table, std::string relation, std::string attribute)
    {
        m_NumaNode = numaNode;

        m_Table = table;
        m_Attribute = attribute;
        m_Relation = relation;

        m_Tree = new VolatileBPTree<uint64_t, VNodeBucketList<uint64_t> *, 10, 10>(numaNode);

        m_Init = false;
        m_CountTuples = 0;
    }

    std::string getTable()
    {
        return m_Table;
    }

    std::string getRelation()
    {
        return m_Relation;
    }

    std::string getAttribute()
    {
        return m_Attribute;
    }

    size_t getNumaNode()
    {
        return m_NumaNode;
    }

    void setInit()
    {
        m_Init = true;
    }

    bool isInit()
    {
        return m_Init;
    }

    size_t getCountValues() const
    {
        return m_CountTuples;
    }

    VNodeBucketList<uint64_t> * find(uint64_t key) const
    {
        VNodeBucketList<uint64_t> * list;
        bool success = m_Tree->lookup(key, &list);
       
        if (success)
           return list;
        else
           return nullptr; 
    }

    void insert(uint64_t key, uint64_t value)
    {

        VNodeBucketList<uint64_t> * list;

        if (m_Tree->lookup(key, &list)) {
            list->insertValue(value); 
        }
        else {
            list = new VNodeBucketList<uint64_t>(m_NumaNode);
            m_Tree->insert(key, list);

            list->insertValue(value);
        }

        m_CountTuples = m_CountTuples + 1;
    }

    bool lookup(uint64_t key, uint64_t val)
    {
        VNodeBucketList<uint64_t> * list;

        if (m_Tree->lookup(key, &list)) {
            return list->lookup(val);
        }

        return false;
    }

    using ScanFunc = std::function<void(const uint64_t &key, VNodeBucketList<uint64_t> * const val)>;
    void scan(const uint64_t &minKey, const uint64_t &maxKey, ScanFunc func) const {
        m_Tree->scan(minKey, maxKey, func);
    }

    inline void scanValue(const uint64_t &minKey, const uint64_t &maxKey, column<uncompr_f>* &col) const {

        std::list<VNodeBucketList<uint64_t> *> list;
        m_Tree->scanValue(minKey, maxKey, list);
        size_t sum_count_values = 0;

        for (auto i : list) {
            sum_count_values += (*i).getCountValues();
        }

        col = new column<uncompr_f>(sizeof(uint64_t) * sum_count_values);
        uint64_t * data = col->get_data();

        for (auto i : list) {
            VNodeBucketList<uint64_t>::Iterator iter = (*i).begin();

            while (iter != (*i).end()) {
                *data = iter.get();
                data++;
                iter++;
            }
        }

        col->set_meta_data(sum_count_values, sizeof(uint64_t) * sum_count_values);
    }

    void scan(ScanFunc func) const
    {
        m_Tree->scan(func);
    }

    bool deleteEntry(uint64_t key, uint64_t value)
    {
        VNodeBucketList<uint64_t> * list;

        if (m_Tree->lookup(key, &list)) {
            bool success = list->deleteValue(value);
            if (list->isEmpty())
                m_Tree->erase(key);
            m_CountTuples = m_CountTuples - 1;
            return success;
        }

        return false;
    }

};

} // namespace morphstore
