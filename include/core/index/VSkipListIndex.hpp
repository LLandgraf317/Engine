#pragma once

#include <core/index/VNodeBucketList.h>
#include <core/index/VSkiplist.hpp>

#include <stdexcept>


namespace morphstore {

class VSkipListIndex {

    using CustomSkiplist = VSkiplist<uint64_t, VNodeBucketList<uint64_t> *, 8>;
private:
    CustomSkiplist * m_SkipList;
    size_t m_CountTuples;
    size_t m_PmemNode;
    bool m_Init;

    std::string m_Table;
    std::string m_Relation;
    std::string m_Attribute;

public:
    VSkipListIndex(int p_PmemNode) : VSkipListIndex(p_PmemNode, std::string("null"), std::string("null"), std::string("null")) {

    }

    VSkipListIndex(int p_PmemNode, std::string relation, std::string table, std::string attribute) : m_PmemNode(p_PmemNode)
    {
        m_Init = false;

        m_SkipList = new CustomSkiplist();

        m_Table = table;
        m_Attribute = attribute;
        m_Relation = relation;
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

    size_t getPmemNode()
    {
        return m_PmemNode;
    }

    const VNodeBucketList<uint64_t> * find(uint64_t key)
    {
        VNodeBucketList<uint64_t> * list;
        bool success = m_SkipList->search(key, list);
        if (!success)
            return nullptr;
        
        return list;
    }

    void insert(uint64_t key, uint64_t value)
    {
        VNodeBucketList<uint64_t> * list;

        if (m_SkipList->search(key, list)) {
            if (list == nullptr)
                list = new VNodeBucketList<uint64_t>(m_PmemNode);
            list->insertValue(value); 
        }
        else {
            list = new VNodeBucketList<uint64_t>(m_PmemNode);
            if (list == nullptr)
                throw new std::runtime_error("out of memory");
            m_SkipList->insert(key, list);
            list->insertValue(value);
        }

        m_CountTuples++;
    }

    bool lookup(uint64_t key, uint64_t val) const
    {
        VNodeBucketList<uint64_t> * list;
        bool success = m_SkipList->search(key, list);
        if (!success || list == nullptr)
            return false;

        return list->lookup(val);
    }

    bool deleteEntry(uint64_t key, uint64_t value)
    {
        VNodeBucketList<uint64_t> * list;
        bool success = m_SkipList->search(key, list);
        
        if (!success || list == nullptr)
            return false;

        if (list->deleteValue(value)) {
            m_CountTuples--;
            return true;
        }
        else {
            return false;
        }
    }

    using ScanFunc = std::function<void(const uint64_t &key, VNodeBucketList<uint64_t> * const &val)>;
    void scan(ScanFunc func) const
    {
        m_SkipList->scan(func);
    }

    void scanValue(const uint64_t &minKey, const uint64_t &maxKey, column<uncompr_f>* &outCol) const {

        std::list<VNodeBucketList<uint64_t> *> list;

        m_SkipList->scanValue(minKey, maxKey, list);
        size_t sum_count_values = 0;

        for (auto i : list) {
            sum_count_values += (*i).getCountValues();
        }

        outCol = new column<uncompr_f>(sizeof(uint64_t) * sum_count_values);
        uint64_t * data = outCol->get_data();

        for (auto i : list) {
            VNodeBucketList<uint64_t>::Iterator iter = (*i).begin();
            while (iter != (*i).end()) {
                *data = iter.get();
                data++;
                iter++;
            }
        }

        outCol->set_meta_data(sum_count_values, sizeof(uint64_t) * sum_count_values);
    }

    void print()
    {
        m_SkipList->printList();
    }

    void printContents()
    {
        auto printLambda = [&](const uint64_t &key, VNodeBucketList<uint64_t> * const buck) {
            if (buck == nullptr) {
                trace_l(T_INFO, "key, bucket is nullptr");
            }
            else {
                trace_l(T_INFO, "key: ", key, ", value_count: ", buck->getCountValues());
                trace_l(T_INFO, "values: ");
                for (auto iter = buck->begin(); iter != buck->end(); iter++) {
                    trace_l(T_INFO, iter.get());
                } 
                trace_l(T_INFO, "");
            }
        };

        m_SkipList->scan(printLambda);
    }
};

} // namespace morphstore
