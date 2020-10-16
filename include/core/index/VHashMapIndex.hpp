#pragma once

#include <core/memory/management/abstract_mm.h>
#include <core/memory/management/general_mm.h>

#include <core/index/VHashMap.hpp>
#include <core/index/VNodeBucketList.h>

#include <core/storage/column.h>
#include <vector/scalar/extension_scalar.h>
#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>

namespace morphstore {

class VHashMapIndex {

    using ps = vectorlib::scalar<vectorlib::v64<uint64_t>>;
    using CustomHashmap = VHashMap<ps, uint64_t, uint64_t>;

    CustomHashmap * m_HashMap;
    uint64_t m_PmemNode;

    std::string m_Table;
    std::string m_Relation;
    std::string m_Attribute;

    bool m_Init;
    uint64_t m_EstimateElemCount;
    size_t m_CountTuples;

public:

    VHashMapIndex(uint64_t estimateElemCount, uint64_t pMemNode, std::string table, std::string relation, std::string attribute)
	    : m_PmemNode(pMemNode), m_Init(false), m_EstimateElemCount(estimateElemCount)
    {
        m_Table = table;
        m_Attribute = attribute;
        m_Relation = relation;

        auto tmp = reinterpret_cast<CustomHashmap*>(general_memory_manager::get_instance().allocateNuma(sizeof(CustomHashmap), pMemNode));
        m_HashMap = new (tmp) CustomHashmap(estimateElemCount, pMemNode);
    }

    ~VHashMapIndex()
    {
        m_HashMap->~CustomHashmap();
        general_memory_manager::get_instance().deallocateNuma(reinterpret_cast<void*>(m_HashMap), sizeof(CustomHashmap));
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

    size_t getCountValues()
    {
        return m_CountTuples;
    }

    size_t getNumaNode()
    {
        return m_PmemNode;
    }

    VNodeBucketList<uint64_t> * find(uint64_t key)
    {
        using HashMapElem = std::tuple<uint64_t, VNodeBucketList<uint64_t> *>; 
        HashMapElem res = m_HashMap->lookup(key);

        return std::get<1>(res);
	}

    void insert(uint64_t key, uint64_t value)
    {
        m_HashMap->insert(key, value);	

        m_CountTuples++;
    }

    bool lookup(uint64_t key, uint64_t val)
    {
        return m_HashMap->lookup(key, val);
    }

    using ScanFunc = std::function<void(const uint64_t &key, VNodeBucketList<uint64_t> * const val)>;
    void scan(const uint64_t &minKey, const uint64_t &maxKey, ScanFunc func) const
    {
        m_HashMap->apply(minKey, maxKey, func);
    }

    void scan(ScanFunc func) const
    {
        m_HashMap->apply(func);
    }

    inline void scanValue(const uint64_t &minKey, const uint64_t &maxKey, column<uncompr_f>* &outCol) const {
        std::list<VNodeBucketList<uint64_t> *> list;

        m_HashMap->scanValue(minKey, maxKey, list);
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

    bool deleteEntry(uint64_t key, uint64_t value)
    {
        if (m_HashMap->erase(key, value)) {
            m_CountTuples--;
            return true;
        }
        else {
            return false;
        }
    }

};

} // namespace morphstore
