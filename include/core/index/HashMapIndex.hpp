#pragma once

#include <core/index/PHashMap.hpp>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>

namespace morphstore {

class HashMapIndex {

    using ps = scalar<v64<uint64_t>>;
    template <typename Object>
    using pptr = pmem::obj::persistent_ptr<Object>;

    pptr<PHashMap <ps, uint64_t, uint64_t>> m_HashMap;
    uint64_t m_PmemNode;

    pptr<char[]> m_Table;
    pptr<char[]> m_Relation;
    pptr<char[]> m_Attribute;

    bool m_Init;
    uint64_t m_EstimateElemCount;

    HashMapIndex(uint64_t estimateElemCount, uint64_t pMemNode, std::string table, std::string relation, std::string attribute)
	    : m_PmemNode(pMemNode), m_Init(false), m_EstimateElemCount(estimateElemCount)
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), pMemNode);

        m_Table = make_persistent<char[]>(table.length() + 1);
        m_Attribute = make_persistent<char[]>(attribute.length() + 1);
        m_Relation = make_persistent<char[]>(relation.length() + 1);

        pop.memcpy_persist(m_Table.raw_ptr(), table.c_str(), table.length() + 1);
        pop.memcpy_persist(m_Attribute.raw_ptr(), attribute.c_str(), attribute.length() + 1);
        pop.memcpy_persist(m_Relation.raw_ptr(), relation.c_str(), relation.length() + 1);

        m_HashMap = make_persistent<PHashMap<ps, uint64_t, uint64_t>>(estimateElemCount);
    }

    void generateFromPersistentColumn(pptr<PersistentColumn> keyCol, pptr<PersistentColumn> valueCol)
    {
        if (m_Init) return; // Should throw exception instead

        auto count_values = keyCol->get_count_values();
        uint64_t* key_data = keyCol->get_data();
        uint64_t* value_data = valueCol->get_data();

        //TODO: slow, much optimization potential
        for (size_t i = 0; i < count_values; i++)
            insert(key_data[i], value_data[i]);

        m_Init = true;
    }

    void generateKeyToPos(pptr<PersistentColumn> keyCol)
    {
        if (m_Init) return; // Should throw exception

        auto count_values = keyCol->get_count_values();
        uint64_t* key_data = keyCol->get_data();

        //TODO: slow, much optimization potential
        for (size_t i = 0; i < count_values; i++)
            insert(key_data[i], i);

        m_Init = true;
    }

    const column<uncompr_f> * find(uint64_t key)
    {
        using HashMapElem = std::tuple<uint64_t, pptr<NodeBucketList<uint64_t>>>; 
        HashMapElem res = m_HashMap->lookup(key);

        size_t elemCount = std::get<1>(res)->getCountValues();

        column<uncompr_f> * outCol = new column<uncompr_f>(sizeof(uint64_t) * elemCount);
        uint64_t * outData = outCol->get_data();

        NodeBucketList<uint64_t>::Iterator iter = std::get<1>(res)->begin();
        for (; iter != std::get<1>(res)->end(); iter++) {
            (*outData) = iter.get();
            outData++;
        }
        outCol->set_meta_data(elemCount, sizeof(uint64_t) * elemCount);

        return outCol;
	}

    void insert(uint64_t key, uint64_t value)
    {
         m_HashMap->insert(key, value);	
    }

    bool lookup(uint64_t key, uint64_t val)
    {
        return m_HashMap->lookup(key, val);
    }

    using ScanFunc = std::function<void(const uint64_t &key, const pptr<NodeBucketList<uint64_t>> &val)>;
    void scan(const uint64_t &minKey, const uint64_t &maxKey, ScanFunc func) const
    {
        for (uint64_t i = minKey; i <= maxKey; i++)
            m_HashMap->apply(i, func);
    }

    void scan(ScanFunc func) const
    {
        for (uint64_t i = 0; i <= m_EstimateElemCount; i++)
            m_HashMap->apply(i, func);
    }

    bool deleteEntry(uint64_t key, uint64_t value)
    {
        return m_HashMap->erase(key, value);
    }

};

} // namespace morphstore
