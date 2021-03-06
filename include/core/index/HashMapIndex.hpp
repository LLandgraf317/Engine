#pragma once

#include <core/index/PHashMap.hpp>
#include <core/storage/PersistentColumn.h>
#include <core/storage/column.h>
#include <vector/scalar/extension_scalar.h>
#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>

namespace morphstore {

template<uint64_t t_bucket_size = OSP_SIZE>
class PHashMapIndex {

    //template<template < template <uint64_t> class t_index> class t_pptr>
    friend class IndexGen;

    using ps = vectorlib::scalar<vectorlib::v64<uint64_t>>;
    template <typename Object>
    using pptr = pmem::obj::persistent_ptr<Object>;
    using CustomHashmap = PHashMap<ps, uint64_t, uint64_t, t_bucket_size>;

    pptr<CustomHashmap> m_HashMap;
    p<uint64_t> m_PmemNode;

    pptr<char[]> m_Table;
    pptr<char[]> m_Relation;
    pptr<char[]> m_Attribute;

    p<size_t> m_rl;
    p<size_t> m_tl;
    p<size_t> m_al;

    p<bool> m_Init;
    p<uint64_t> m_EstimateElemCount;
    p<size_t> m_CountTuples;

protected:
    pptr<CustomHashmap> getDS()
    {
        return m_HashMap;
    }

public:

    PHashMapIndex(uint64_t estimateElemCount, uint64_t pMemNode, std::string relation, std::string table, std::string attribute)
	    : m_PmemNode(pMemNode), m_Init(false), m_EstimateElemCount(estimateElemCount)
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = mgr.getPop(pMemNode);

        m_Table = make_persistent<char[]>(table.length() + 1);
        m_tl = table.length() + 1;
        m_Attribute = make_persistent<char[]>(attribute.length() + 1);
        m_al = attribute.length() + 1;
        m_Relation = make_persistent<char[]>(relation.length() + 1);
        m_rl = relation.length() + 1;

        pop.memcpy_persist(m_Table.get(), table.c_str(), table.length() + 1);
        pop.memcpy_persist(m_Attribute.get(), attribute.c_str(), attribute.length() + 1);
        pop.memcpy_persist(m_Relation.get(), relation.c_str(), relation.length() + 1);

        m_HashMap = make_persistent<CustomHashmap>(estimateElemCount, pMemNode);
    }

    void prepareDest()
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = mgr.getPop(m_PmemNode);

        m_HashMap->apply([&] (const uint64_t &, const pptr<NodeBucketList<uint64_t, t_bucket_size>> & val) {
            if (val != nullptr) {
                val->prepareDest();
                transaction::run(pop, [&] {
                    delete_persistent<NodeBucketList<uint64_t, t_bucket_size>>(val);
                });
            }
        });

        transaction::run(pop, [&] {
            delete_persistent<CustomHashmap>(m_HashMap);
        });

        delete_persistent_atomic<char[]>(m_Relation, m_rl);
        delete_persistent_atomic<char[]>(m_Table, m_tl);
        delete_persistent_atomic<char[]>(m_Attribute, m_al);
    }

    std::string getTable()
    {
        return std::string(m_Table.get());
    }

    std::string getRelation()
    {
        return std::string(m_Relation.get());
    }

    std::string getAttribute()
    {
        return std::string(m_Attribute.get());
    }

    size_t memory_footprint()
    {
        size_t sum = m_HashMap->memory_footprint();

        return sum + sizeof(PHashMapIndex<t_bucket_size>);
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

    //const column<uncompr_f> * find(uint64_t key)
    pptr<NodeBucketList<uint64_t, t_bucket_size>> find(uint64_t key)
    {
        using HashMapElem = std::tuple<uint64_t, pptr<NodeBucketList<uint64_t, t_bucket_size>>>; 
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

    using ScanFunc = std::function<void(const uint64_t &key, const pptr<NodeBucketList<uint64_t, t_bucket_size>> &val)>;
    void scan(const uint64_t &minKey, const uint64_t &maxKey, ScanFunc func) const
    {
        m_HashMap->apply(minKey, maxKey, func);
    }

    void scan(ScanFunc func) const
    {
        m_HashMap->apply(func);
    }

    inline void scanValue(const uint64_t &minKey, const uint64_t &maxKey, column<uncompr_f>* &outCol) const {
        std::list<pptr<NodeBucketList<uint64_t, t_bucket_size>>> list;

        m_HashMap->scanValue(minKey, maxKey, list);
        size_t sum_count_values = 0;

        for (auto i : list) {
            sum_count_values += (*i).getCountValues();
        }

        outCol = new column<uncompr_f>(sizeof(uint64_t) * sum_count_values);
        uint64_t * data = outCol->get_data();

        for (auto i : list) {
            typename NodeBucketList<uint64_t, t_bucket_size>::Iterator iter = (*i).begin();
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
