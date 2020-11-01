#pragma once

#include <core/index/TreeDef.h>
#include <core/index/NodeBucketList.h>
#include <core/storage/PersistentColumn.h>
#include <core/memory/constants.h>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pool.hpp>

namespace morphstore {

using namespace pmem::obj;

class UniqueTreeIndex {

        //template<template<uint64_t> class> class t_pptr, template<uint64_t> class T, t_bucket_size
    /*template< template <template <uint64_t> class> class t_pptr, template<uint64_t> class t_index, uint64_t t_size>
    friend class IndexGen;*/
    //template<template < template <uint64_t> class t_index> class t_pptr>
    friend class IndexGen;

    pptr<SingleValTree> m_Tree;
    pptr<char[]> m_Relation;
    pptr<char[]> m_Attribute;
    pptr<char[]> m_Table;

    p<size_t> m_rl;
    p<size_t> m_tl;
    p<size_t> m_al;

    p<size_t> m_PmemNode;
    p<bool> m_Init;
    p<size_t> m_CountTuples;

protected:
    pptr<SingleValTree> getDS()
    {
        return m_Tree;
    }

public:
    UniqueTreeIndex(uint64_t pMemNode, pobj_alloc_class_desc alloc_class, std::string relation, std::string table, std::string attribute)
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = mgr.getPop(pMemNode);

        m_PmemNode = pMemNode;

        m_Table = make_persistent<char[]>(table.length() + 1);
        m_tl = table.length() + 1;
        m_Attribute = make_persistent<char[]>(attribute.length() + 1);
        m_al = attribute.length() + 1;
        m_Relation = make_persistent<char[]>(relation.length() + 1);
        m_rl = relation.length() + 1;

        m_Tree = make_persistent<MultiValTree<t_bucket_size>>(alloc_class);

        pop.memcpy_persist(m_Table.get(), table.c_str(), table.length() + 1);
        pop.memcpy_persist(m_Attribute.get(), attribute.c_str(), attribute.length() + 1);
        pop.memcpy_persist(m_Relation.get(), relation.c_str(), relation.length() + 1);

        m_Init = false;
        m_CountTuples = 0;
    }

    void prepareDest()
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = mgr.getPop(m_PmemNode);

        transaction::run(pop, [&] {
            delete_persistent<MultiValTree<t_bucket_size>>(m_Tree);
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

    size_t getNumaNode()
    {
        return m_PmemNode;
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

    size_t memory_footprint()
    {
        size_t sum = m_Tree->memory_footprint() + sizeof(PTreeIndex<t_bucket_size>);

        auto lambda = [&] (const uint64_t &, const persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> & val) {
            if (val != nullptr)
                sum += val->memory_footprint();
        };

        scan(lambda);

        return sum;
    }

    size_t getKeyCount()
    {
        return m_CountTuples;
    }

    /*uint64_t find(uint64_t key)
    {
        uint64_t value;
        bool success = m_Tree->lookup(key, &value);
       
        if (success)
           return list;
        else
           return 0; 
    }*/

    void insert(uint64_t key, uint64_t value)
    {

        pptr<NodeBucketList<uint64_t>> list;

        m_Tree->insert(key, value);

        m_CountTuples++;
    }


    inline bool lookup(uint64_t key, uint64_t & val)
    {
        return m_Tree->lookup(key, val);
    }

    using ScanFunc = std::function<void(const uint64_t &key, const pptr<NodeBucketList<uint64_t>> &val)>;
    void scan(const uint64_t &minKey, const uint64_t &maxKey, ScanFunc func) const {
        m_Tree->scan(minKey, maxKey, func);
    }

    inline void scanValue(const uint64_t &minKey, const uint64_t &maxKey, column<uncompr_f>* &col) const {

        std::list<uint64_t> list;
        m_Tree->scanValue(minKey, maxKey, list);

        size_t count_values = list.length();
        col = new column<uncompr_f>(sizeof(uint64_t) * count_values);
        uint64_t * data = col->get_data();

        for (auto i : list) {
            *data = *i;
            data++;
        }

        col->set_meta_data(sum_count_values, sizeof(uint64_t) * count_values);
    }

    void scan(ScanFunc func) const
    {
        m_Tree->scan(func);
    }

    bool deleteEntry(uint64_t key, uint64_t /*value*/)
    {
        bool suc = m_Tree->erase(key);
        if (suc)
            m_CountTuples--;

        return suc;
    }

};

} // namespace morphstore
