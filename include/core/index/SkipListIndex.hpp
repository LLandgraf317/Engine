#pragma once

#include <core/storage/PersistentColumn.h>
#include <core/index/NodeBucketList.h>

#include <nvmdatastructures/src/pskiplists/simplePSkiplist.hpp>

#include <stdexcept>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

using dbis::pskiplists::simplePSkiplist;
using pmem::obj::persistent_ptr;
using pmem::obj::make_persistent;

namespace morphstore {

template<unsigned t_bucket_size = OSP_SIZE>
class PSkipListIndex {
    /*template< template <template <uint64_t> class> class t_pptr, template<uint64_t> class t_index, uint64_t t_size>
    friend class IndexGen;*/
    //template<template < template <uint64_t> class t_index> class t_pptr>
    friend class IndexGen;

    using CustomSkiplist = simplePSkiplist<uint64_t, persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>>, 8>;
private:
    persistent_ptr<CustomSkiplist> m_SkipList;
    p<size_t> m_CountTuples;
    p<size_t> m_PmemNode;
    p<bool> m_Init;

    persistent_ptr<char[]> m_Table;
    persistent_ptr<char[]> m_Relation;
    persistent_ptr<char[]> m_Attribute;

    p<size_t> m_rl;
    p<size_t> m_tl;
    p<size_t> m_al;

protected:
    persistent_ptr<CustomSkiplist> getDS()
    {
        return m_SkipList;
    }

public:
    PSkipListIndex(int p_PmemNode) : PSkipListIndex(p_PmemNode, std::string("null"), std::string("null"), std::string("null")) {

    }

    PSkipListIndex(int p_PmemNode, std::string relation, std::string table, std::string attribute) : m_PmemNode(p_PmemNode)
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_PmemNode);

        m_Init = false;

        m_SkipList = make_persistent<CustomSkiplist>();

        m_Table = make_persistent<char[]>(table.length() + 1);
        m_tl = table.length() + 1;
        m_Attribute = make_persistent<char[]>(attribute.length() + 1);
        m_al = attribute.length() + 1;
        m_Relation = make_persistent<char[]>(relation.length() + 1);
        m_rl = relation.length() + 1;

        pop.memcpy_persist(m_Table.get(), table.c_str(), table.length() + 1);
        pop.memcpy_persist(m_Attribute.get(), attribute.c_str(), attribute.length() + 1);
        pop.memcpy_persist(m_Relation.get(), relation.c_str(), relation.length() + 1);

    }

    void prepareDest()
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_PmemNode);

        m_SkipList->scan([&] (const uint64_t &, const persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> & val) {
            if (val != nullptr) {
                val->prepareDest();
                transaction::run(pop, [&] {
                    delete_persistent<NodeBucketList<uint64_t, t_bucket_size>>(val);
                });
            }
        });

        transaction::run(pop, [&] {
            delete_persistent<CustomSkiplist>(m_SkipList);
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

    size_t memory_footprint()
    {
        size_t sum = 0;

        sum += m_SkipList->memory_footprint();
        //using ScanFunc = std::function<void(const uint64_t &key, const persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> &val)>;
        //
        auto lambda = [&] (const uint64_t & /*key*/, const persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> & val) {
            if (val != nullptr)
                sum += val->memory_footprint();
        };
        scan(lambda);

        return sum + sizeof(PSkipListIndex<t_bucket_size>);
    }

    persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> find(uint64_t key)
    {
        persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> list;
        bool success = m_SkipList->search(key, list);
        if (!success)
            return nullptr;
        
        return list;
    }

    void insert(uint64_t key, uint64_t value)
    {
        persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> list;
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_PmemNode);

        if (m_SkipList->search(key, list)) {
            if (list == nullptr) {
                //trace_l(T_INFO, "Skiplist search returned true, but list is null");
                transaction::run(pop, [&] {
                    list = make_persistent<NodeBucketList<uint64_t, t_bucket_size>>(m_PmemNode);
                });
            }
            m_SkipList->insert(key, list);
            list->insertValue(value); 
        }
        else {
            transaction::run(pop, [&] {
                list = make_persistent<NodeBucketList<uint64_t, t_bucket_size>>(m_PmemNode);
            });
            //trace_l(T_INFO, "Inserting new entry");
            if (list == nullptr)
                throw new std::runtime_error("out of memory");
            bool success;
            transaction::run(pop, [&] {
                success = m_SkipList->insert(key, list);
            });
            //trace_l(T_INFO, "success: ", success);
            list->insertValue(value);
        }

        m_CountTuples++;
    }

    bool lookup(uint64_t key, uint64_t val) const
    {
        persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> list;
        bool success = m_SkipList->search(key, list);
        if (!success || list == nullptr) {
            //trace_l(T_INFO, "Did not find any list for key ", key, ", success = ", success);
            return false;
        }

        //trace_l(T_INFO, "Looking up ", val);
        return list->lookup(val);
    }

    bool deleteEntry(uint64_t key, uint64_t value)
    {
        persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> list;
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

    using ScanFunc = std::function<void(const uint64_t &key, const persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> &val)>;
    void scan(ScanFunc func) const
    {
        m_SkipList->scan(func);
    }

    void scanValue(const uint64_t &minKey, const uint64_t &maxKey, column<uncompr_f>* &outCol) const {

        //trace_l(T_INFO, "Got values ", minKey, " and ", maxKey);
        std::list<persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>>> list;

        m_SkipList->scanValue(minKey, maxKey, list);
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

    void print()
    {
        m_SkipList->printList();
    }

    void printContents()
    {
        auto printLambda = [&](const uint64_t &key, const persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> buck) {
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
