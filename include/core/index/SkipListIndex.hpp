#pragma once

#include <core/storage/PersistentColumn.h>
#include <core/index/NodeBucketList.h>

#include <nvmdatastructures/src/pskiplists/simplePSkiplist.hpp>

#include <stdexcept>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

namespace morphstore {

class SkipListIndex {
private:
    pmem::obj::persistent_ptr<dbis::pskiplists::simplePSkiplist<uint64_t, pmem::obj::persistent_ptr<NodeBucketList<uint64_t>>, 8>> m_SkipList;
    p<size_t> m_CountTuples;
    p<size_t> m_PmemNode;
    p<bool> m_Init;

public:
    SkipListIndex(int p_PmemNode) : m_CountTuples(0), m_PmemNode(p_PmemNode), m_Init(false) {
        m_SkipList = pmem::obj::make_persistent<dbis::pskiplists::simplePSkiplist<uint64_t, pmem::obj::persistent_ptr<NodeBucketList<uint64_t>>, 8>>();
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

    pptr<const NodeBucketList<uint64_t>> find(uint64_t key)
    {
        pptr<NodeBucketList<uint64_t>> list;
        bool success = m_SkipList->search(key, list);
        if (!success)
            return nullptr;
        
        return list;
    }

    void insert(uint64_t key, uint64_t value)
    {
        pptr<NodeBucketList<uint64_t>> list;
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_PmemNode);

        if (m_SkipList->search(key, list)) {
            if (list == nullptr)
                transaction::run(pop, [&] {
                    list = make_persistent<NodeBucketList<uint64_t>>(m_PmemNode);
                });
            transaction::run(pop, [&] {
                list->insertValue(value); 
            });
        }
        else {
            transaction::run(pop, [&] {
                list = make_persistent<NodeBucketList<uint64_t>>(m_PmemNode);
            });
            if (list == nullptr)
                throw new std::runtime_error("out of memory");
            transaction::run(pop, [&] {
                m_SkipList->insert(key, list);
                list->insertValue(value);
            });
        }

        m_CountTuples++;
    }

    bool lookup(uint64_t key, uint64_t val) const
    {
        pptr<NodeBucketList<uint64_t>> list;
        bool success = m_SkipList->search(key, list);
        if (!success || list == nullptr)
            return false;

        return list->lookup(val);
    }

    bool deleteEntry(uint64_t key, uint64_t value)
    {
        pptr<NodeBucketList<uint64_t>> list;
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

    using ScanFunc = std::function<void(const uint64_t &key, const pptr<NodeBucketList<uint64_t>> &val)>;
    void scan(ScanFunc func) const
    {
        m_SkipList->scan(func);
    }

    void scanValue(const uint64_t &minKey, const uint64_t &maxKey, column<uncompr_f>* &outCol) const {

        //trace_l(T_INFO, "Got values ", minKey, " and ", maxKey);
        std::list<pptr<NodeBucketList<uint64_t>>> list;

        m_SkipList->scanValue(minKey, maxKey, list);
        size_t sum_count_values = 0;

        for (auto i : list) {
            sum_count_values += (*i).getCountValues();
        }

        outCol = new column<uncompr_f>(sizeof(uint64_t) * sum_count_values);
        uint64_t * data = outCol->get_data();

        for (auto i : list) {
            NodeBucketList<uint64_t>::Iterator iter = (*i).begin();
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
        auto printLambda = [&](const uint64_t &key, const pptr<NodeBucketList<uint64_t>> buck) {
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
