#pragma once

#include <core/storage/PersistentColumn.h>
#include <core/index/NodeBucketList.h>

#include <nvmdatastructures/src/pskiplists/simplePSkiplist.hpp>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

namespace morphstore {

class SkipListIndex {
private:
    pmem::obj::persistent_ptr<dbis::pskiplists::simplePSkiplist<uint64_t, pmem::obj::persistent_ptr<NodeBucketList<uint64_t>>, 8>> m_SkipList;
    size_t m_CountTuples;
    size_t m_PmemNode;
    bool m_Init;

public:
    SkipListIndex(int p_PmemNode) : m_CountTuples(0), m_PmemNode(p_PmemNode), m_Init(false) {
        m_SkipList = pmem::obj::make_persistent<dbis::pskiplists::simplePSkiplist<uint64_t, pmem::obj::persistent_ptr<NodeBucketList<uint64_t>>, 8>>();
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
            transaction::run(pop, [&] {
                list->insertValue(value); 
            });
        }
        else {
            transaction::run(pop, [&] {
                list = make_persistent<NodeBucketList<uint64_t>>();
                m_SkipList->insert(key, list);
                list->insertValue(value);
            });
        }

        trace_l(T_INFO, "Inserting ", key, "; ", value);
        printContents();

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

    inline void scanValue(const uint64_t &minKey, const uint64_t &maxKey, column<uncompr_f>* &outCol) const {
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
            trace_l(T_INFO, "key: ", key, ", value_count: ", buck->getCountValues());
            trace_l(T_INFO, "values: ");
            for (auto iter = buck->begin(); iter != buck->end(); iter++) {
                trace_l(T_INFO, iter.get());
            } 
            trace_l(T_INFO, "");
        };

        m_SkipList->scan(printLambda);
    }
};

} // namespace morphstore
