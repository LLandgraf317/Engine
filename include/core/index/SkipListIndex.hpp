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
    pmem::obj::persistent_ptr<simplePSkiplist<uint64_t, pmem::obj::persistent_ptr<NodeBucketList<uint64_t>>, 8>> m_SkipList;
    size_t m_CountTuples;
    size_t m_PmemNode;
    bool m_Init;

public:
    SkipListIndex(int p_PmemNode, uint64_t maxKey) : m_CountTuples(0), m_PmemNode(p_PmemNode), m_Init(false) {
        m_SkipList = pmem::obj::make_persistent<simplePSkiplist<uint64_t, pmem::obj::persistent_ptr<NodeBucketList<uint64_t>>, 8>>(maxKey);
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

    pptr<NodeBucketList<uint64_t>> find(uint64_t key)
    {
        pptr<NodeBucketList<uint64_t>> list;
        auto node = m_SkipList->search(key);
       
        if (node != nullptr)
           return node->value;
        else
           return nullptr; 
    }

    bool insert(uint64_t key, uint64_t value)
    {
        pptr<NodeBucketList<uint64_t>> list;
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_PmemNode);

        auto node = m_SkipList->search(key);
        
        if (node == nullptr) {
            list = pmem::obj::make_persistent<NodeBucketList<uint64_t>>();
            list->insertValue(value);
            m_SkipList->insert(key, list);
        }
        else {
            node->value.get_rw()->insertValue(value);
        }

        m_CountTuples++;
        return true;
    }

    bool lookup(uint64_t key, uint64_t val)
    {
        auto node = m_SkipList->search(key);

        return node->value.get_rw()->lookup(val);
    }

    bool deleteEntry(uint64_t key, uint64_t value)
    {
        auto node = m_SkipList->search(key);

        return node->value.get_rw()->deleteValue(value);
    }

    using ScanFunc = std::function<void(const uint64_t &key, const pptr<NodeBucketList<uint64_t>> &val)>;
    void scan(ScanFunc /*func*/) const
    {
        //m_Tree->scan(func);
    }
};

} // namespace morphstore
