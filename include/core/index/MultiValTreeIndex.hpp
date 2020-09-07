#pragma once

#include <core/index/TreeDef.h>
#include <core/index/NodeBucketList.h>
#include <core/storage/PersistentColumn.h>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>

namespace morphstore {

class MultiValTreeIndex {

    pptr<MultiValTree> m_Tree;
    pptr<char[]> m_Relation;
    pptr<char[]> m_Attribute;
    pptr<char[]> m_Table;

    size_t m_PmemNode;
    bool m_Init;
    size_t m_CountTuples;

public:
    MultiValTreeIndex(uint64_t pMemNode, pobj_alloc_class_desc alloc_class, std::string table, std::string relation, std::string attribute)
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), pMemNode);

        m_PmemNode = pMemNode;

        m_Table = make_persistent<char[]>(table.length() + 1);
        m_Attribute = make_persistent<char[]>(attribute.length() + 1);
        m_Relation = make_persistent<char[]>(relation.length() + 1);

        m_Tree = make_persistent<MultiValTree>(alloc_class);

        pop.memcpy_persist(m_Table.raw_ptr(), table.c_str(), table.length() + 1);
        pop.memcpy_persist(m_Attribute.raw_ptr(), attribute.c_str(), attribute.length() + 1);
        pop.memcpy_persist(m_Relation.raw_ptr(), relation.c_str(), relation.length() + 1);

        m_Init = false;
        m_CountTuples = 0;
    }

    void generateFromPersistentColumn(pptr<PersistentColumn> keyCol, pptr<PersistentColumn> valueCol)
    {
        if (m_Init) return; // Should throw exception instead

        auto count_values = keyCol->get_count_values();
        uint64_t* key_data = keyCol->get_data();
        uint64_t* value_data = valueCol->get_data();
        RootManager& root_mgr = RootManager::getInstance();

        //TODO: slow, much optimization potential
        for (size_t i = 0; i < count_values; i++) {
            insert(key_data[i], value_data[i]);
            if (i % 200 == 0)
                root_mgr.drainAll();
        }

        m_Init = true;
    }

    void generateKeyToPos(pptr<PersistentColumn> keyCol)
    {
        if (m_Init) return; // Should throw exception

        auto count_values = keyCol->get_count_values();
        uint64_t* key_data = keyCol->get_data();

        RootManager& root_mgr = RootManager::getInstance();
        //TODO: slow, much optimization potential
        for (size_t i = 0; i < count_values; i++) {
            insert(key_data[i], i);
            if (i % 200 == 0)
                root_mgr.drainAll();
        }

        m_Init = true;
    }

    size_t getCountValues() const
    {
        return m_CountTuples;
    }

    pptr<NodeBucketList<uint64_t>> find(uint64_t key)
    {
        pptr<NodeBucketList<uint64_t>> list;
        bool success = m_Tree->lookup(key, &list);
       
        if (success)
           return list;
        else
           return nullptr; 
    }

    void insert(uint64_t key, uint64_t value)
    {

        pptr<NodeBucketList<uint64_t>> list;
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_PmemNode);

        if (m_Tree->lookup(key, &list)) {
            transaction::run(pop, [&] {
                list->insertValue(value); 
            });
        }
        else {
            transaction::run(pop, [&] {
                list = make_persistent<NodeBucketList<uint64_t>>();
                m_Tree->insert(key, list);
                list->insertValue(value);
            });
        }

        m_CountTuples++;
    }

    bool lookup(uint64_t key, uint64_t val)
    {
        pptr<NodeBucketList<uint64_t>> list;

        if (m_Tree->lookup(key, &list)) {
            return list->lookup(val);
        }

        return false;
    }

    using ScanFunc = std::function<void(const uint64_t &key, const pptr<NodeBucketList<uint64_t>> &val)>;
    void scan(const uint64_t &minKey, const uint64_t &maxKey, ScanFunc func) const {
        m_Tree->scan(minKey, maxKey, func);
    }

    inline void scanValue(const uint64_t &minKey, const uint64_t &maxKey, column<uncompr_f>* &col) const {

        std::list<pptr<NodeBucketList<uint64_t>>> list;
        m_Tree->scanValue(minKey, maxKey, list);
        size_t sum_count_values = 0;

        for (auto i : list) {
            sum_count_values += (*i).getCountValues();
        }

        col = new column<uncompr_f>(sizeof(uint64_t) * sum_count_values);
        uint64_t * data = col->get_data();

        for (auto i : list) {
            NodeBucketList<uint64_t>::Iterator iter = (*i).begin();

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
        pptr<NodeBucketList<uint64_t>> list;

        if (m_Tree->lookup(key, &list)) {
            bool success = list->deleteValue(value);
            if (list->isEmpty())
                m_Tree->erase(key);
	    m_CountTuples--;
            return success;
        }

        return false;
    }

};

} // namespace morphstore
