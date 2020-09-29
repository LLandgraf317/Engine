#pragma once

#include <core/index/NodeBucketList.h>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pool.hpp>

#include <functional>
#include <list>

using pmem::obj::pool;

namespace morphstore {

template<class VectorExtension>
struct p_multiply_mod_hash {

    p<size_t> m_ModSize;

    p_multiply_mod_hash(size_t modsize) : m_ModSize(modsize) {}

    size_t apply(size_t num)
    {
        return num * num % m_ModSize; 
    }
};

//using namespace vectorlib;
// Polymorphism disallowed, hence no flexible utilization of template strategies
// Must be refitted to fit compression paradigm fully
template<class VectorExtension,
    typename KeyType,
    typename ValueType,
    uint64_t t_bucket_size>
class PHashMap {

    template <typename Object>
    using pptr = pmem::obj::persistent_ptr<Object>;
    using HashMapElem = std::tuple<KeyType, pptr<NodeBucketList<ValueType, t_bucket_size>>>; 

    pptr<pptr<NodeBucketList<HashMapElem, OSP_SIZE>>[]> m_Map;
    p<size_t> m_MapElemCount;
    p<size_t> m_PmemNode;

    p_multiply_mod_hash<VectorExtension> m_HashStrategy;
   
public:
    PHashMap(size_t p_DistinctElementCountEstimate, size_t pmemNode)
        :
         m_MapElemCount(p_DistinctElementCountEstimate),
         //m_SizeHelper{
         //   p_DistinctElementCountEstimate
         //},
         m_PmemNode(pmemNode),
            m_HashStrategy(p_DistinctElementCountEstimate)
    {
        m_Map = make_persistent<
            pptr<NodeBucketList<HashMapElem, OSP_SIZE>>[]
                >(p_DistinctElementCountEstimate);
        for (size_t i = 0; i<m_MapElemCount; i++)
            m_Map[i] = nullptr;
    }

    void insert(KeyType key, pptr<NodeBucketList<ValueType, t_bucket_size>> bucket)
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_PmemNode);

        auto offset = m_HashStrategy.apply(key);

        if (m_Map[offset] == nullptr) {
            transaction::run(pop, [&] {
                m_Map[offset] = make_persistent<NodeBucketList<HashMapElem, OSP_SIZE>>(m_PmemNode);
            });
        }
        m_Map[offset]->insertValue(std::make_tuple(key, bucket));
    }

    void insert(KeyType key, ValueType value)
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_PmemNode);

        auto offset = m_HashStrategy.apply(key);
        if (m_Map[offset] == nullptr) {
            transaction::run(pop, [&] {
                m_Map[offset] = make_persistent<NodeBucketList<HashMapElem, OSP_SIZE>>(m_PmemNode);

                pptr<NodeBucketList<ValueType, t_bucket_size>> tmp = make_persistent<NodeBucketList<ValueType, t_bucket_size>>(m_PmemNode);
                tmp->insertValue(value);
                m_Map[offset]->insertValue(std::make_tuple(key, tmp));
            });

            return;
        }

        auto iter = m_Map[offset]->begin();

        for (; iter != m_Map[offset]->end(); iter++) {
            if (std::get<0>(iter.get()) == key) {
                std::get<1>(iter.get())->insertValue(value);
                return;
            }
        }

        pptr<NodeBucketList<ValueType, t_bucket_size>> tmp;

        transaction::run(pop, [&] {
            tmp = make_persistent<NodeBucketList<ValueType, t_bucket_size>>(m_PmemNode);
        });
        tmp->insertValue(value);
        m_Map[offset]->insertValue(std::make_tuple(key, tmp));
    }

    HashMapElem lookup(KeyType key)
    {
        auto entry = m_Map[m_HashStrategy.apply(key)];

        if (entry == nullptr)
            return std::make_tuple(0, pptr<NodeBucketList<ValueType, t_bucket_size>>(nullptr));

        typename NodeBucketList<HashMapElem, OSP_SIZE>::Iterator iter = entry->begin();
        for (; iter != entry->end(); iter++) {
            if (std::get<0>(iter.get()) == key)
                return iter.get();
        }

        return std::make_tuple(0, pptr<NodeBucketList<ValueType, t_bucket_size>>(nullptr));
    }

    bool lookup(KeyType key, ValueType value)
    {
        auto entry = m_Map[m_HashStrategy.apply(key)];

        auto iter = entry->begin();
        for (; iter != entry->end(); iter++) {
            if (std::get<0>(iter.get()) == key) {
                auto valIter = std::get<1>(iter.get())->begin();

                for (; valIter != std::get<1>(iter.get())->end(); valIter++)
                    if (valIter.get() == value)
                        return true;

                return false;
            }
        }

        return false;
    }

    using ScanFunc = std::function<void(const KeyType &key, const pptr<NodeBucketList<ValueType, t_bucket_size>> &val)>;
    void apply(ScanFunc func)
    {
        for (size_t i = 0; i < m_MapElemCount; i++) {
            if (m_Map[i] == nullptr)
                continue;
            auto key_bucket_iter = m_Map[i]->begin();

            for (; key_bucket_iter != m_Map[i]->end(); key_bucket_iter++) {
                HashMapElem pair = key_bucket_iter.get();
                pptr<NodeBucketList<ValueType, t_bucket_size>> node_bucket = std::get<1>(pair);
                func(std::get<0>(pair), node_bucket);
            }
        }
    }

    void apply(const KeyType &minKey, const KeyType maxKey, ScanFunc func)
    {
        for (size_t i = 0; i < m_MapElemCount; i++) {
            if (m_Map[i] == nullptr)
                continue;

            auto key_bucket_iter = m_Map[i]->begin();
            for (; key_bucket_iter != m_Map[i]->end(); key_bucket_iter++) {
                HashMapElem pair = key_bucket_iter.get();
                KeyType key = std::get<0>(pair);

                if (key < minKey || key > maxKey)
                        continue;

                pptr<NodeBucketList<ValueType, t_bucket_size>> node_bucket = std::get<1>(pair);
                func(key, node_bucket);
            }
        }
    }

    inline void scanValue(const uint64_t &minKey, const uint64_t &maxKey, std::list<pptr<NodeBucketList<ValueType, t_bucket_size>>> &outList) const {
        for (size_t i = 0; i < m_MapElemCount; i++) {
            if (m_Map[i] == nullptr)
                continue;

            auto key_bucket_iter = m_Map[i]->begin();
            for (; key_bucket_iter != m_Map[i]->end(); key_bucket_iter++) {
                HashMapElem pair = key_bucket_iter.get();
                KeyType key = std::get<0>(pair);

                if (key < minKey)
                    continue;
                if (key > maxKey)
                    continue;

                pptr<NodeBucketList<ValueType, t_bucket_size>> value = std::get<1>(pair);
                outList.push_back(value);
            }
        }
    }

    bool erase(KeyType key, ValueType val)
    {
        auto entry = m_Map[m_HashStrategy.apply(key)];

        auto iter = entry->begin();

        for (; iter != entry->end(); iter++) {
             if (std::get<0>(iter.get()) == key) {
                 std::get<1>(iter.get())->deleteValue(val);
                 return true;
             }
        }

        return false;
    }

    bool erase(KeyType key)
    {
        auto entry = m_Map[m_HashStrategy.apply(key)]; 

        auto iter = entry->begin();

        for (; iter != entry->end(); iter++) {
             if (iter.get().first == key) {
                 entry->deleteValue(std::make_tuple(key, iter.get().second));

                 return true;
             }
        }
        
        return false;
    }
};

} //namespace morphstore
