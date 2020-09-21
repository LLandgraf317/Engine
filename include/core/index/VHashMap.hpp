#pragma once

#include <core/memory/management/abstract_mm.h>
#include <core/memory/management/general_mm.h>

#include <core/index/VNodeBucketList.h>
#include <core/index/HashMethod.h>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>

#include <functional>
#include <list>

namespace morphstore {

//using namespace vectorlib;
// Polymorphism disallowed, hence no flexible utilization of template strategies
// Must be refitted to fit compression paradigm fully
template<class VectorExtension,
    typename KeyType,
    typename ValueType>
class VHashMap {

    using HashMapElem = std::tuple<KeyType, VNodeBucketList<ValueType> *>; 

    VNodeBucketList<HashMapElem>** m_Map;
    size_t m_MapElemCount;
    size_t m_PmemNode;

    multiply_mod_hash<VectorExtension> m_HashStrategy;
   
public:
    VHashMap(size_t p_DistinctElementCountEstimate, size_t pmemNode)
        :
         m_MapElemCount(p_DistinctElementCountEstimate),
         //m_SizeHelper{
         //   p_DistinctElementCountEstimate
         //},
         m_PmemNode(pmemNode),
         m_HashStrategy(p_DistinctElementCountEstimate)
    {
        m_Map = reinterpret_cast<VNodeBucketList<HashMapElem>**>(general_memory_manager::get_instance().allocateNuma(
                p_DistinctElementCountEstimate * sizeof(VNodeBucketList<HashMapElem>*), pmemNode));

        for (size_t i = 0; i<m_MapElemCount; i++)
            m_Map[i] = nullptr;
    }

    ~VHashMap()
    {
        for (size_t i = 0; i<m_MapElemCount; i++)
            if (m_Map[i] != nullptr) {
                m_Map[i]->~VNodeBucketList();
            }
        general_memory_manager::get_instance().deallocateNuma(m_Map, m_MapElemCount * sizeof(VNodeBucketList<HashMapElem>*));
    }

    void insert(KeyType key, ValueType value)
    {
        auto offset = m_HashStrategy.apply(key);
        if (m_Map[offset] == nullptr) {
            m_Map[offset] = new VNodeBucketList<HashMapElem>(m_PmemNode);

            VNodeBucketList<ValueType> * tmp = new VNodeBucketList<ValueType>(m_PmemNode);
            tmp->insertValue(value);
            m_Map[offset]->insertValue(std::make_tuple(key, tmp));

            return;
        }

        auto iter = m_Map[offset]->begin();

        for (; iter != m_Map[offset]->end(); iter++) {
            if (std::get<0>(iter.get()) == key) {
                std::get<1>(iter.get())->insertValue(value);
                return;
            }
        }

        VNodeBucketList<ValueType> * tmp = new VNodeBucketList<ValueType>(m_PmemNode);
        tmp->insertValue(value);
        m_Map[offset]->insertValue(std::make_tuple(key, tmp));
    }

    HashMapElem lookup(KeyType key)
    {
        auto entry = m_Map[m_HashStrategy.apply(key)];

        if (entry == nullptr)
            return std::make_tuple(0, nullptr);

        typename VNodeBucketList<HashMapElem>::Iterator iter = entry->begin();
        for (; iter != entry->end(); iter++) {
            if (std::get<0>(iter.get()) == key)
                return iter.get();
        }

        return std::make_tuple(0, nullptr);
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

    using ScanFunc = std::function<void(const KeyType &key, VNodeBucketList<ValueType> * const val)>;
    void apply(ScanFunc func)
    {
        for (size_t i = 0; i < m_MapElemCount; i++) {
            if (m_Map[i] == nullptr)
                continue;
            auto key_bucket_iter = m_Map[i]->begin();

            for (; key_bucket_iter != m_Map[i]->end(); key_bucket_iter++) {
                HashMapElem pair = key_bucket_iter.get();
                VNodeBucketList<ValueType> * node_bucket = std::get<1>(pair);
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

                VNodeBucketList<ValueType> * node_bucket = std::get<1>(pair);
                func(key, node_bucket);
            }
        }
    }

    inline void scanValue(const uint64_t &minKey, const uint64_t &maxKey, std::list<VNodeBucketList<ValueType> *> &outList) const {
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

                VNodeBucketList<ValueType> * value = std::get<1>(pair);
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
