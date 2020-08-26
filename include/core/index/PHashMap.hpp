#pragma once

#include <core/index/NodeBucketList.h>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>

#include <functional>

namespace morphstore {

template<class VectorExtension>
struct multiply_mod_hash {

    size_t m_ModSize;
    multiply_mod_hash(size_t modsize) : m_ModSize(modsize) {}

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
    typename ValueType>
class PHashMap {
    template <typename Object>
    using pptr = pmem::obj::persistent_ptr<Object>;

    using HashMapElem = std::tuple<KeyType, pptr<NodeBucketList<ValueType>>>; 
    pptr<pptr<NodeBucketList<HashMapElem>>[]> m_Map;
    size_t m_MapElemCount;

    //size_helper<VectorExtension, 60, size_policy_hash::EXPONENTIAL> const m_SizeHelper;
    multiply_mod_hash<VectorExtension> m_HashStrategy;
    //scalar_key_vectorized_linear_search<VectorExtension, VectorExtension, multiply_mod_hash, size_policy_hash::EXPONENTIAL> m_SearchStratey;
   
public:
    PHashMap(size_t p_DistinctElementCountEstimate)
        :
         m_MapElemCount(p_DistinctElementCountEstimate),
         //m_SizeHelper{
         //   p_DistinctElementCountEstimate
         //},
            m_HashStrategy(p_DistinctElementCountEstimate)
    {
        m_Map = make_persistent<
            pptr<NodeBucketList<HashMapElem>>[]
                >(p_DistinctElementCountEstimate);
        for (size_t i = 0; i<m_MapElemCount; i++)
            m_Map[i] = nullptr;
    }

    void insert(KeyType key, ValueType value)
    {
        auto offset = m_HashStrategy.apply(key);
        if (m_Map[offset] == nullptr) {
            m_Map[offset] = make_persistent<NodeBucketList<HashMapElem>>();

            pptr<NodeBucketList<ValueType>> tmp = make_persistent<NodeBucketList<ValueType>>();
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

        pptr<NodeBucketList<ValueType>> tmp = make_persistent<NodeBucketList<ValueType>>();
        tmp->insertValue(value);
        m_Map[offset]->insertValue(std::make_tuple(key, tmp));
    }

    HashMapElem lookup(KeyType key)
    {
        auto entry = m_Map[m_HashStrategy.apply(key)];

        typename NodeBucketList<HashMapElem>::Iterator iter = entry->begin();
        for (; iter != entry->end(); iter++) {
            if (std::get<0>(iter.get()) == key)
                return iter.get();
        }

        return std::make_tuple(0, pptr<NodeBucketList<ValueType>>(nullptr));
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

    using ScanFunc = std::function<void(const KeyType &key, const pptr<NodeBucketList<ValueType>> &val)>;
    void apply(KeyType key, ScanFunc func)
    {
        auto i = std::get<1>(this->lookup(key));
        func(key, i);

        /*auto iter = i->begin();
        for (; iter != i->end(); iter++) {
            func(key, iter.get());
        }*/
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
