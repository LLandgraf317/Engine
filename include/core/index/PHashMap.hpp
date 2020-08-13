#pragma once

#include <vector/complex/hash.h>
#include <core/index/NodeBucketList.h>
#include <vector/datastructures/hash_based/hash_utils.h>
#include <vector/datastructures/hash_based/strategies/linear_probing.h>

namespace morphstore {

using namespace vectorlib;
// Polymorphism disallowed, hence no flexible utilization of template strategies
// Must be refitted to fit compression paradigm fully
template<class VectorExtension,
    typename KeyType,
    typename ValueType>
class PHashMap {
    //  multiply_mod_hash,
    
    pptr<NodeBucketList<KeyType>[]> m_Keys;
    pptr<NodeBucketList<std::tuple<KeyType, ValueType>>[]> m_Values;

    size_helper<VectorExtension, 60, size_policy_hash::EXPONENTIAL> const m_SizeHelper;
    multiply_mod_hash<VectorExtension> m_HashStrategy;
    scalar_key_vectorized_linear_search<VectorExtension, VectorExtension, multiply_mod_hash, size_policy_hash::EXPONENTIAL> m_SearchStratey;

    PHashMap(size_t p_DistinctElementCountEstimate)
        : 
         m_SizeHelper{
            p_DistinctElementCountEstimate
         }
    {
        m_Keys =   pmemobj_tx_alloc(p_DistinctElementCountEstimate * sizeof(KeyType), 0);
        m_Values = pmemobj_tx_alloc(p_DistinctElementCountEstimate * sizeof(ValueType), 0);

        std::fill(m_Keys, m_Keys+m_SizeHelper.m_Count, 0);
        std::fill(m_Values, m_Values+m_SizeHelper.m_Count, nullptr);
    }

    void insert(KeyType key, ValueType value)
    {
        m_Keys[m_HashStrategy.apply(key)].insertValue(key);
        m_Values[m_HashStrategy.apply(key)] = make_persistent<NodeBucketList<std::tuple<KeyType, ValueType>>>();
    }

    NodeBucketList<std::tuple<KeyType, ValueType>> lookup(KeyType key)
    {
        return m_Values[m_HashStrategy.apply(key)];
    }

    bool erase(KeyType key)
    {
        m_Keys[m_HashStrategy.apply(key)].deleteValue(key); 
        // iterate the buckets for all entries containing the key
        auto nodeBucketList = m_Values[m_HashStrategy.apply(key)];
        auto bucketIter = nodeBucketList->begin();
        bool success = false;

        for (; bucketIter != nodeBucketList->end(); bucketIter++) {
            while (bucketIter.get().first == key) {
                nodeBucketList.deleteValue(bucketIter.get().second); 
                success = true;
            }
        }

        return success;
    }
};

} //namespace morphstore
