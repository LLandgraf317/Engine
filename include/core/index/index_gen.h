#pragma once
#include <core/tracing/trace.h>

#include <libpmemobj++/persistent_ptr.hpp>
#include <core/storage/PersistentColumn.h>
#include <core/memory/constants.h>

#include <cassert>

namespace morphstore {

using pmem::obj::persistent_ptr;

class IndexGen {
public:

    template<class t_pptr>
    static void generateFromPersistentColumn(t_pptr index, persistent_ptr<PersistentColumn> keyCol, persistent_ptr<PersistentColumn> valueCol)
    {
        if (index->isInit()) return; // Should throw exception instead

        auto count_values = keyCol->get_count_values();
        uint64_t* key_data = keyCol->get_data();
        uint64_t* value_data = valueCol->get_data();

        //TODO: slow, much optimization potential
        for (size_t i = 0; i < count_values; i++) {
            index->insert(key_data[i], value_data[i]);
        }

        index->setInit();
    }

    //template< template <template <uint64_t t_bucket_size> class t_index> class t_pptr>
    template<class t_pptr>
    static void generateKeyToPos(t_pptr index, persistent_ptr<PersistentColumn> keyCol)
    {
        RootManager& root_mgr = RootManager::getInstance();
        if (index->isInit()) return; // Should throw exception

        auto count_values = keyCol->get_count_values();
        uint64_t * key_data = keyCol->get_data();

        //TODO: slow, much optimization potential
        for (size_t i = 0; i < count_values; i++) {
            index->insert(key_data[i], i);
            assert(index->lookup(key_data[i], i));
            if (i % 5000 == 0) {
                //trace_l(T_INFO, "Inserted key ", i);
                root_mgr.drainAll();
            }
        }

        index->setInit();
    }

    //template< template <template <uint64_t t_bucket_size> class t_index> class t_pptr>
    template<class t_pptr, uint64_t t_bucket_size>
    static void generateFast(t_pptr index, persistent_ptr<PersistentColumn> keyCol)
    {
        auto count_values = keyCol->get_count_values();
        const column<uncompr_f> * posCol = generate_sorted_unique(count_values, 0);
        auto keyColVol = keyCol->convert();
        batchInsert<t_pptr, t_bucket_size>(index, keyColVol, posCol);
        delete keyColVol;
    }

    template< class t_pptr, uint64_t t_bucket_size >
    static void batchInsert(t_pptr index, const column<uncompr_f> * keyCol, const column<uncompr_f> * valCol)
    {
        RootManager& root_mgr = RootManager::getInstance();
        size_t pmemNode = index->getPmemNode();
        auto pop = root_mgr.getPop(pmemNode);

        auto count_values = keyCol->get_count_values();
        uint64_t* key_data = keyCol->get_data();
        uint64_t* val_data = valCol->get_data();

        uint64_t index_count_values = 0;

        std::vector<std::tuple<uint64_t, uint64_t>> sortVec;
        for (size_t i = 0; i < count_values; i++) {
            sortVec.push_back(std::make_tuple(key_data[i], val_data[i]));
        }

        std::sort(sortVec.begin(), sortVec.end(), [](std::tuple<uint64_t, uint64_t> a, std::tuple<uint64_t, uint64_t> b) {
                    return std::get<0>(a) < std::get<0>(b);
                });
        //trace_l(T_INFO, "Done with sorting");

        uint64_t currentKey = std::get<0>(*sortVec.begin());
        //trace_l(T_INFO, "First key is ", currentKey);
        persistent_ptr<NodeBucketList<uint64_t, t_bucket_size>> currentList = index->find(currentKey);

        if (currentList == nullptr) {
            transaction::run(pop, [&] {
                currentList = make_persistent<NodeBucketList<uint64_t, t_bucket_size>>(pmemNode);
            });
            index->getDS()->insert(currentKey, currentList);
        }
        
        persistent_ptr<NodeBucket<uint64_t, t_bucket_size>> currentBucket = currentList->last;
        
        if (currentBucket == nullptr) {
            transaction::run(pop, [&] {
                currentBucket = make_persistent<NodeBucket<uint64_t, t_bucket_size>>();
            });
            currentList->first = currentBucket;
            currentList->last = currentBucket;
        }

        auto iter = sortVec.begin();
        // we try to circumvent persistent writes and just call it on the respective NodeBucket
        size_t insertCount = 0;
        while (iter != sortVec.end()) {

            if (currentKey != std::get<0>(*iter)) {
                //trace_l(T_INFO, "Inserted for key ", currentKey, " value count ", insertCount);
                currentKey = std::get<0>(*iter);
                //trace_l(T_INFO, "Got new key ", currentKey);

                currentList->setCountValues(insertCount);
                index_count_values += insertCount;
                insertCount = 0;
                currentList = index->find(currentKey);
                if (currentList == nullptr) {
                    transaction::run(pop, [&] {
                        currentList = make_persistent<NodeBucketList<uint64_t, t_bucket_size>>(pmemNode);
                    });
                    index->getDS()->insert(currentKey, currentList);
                }

                if (currentList->last == nullptr) {
                    persistent_ptr<NodeBucket<uint64_t, t_bucket_size>> tmp;
                    transaction::run(pop, [&] {
                        tmp = make_persistent<NodeBucket<uint64_t, t_bucket_size>>();
                    });
                    currentList->first = tmp;
                    currentList->last = tmp;
                    currentBucket = tmp;
                }
                else {
                    currentBucket = currentList->last;
                }
            }

            if (currentBucket->isFull()) {
                persistent_ptr<NodeBucket<uint64_t, t_bucket_size>> tmp;
                transaction::run(pop, [&] {
                    tmp = make_persistent<NodeBucket<uint64_t, t_bucket_size>>();
                });
                currentBucket->next = tmp;
                tmp->prev = currentBucket;
                currentBucket = tmp;
            }
            currentBucket->insertLast(std::get<1>(*iter));
            insertCount++;

            iter++;
        }

        //trace_l(T_INFO, "Inserted for key ", currentKey, " value count ", insertCount);
        currentList->setCountValues(insertCount);
        index_count_values += insertCount;
        index->m_CountTuples = index_count_values;
    }

};

}
