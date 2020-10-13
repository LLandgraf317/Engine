#pragma once

#include <core/tracing/trace.h>
#include <core/access/RootManager.h>
#include <core/memory/constants.h>

#include <stdexcept>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>

using pmem::obj::make_persistent;
using pmem::obj::make_persistent_atomic;
using pmem::obj::persistent_ptr;
using pmem::obj::delete_persistent;
using pmem::obj::p;
using pmem::obj::transaction;

using morphstore::OSP_SIZE;

template<typename T, unsigned t_bucket_size>
struct NodeBucket {
    template <typename Object>
    using pptr = pmem::obj::persistent_ptr<Object>;

    uint64_t fill_count;
    pptr<NodeBucket<T, t_bucket_size>> next;
    pptr<NodeBucket<T, t_bucket_size>> prev;
    T bucket_list[(t_bucket_size - sizeof(fill_count) - 2*sizeof(pptr<NodeBucket<T, t_bucket_size>>)) / sizeof(T)];

    static const size_t MAX_ENTRIES = (t_bucket_size - sizeof(fill_count) - 2*sizeof(pptr<NodeBucket<T, t_bucket_size>>)) / sizeof(T);

public:
    NodeBucket()
    {
        this->fill_count = 0;
        this->next = nullptr;
        this->prev = nullptr;
    }        

    bool isEmpty()
    {
        return this->fill_count == 0;
    }

    inline T getBucketEntry(uint64_t i)
    {
        return bucket_list[i];
    }

    inline void setBucketEntry(uint64_t i, T val)
    {
        assert(i <= MAX_ENTRIES);
        bucket_list[i] = val;
    }

    pptr<NodeBucket<T, t_bucket_size>> getNext()
    {
        return next;
    }

    pptr<NodeBucket<T, t_bucket_size>> getPrev()
    {
        return prev;
    }

    void setNext(pptr<NodeBucket<T, t_bucket_size>> next)
    {
        this->next = next;
    }

    void setPrev(pptr<NodeBucket<T, t_bucket_size>> prev)
    {
        this->prev = prev;
    }

    inline bool isFull()
    {
        return fill_count == MAX_ENTRIES;
    }

    inline void insertLast(T val)
    {
        assert(fill_count < MAX_ENTRIES);
        bucket_list[fill_count] = val;
        fill_count++;
    }

    inline T getLastAndDecr()
    {
        assert(fill_count != 0);
        return bucket_list[fill_count--];
    }

};

template<typename T, unsigned t_bucket_size = OSP_SIZE>
struct NodeBucketList {
    template <typename Object>
    using pptr = pmem::obj::persistent_ptr<Object>;

    p<size_t> value_count;
    p<size_t> m_PmemNode;

    pptr<NodeBucket<T, t_bucket_size>> first;
    pptr<NodeBucket<T, t_bucket_size>> last;

    class Iterator {
        pptr<NodeBucket<T, t_bucket_size>> curr;
        uint64_t iterator_count;

      public:	
        Iterator(const Iterator& iter)
        {
            curr = iter.curr;
            iterator_count = iter.iterator_count;
        }

        Iterator& operator=(const Iterator& iter)
        {
            return Iterator(iter.curr, iter.iterator_count);
        }

        Iterator(pptr<NodeBucket<T, t_bucket_size>> but, uint64_t it) : curr(but), iterator_count(it)
        { }

        inline void operator++()
        {
            if (iterator_count + 1 < curr->fill_count) {
                iterator_count++;
            }
            else {
                curr = curr->getNext();
                iterator_count = 0;
            }
        }

        inline void operator++(int) {
                this->operator++();
        }

        inline friend bool operator==(const Iterator& lhs, const Iterator& rhs)
        {
            return lhs.curr == rhs.curr && lhs.iterator_count == rhs.iterator_count;
        }

        inline friend bool operator!=(const Iterator& lhs, const Iterator& rhs)
        {
            return lhs.curr != rhs.curr || lhs.iterator_count != rhs.iterator_count;
        }

        T operator*()
        {
            return curr->getBucketEntry(iterator_count);
        }

        inline T get()
        {
            return curr->getBucketEntry(iterator_count);
        }
    };

    static size_t max_entries_per_bucket()
    {
        return NodeBucket<T, t_bucket_size>::MAX_ENTRIES;
    }

    void prepareDest()
    {
        morphstore::RootManager& mgr = morphstore::RootManager::getInstance();
        pmem::obj::pool<morphstore::root> pop = mgr.getPop(m_PmemNode);

        auto next = first;
        while (next != nullptr) {
            auto del = next;
            next = next->next;
            transaction::run(pop,[&]() {
                delete_persistent<NodeBucket<T, t_bucket_size>>(del);
            });
        }
    }

    inline Iterator begin() const
    {
        return Iterator(first, 0);
    }

    inline Iterator end() const
    {
        return Iterator(nullptr, 0);
    }

    size_t memory_footprint()
    {
        size_t sum = 0;
        auto curr = first;
        
        while (curr != nullptr) {
           sum += sizeof(NodeBucket<T, t_bucket_size>);
           curr = curr->next;
        }
        sum += sizeof(NodeBucketList<T, t_bucket_size>);
        return sum; 
    }

    size_t count_buckets()
    {
        uint64_t count = 0;
        auto curr = first;
        while (curr != nullptr) {
            count++;
            curr = curr->next;
        }

        return count;
    }

    NodeBucketList(size_t pmemNode)
    {
        value_count = 0;
        first = nullptr;
        last = nullptr;
        m_PmemNode = pmemNode;
    }

    //TODO: need to nail pre and post conditions
    inline bool isEmpty() const
    {
        return first == nullptr || first->fill_count == 0;
    }

    size_t getSum() const
    {
        if (first == nullptr)
            return 0; // should we throw an exception?

        size_t sum = 0;
        auto it = first;
        while (it != nullptr) {
            for (uint64_t i = 0; i < it->fill_count; i++) {
                sum += it->getBucketEntry(i);
            }
            it = it->getNext();
        }

        return sum;
    }

    size_t getCountValues() const
    {
        return value_count;
    }

    void recalculateCountValues()
    {
        size_t tmp = 0;
        auto curr = first;
        while (curr != nullptr) {
            tmp += curr->fill_count;
            curr = curr->next;
        }

        value_count = tmp;
    }

    void setCountValues(size_t count)
    {
        value_count = count;
    }

    bool lookup(T val) const
    {
        if (first == nullptr)
            return false; // should we throw an exception?

        auto it = first;
        while (it != nullptr) {
            for (uint64_t i = 0; i < it->fill_count; i++) {
                if (it->getBucketEntry(i) == val) {
                    return true;
                }
            }
            it = it->getNext();
        }
        return false;
    }

    inline void insertValue(T val)
    {

        morphstore::RootManager& mgr = morphstore::RootManager::getInstance();
        pmem::obj::pool<morphstore::root> pop = mgr.getPop(m_PmemNode);

        if (first == nullptr) {
            transaction::run(pop, [&] {
                first = make_persistent<NodeBucket<T, t_bucket_size>>();
                assert(first->next == nullptr);
                assert(first->prev == nullptr);

                last = first;

                first->insertLast(val);
            });
        }
        else {
            if (last->isFull()) {
                trace_l(T_DEBUG, "Last bucket is full");
                persistent_ptr<NodeBucket<T, t_bucket_size>> tmp;
                transaction::run(pop, [&] {
                    tmp = make_persistent<NodeBucket<T, t_bucket_size>>();
                    if (tmp == nullptr)
                        throw new std::runtime_error("out of memory");
                    tmp->insertLast(val);
                });

                last->setNext(tmp);
                tmp->setPrev(last);
                last = tmp;
            }
            else {
                transaction::run(pop, [&] {
                    last->insertLast(val);
                });
            }
        }

        value_count = value_count + 1;
    }

    bool deleteValue(T val)
    {
        auto it = first;
        
        while (it != nullptr) {
            for (uint64_t i = 0; i < it->fill_count; i++) {
                if (it->getBucketEntry(i) == val) {
                    if (last->isEmpty()) {
                        auto toDel = last;
                        if (last->getPrev() != nullptr) {
                            last = last->getPrev();
                            last->setNext(nullptr);
                        }
                        else {
                            first = nullptr;
                            last = nullptr;
                        }
                        delete_persistent<NodeBucket<T, t_bucket_size>>(toDel);
                    }

                    auto tmp = last->getLastAndDecr();
                    it->bucket_list[i] = tmp;
                    value_count = value_count - 1;
                    return true;
                }
            }
            it = it->getNext();
        }
        return false;
    }

    void printAll()
    {
        auto curr = first;
        while (curr != nullptr) {
            for (uint32_t c = 0; c < curr->fill_count; c++) {
                trace_l(T_INFO, curr->getBucketEntry(c));
            }

            curr = curr->getNext();
        }
    }

    void printAllIter()
    {
        auto curr = begin();

        while (curr != end())
        {
            trace_l(T_INFO, curr.get());
            curr++;
        }
    }
};
