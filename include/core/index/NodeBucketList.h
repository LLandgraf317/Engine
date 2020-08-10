#pragma once

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>

using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::delete_persistent;


template<typename T>
struct NodeBucket {
    template <typename Object>
    using pptr = pmem::obj::persistent_ptr<Object>;

    uint16_t fill_count;
    pptr<NodeBucket<T>> next;
    pptr<NodeBucket<T>> prev;
    T bucket_list[(4096 - sizeof(fill_count) - 2*sizeof(pptr<NodeBucket>)) / sizeof(T)];

    const size_t MAX_ENTRIES = (4096 - sizeof(fill_count) - 2*sizeof(pptr<NodeBucket<T>>)) / sizeof(T);

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

    T getBucketEntry(uint64_t i)
    {
        return bucket_list[i];
    }

    void setBucketEntry(uint64_t i, T val)
    {
        assert(i <= MAX_ENTRIES);
        bucket_list[i] = val;
    }

    pptr<NodeBucket<T>> getNext()
    {
        return next;
    }

    pptr<NodeBucket<T>> getPrev()
    {
        return prev;
    }

    void setNext(pptr<NodeBucket<T>> next)
    {
        this->next = next;
    }

    void setPrev(pptr<NodeBucket<T>> prev)
    {
        this->prev = prev;
    }

    bool isFull()
    {
        return fill_count < MAX_ENTRIES;
    }

    void insertLast(T val)
    {
        assert(fill_count < MAX_ENTRIES);
        bucket_list[fill_count++] = val;
    }

    T getLastAndDecr()
    {
        assert(fill_count != 0);
        return bucket_list[fill_count--];
    }

};

template<typename T>
struct NodeBucketList {
    template <typename Object>
    using pptr = pmem::obj::persistent_ptr<Object>;

    pptr<NodeBucket<T>> first;
    pptr<NodeBucket<T>> last;

    NodeBucketList()
    {
        first = nullptr;
        last = nullptr;
    }

    //TODO: need to nail pre and post conditions
    bool isEmpty()
    {
        return first == nullptr || first->fill_count == 0;
    }

    size_t getSum()
    {
        if (first == nullptr)
            return 0; // should we throw an exception?

        size_t sum = 0;
        auto it = first;
        while (it != nullptr) {
            for (uint64_t i = 0; i < it->fill_count; i++) {
                sum += it->getEntry(i);
            }
            it = it->getNext();
        }

        return sum;
    }

    size_t getCountValues()
    {
        if (first == nullptr)
            return 0; // should we throw an exception?

        size_t sum = 0;
        auto it = first;
        while (it != nullptr) {
            sum += it->fill_count;
            it = it->getNext();
        }

        return sum;
    }

    bool lookup(T val)
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

    void insertValue(T val)
    {
        if (first == nullptr) {
            first = make_persistent<NodeBucket<T>>();
            last = first;

            first->insertLast(val);
        }
        else {
            if (last->isFull()) {
                auto tmp = make_persistent<NodeBucket<T>>();
                tmp->insertLast(val);
                last->setNext(tmp);
                tmp->setPrev(last);
                last = tmp;
            }
            else {
                last->insertLast(val);
            }
        }
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
                        delete_persistent<NodeBucket<T>>(toDel);
                    }

                    auto tmp = last->getLastAndDecr();
                    it->bucket_list[i] = tmp;
                    return true;
                }
            }
            it = it->getNext();
        }
        return false;
    }
};
