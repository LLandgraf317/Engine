#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/index/NodeBucketList.h>
#include <core/access/root.h>
#include <core/access/RootManager.h>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <core/tracing/trace.h>

using pmem::obj::transaction;
using namespace morphstore;

int main( void )
{
    constexpr auto ARRAY_SIZE = 10000ul / sizeof(uint64_t);
    RootInitializer::getInstance().initPmemPool(std::string("nodebuckettest"), std::string("NVMDS"), 50ul << 20);

    auto root_mgr = RootManager::getInstance();
    auto pop = root_mgr.getPop(0);

    pmem::obj::persistent_ptr<NodeBucketList<uint64_t>> bucket_list;

    transaction::run(pop, [&]() {
        bucket_list = make_persistent<NodeBucketList<uint64_t>>(0);
    });

    auto i = bucket_list->begin();
    assert(i == bucket_list->end());

    transaction::run(pop, [&]() {
        bucket_list->insertValue(8);
    });

    {
        auto iter = bucket_list->begin();

        bucket_list->printAll();
        bucket_list->printAllIter();

        assert(iter.get() == 8);
        iter++;
        assert(iter == bucket_list->end());
    }

    transaction::run(pop, [&]() {
        bucket_list->insertValue(9);
    });

    {
        auto iter = bucket_list->begin();

        bucket_list->printAll();
        bucket_list->printAllIter();

        assert(iter.get() == 8);
        iter++;
        assert(iter.get() == 9);
        iter++;
        assert(iter == bucket_list->end());
        assert(bucket_list->count_buckets() == 1);
    }

    for (uint64_t i = 0; i < ARRAY_SIZE; i++) {
        transaction::run(pop, [&]() {
            bucket_list->insertValue(i + 10);
        });
    }

    {
        auto iter = bucket_list->begin();
        uint64_t i = 0;

        while (iter != bucket_list->end()) {
            if (iter.get() != 8+i)
                std::cout << "Assertion failed, iter is " << iter.get() << ", should be " << 8+i << std::endl;
            assert(iter.get() == 8 + i);
            iter++;
            i++;
        }
    }

    RootInitializer::getInstance().cleanUp();
  
    trace_l(T_INFO, "Success");     
    return 0;
}
