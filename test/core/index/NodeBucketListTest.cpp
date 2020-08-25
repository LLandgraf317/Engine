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
    RootInitializer::initPmemPool();

    auto root_mgr = RootManager::getInstance();
    auto pop = root_mgr.getPop(0);

    pmem::obj::persistent_ptr<NodeBucketList<uint64_t>> bucket_list;

    transaction::run(pop, [&]() {
        bucket_list = make_persistent<NodeBucketList<uint64_t>>();
    });

    transaction::run(pop, [&]() {
        bucket_list->insertValue(8);
        bucket_list->insertValue(9);
    });

    auto iter = bucket_list->begin();

    bucket_list->printAll();
    bucket_list->printAllIter();

    assert(iter.get() == 8);
    iter++;
    assert(iter.get() == 9);
    iter++;

    assert(iter == bucket_list->end());
  
    trace_l(T_INFO, "Success");     
    return 0;
}
