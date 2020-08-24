#include <core/index/NodeBucketList.h>
#include <core/access/root.h>
#include <core/access/RootManager.h>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

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

    assert(iter.get() == 8);
    iter++;
    assert(iter.get() == 9);
    iter++;

    assert(iter == bucket_list->end());
   
    return 0;
}
