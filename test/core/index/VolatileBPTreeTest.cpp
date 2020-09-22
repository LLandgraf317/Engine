#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/index/VNodeBucketList.h>
#include <core/index/VolatileBPTree.hpp>

using namespace morphstore;
using namespace vectorlib;

int main( void ) {
    VolatileBPTree<uint64_t, uint64_t, 10, 10> * tree = new VolatileBPTree<uint64_t, uint64_t, 10, 10>(0);

    for (uint64_t i = 0; i < 1000; i++)
        tree->insert(i,i);

    uint64_t val;
    for (uint64_t i = 0; i < 1000; i++) {
        tree->lookup(i, &val);
        assert(val == i);
    }

    return 0;
}
