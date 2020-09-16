#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/index/VHashMap.hpp>
#include <vector/scalar/extension_scalar.h>
#include <core/index/VNodeBucketList.h>

using namespace morphstore;
using namespace vectorlib;
using ps = vectorlib::scalar<vectorlib::v64<uint64_t>>;

int main( void ) {
    VHashMap<ps, uint64_t, uint64_t> * index = new VHashMap<ps, uint64_t, uint64_t>(60, 0);

    for (uint64_t i = 0; i < 1000; i++) {
        index->insert(i,i);
    }


    for (uint64_t i = 0; i < 1000; i++) {
        auto elem = index->lookup(i);
        assert(std::get<1>(elem)->begin().get() == i);
    }

    //print(outPosOrig, outPosIndex);

    trace_l(T_INFO, "Success");
    return 0;
}
