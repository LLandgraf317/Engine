#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/access/root.h>
#include <core/access/RootManager.h>
#include <core/storage/column_gen.h>

#include <core/operators/scalar/calc_uncompr.h>
#include <core/operators/general_vectorized/between_compr.h>
#include <core/operators/scalar/select_uncompr.h>
#include <core/operators/general_vectorized/select_compr.h>
#include <core/operators/scalar/agg_sum_uncompr.h>
#include <core/operators/scalar/join_uncompr.h>

#include <vector/primitives/compare.h>
#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>
#include <vector/scalar/extension_scalar.h>

#include <core/index/IndexDef.h>

#include <core/index/NodeBucketList.h>
#include <core/index/VNodeBucketList.h>
#include <core/index/index_gen.h>

pobj_alloc_class_desc alloc_class;

void pseudo_random_access(pptr<PersistentColumn> col)
{
    auto size = col->get_count_values();
    const size_t pages = size / OSP_SIZE;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, pages-1);

    uint64_t * data = col->get_data();
    uint64_t sum = 0;

    for (uint64_t i = 0; i < ITER_MAX; i++) {

        uint64_t pos = distrib(gen) * OSP_SIZE;
        uint64_t val = data[pos];
        sum += val;
    }
}

template<class t_index_structure_ptr>
void pseudo_random_access_index(t_index_structure_ptr index)
{
    auto size = index->getCountValues();

}

int main( void ) {
    // L3 Cache at target system is 32MB
    // lets break it

    pmem::obj::persistent_ptr<PersistentColumn> col0 = generate_sorted_unique_pers(8ul << 10, 0);
    pmem::obj::persistent_ptr<PersistentColumn> col1 = generate_sorted_unique_pers(8ul << 10, 0);

    pptr<MultiValTreeIndex> tree0;
    pptr<MultiValTreeIndex> tree1;

    transaction::run( pop, [&] () {
        tree0 = make_persistent<MultiValTreeIndex>(alloc_class, 0, std::string(""), std::string(""), std::string(""));
    });
    transaction::run( pop, [&] () {
        tree1 = make_persistent<MultiValTreeIndex>(alloc_class, 0, std::string(""), std::string(""), std::string(""));
    });

    IndexGen::generateFast<pptr<MultiValTreeIndex>, OSP_SIZE>(tree0, col0);
    IndexGen::generateFast<pptr<MultiValTreeIndex>, OSP_SIZE>(tree1, col1);


    return 0;
}
