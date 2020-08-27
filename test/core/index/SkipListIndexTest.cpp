#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/access/root.h>
#include <core/access/RootManager.h>
#include <core/storage/column_gen.h>
#include <core/operators/scalar/select_uncompr.h>
#include <core/operators/general_vectorized/select_compr.h>
#include <vector/primitives/compare.h>
#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>
#include <vector/scalar/extension_scalar.h>
#include <core/operators/scalar/calc_uncompr.h>

#include <core/index/SkipListIndex.hpp>

using namespace morphstore;
using namespace vectorlib;

int main( void ) {

    RootInitializer::initPmemPool();
    RootManager& root_mgr = RootManager::getInstance();

    auto pop = root_mgr.getPop(0);

    const size_t ARRAY_SIZE = 4000;
    const size_t SEED = 42;

    pmem::obj::persistent_ptr<SkipListIndex> index;

    transaction::run(pop, [&] {
        index = pmem::obj::make_persistent<SkipListIndex>(0);
    });
    pmem::obj::persistent_ptr<PersistentColumn> col = 
           generate_with_outliers_and_selectivity_pers(ARRAY_SIZE,
               0, 20, 0.5, 50, 60, 0.005, false, 0, SEED);

    index->generateKeyToPos(col);

    const column<uncompr_f> * outPosOrig = my_select_wit_t<equal, scalar<v64<uint64_t>>, uncompr_f, uncompr_f>::apply(col->convert(), 0);
    const column<uncompr_f> * outPosIndex = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, SkipListIndex>::apply(index, 0);

    uint64_t * const orig = outPosOrig->get_data();
    uint64_t * const ind = outPosIndex->get_data();

    std::sort(orig, orig + outPosOrig->get_count_values());
    std::sort(ind, ind + outPosIndex->get_count_values());

    const column<uncompr_f> * outComp = calc_binary<
                    std::minus,
                    scalar<v64<uint64_t>>,
                    uncompr_f,
                    uncompr_f,
                    uncompr_f>(outPosOrig, outPosIndex);

    const uint64_t * data = outComp->get_data();
    for (uint64_t i = 0; i < outComp->get_count_values(); i++)
        assert(*data++ == 0);

    transaction::run(pop, [&] {
        delete_persistent<SkipListIndex>(index);
        delete_persistent<PersistentColumn>(col);
    });

    return 0;
}
