#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/access/root.h>
#include <core/access/RootManager.h>
#include <core/storage/column_gen.h>

#include <core/operators/scalar/calc_uncompr.h>
#include <core/operators/general_vectorized/between_compr.h>
#include <core/operators/scalar/select_uncompr.h>
#include <core/operators/general_vectorized/select_compr.h>

#include <vector/primitives/compare.h>
#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>
#include <vector/scalar/extension_scalar.h>

#include <core/index/SkipListIndex.hpp>

using namespace morphstore;
using namespace vectorlib;

using ps = scalar<v64<uint64_t>>;

int main( void ) {

    RootInitializer::initPmemPool();
    RootManager& root_mgr = RootManager::getInstance();

    auto pop = root_mgr.getPop(0);

    const size_t ARRAY_SIZE = 20;
    const size_t SEED = 42;

    pmem::obj::persistent_ptr<SkipListIndex> index;

    transaction::run(pop, [&] {
        index = pmem::obj::make_persistent<SkipListIndex>(0);
    });
    pmem::obj::persistent_ptr<PersistentColumn> col = 
           generate_with_outliers_and_selectivity_pers(ARRAY_SIZE,
               0, 14, 0.5, 15, 20, 0.005, false, 0, SEED);
    const column<uncompr_f> * convCol = col->convert();

    index->generateKeyToPos(col);

    trace_l(T_INFO, "Orig val: ", convCol->get_count_values(), ", index val: ", index->getCountValues());

    {
        size_t sum_col = 0;
        size_t sum_ind = 0;

        for (uint64_t i = 0; i < 25; i++) {
            const column<uncompr_f> * outPosOrig = my_select_wit_t<equal, scalar<v64<uint64_t>>, uncompr_f, uncompr_f>::apply(convCol, i);
            const column<uncompr_f> * outPosIndex = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, SkipListIndex>::apply(index, i);

            trace_l(T_INFO, "orig selection: ", outPosOrig->get_count_values(), " poses");
            trace_l(T_INFO, "index selection: ", outPosIndex->get_count_values(), " poses");
            trace_l(T_INFO, "");

            sum_col += outPosOrig->get_count_values();
            sum_ind += outPosIndex->get_count_values();

            /*uint64_t * const orig = outPosOrig->get_data();
            uint64_t * const ind = outPosIndex->get_data();

            std::sort(orig, orig + outPosOrig->get_count_values());
            std::sort(ind, ind + outPosIndex->get_count_values());

            // Calculate difference in results
            const column<uncompr_f> * outComp = calc_binary<
                            std::minus,
                            scalar<v64<uint64_t>>,
                            uncompr_f,
                            uncompr_f,
                            uncompr_f>(outPosOrig, outPosIndex);


            // Check if output was identical
            const uint64_t * data = outComp->get_data();
            for (uint64_t i = 0; i < outComp->get_count_values(); i++)
                assert(*data++ == 0);

            delete outComp;*/
            delete outPosOrig;
            delete outPosIndex;
        }

        trace_l(T_INFO, "Sum of col: ", sum_col, ", sum of index: ", sum_ind);
    }

    // Between operator
    {
        const column<uncompr_f> * outPosOrig = my_between_wit_t<greaterequal, lessequal, ps, uncompr_f, uncompr_f >
                        ::apply( convCol, 51, 54, 0);
        const column<uncompr_f> * outPosIndex = index_between_wit_t<greaterequal, lessequal, uncompr_f, uncompr_f, SkipListIndex>
                        ::apply( index, 51, 54);

        trace_l(T_INFO, "orig selection: ", outPosOrig->get_count_values(), " poses");
        trace_l(T_INFO, "index selection: ", outPosIndex->get_count_values(), " poses");

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

        delete outComp;
        delete outPosOrig;
        delete outPosIndex;
    }

    // Cleanup

    transaction::run(pop, [&] {
        delete_persistent<SkipListIndex>(index);
        delete_persistent<PersistentColumn>(col);
    });

    return 0;
}
