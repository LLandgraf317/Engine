#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/index/PHashMap.hpp>
#include <core/index/HashMapIndex.hpp>
#include <core/access/root.h>
#include <core/access/RootManager.h>
#include <core/storage/column_gen.h>
#include <core/storage/PersistentColumn.h>
#include <core/storage/column.h>
#include <vector/scalar/extension_scalar.h>
#include <core/tracing/trace.h>

#include <core/operators/general_vectorized/select_compr.h>
#include <core/operators/scalar/calc_uncompr.h>
#include <vector/primitives/compare.h>
#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>
#include <core/operators/scalar/select_uncompr.h>

#include <nvmdatastructures/src/pbptrees/PBPTree.hpp>

#include <libpmempool.h>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <algorithm>

using namespace vectorlib;
using namespace dbis::pbptrees;
using namespace morphstore;

void print(const morphstore::column<uncompr_f> * col1, const morphstore::column<uncompr_f> * col2)
{
    assert(col1->get_count_values() == col2->get_count_values());
    uint64_t * data1 = col1->get_data();
    uint64_t * data2 = col2->get_data();

    for (uint64_t i = 0; i < col1->get_count_values(); i++) {
        trace_l(T_INFO, *data1, " ", *data2);
        data1++; data2++;
    }

}

int main( void ) {

    //using ps = scalar<v64<uint64_t>>;

    RootInitializer::initPmemPool();
    RootManager& root_mgr = RootManager::getInstance();

    auto pop = root_mgr.getPop(0);

    const size_t ARRAY_SIZE = 4000;
    const size_t SEED = 42;

    trace_l(T_DEBUG, "Creating persistent column");
    pmem::obj::persistent_ptr<PersistentColumn> col = 
           generate_with_outliers_and_selectivity_pers(ARRAY_SIZE,
               0, 20, 0.5, 50, 60, 0.005, false, 0, SEED);

    //print(col->convert(), col->convert());

    trace_l(T_DEBUG, "Creating index");
    pmem::obj::persistent_ptr<HashMapIndex> index;
    transaction::run(pop, [&] {
        index =
            make_persistent<HashMapIndex>(61, 0, std::string(""), std::string(""), std::string(""));
    });

    trace_l(T_INFO, "Generating index");
    index->generateKeyToPos(col);
    trace_l(T_INFO, "Index construction finished");

    const column<uncompr_f> * outPosOrig = my_select_wit_t<equal, scalar<v64<uint64_t>>, uncompr_f, uncompr_f>::apply(col->convert(), 0);
    const column<uncompr_f> * outPosIndex = index->find(0);

    //print(outPosOrig, outPosIndex);

    uint64_t * const orig = outPosOrig->get_data();
    uint64_t * const ind = outPosIndex->get_data();

    trace_l(T_INFO, "Sorting");
    std::sort(orig, orig + outPosOrig->get_count_values());
    std::sort(ind, ind + outPosIndex->get_count_values());
    trace_l(T_INFO, "Sorting finished");

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
        delete_persistent<HashMapIndex>(index);
        delete_persistent<PersistentColumn>(col);
    });

    trace_l(T_INFO, "Success");
    return 0;
}
