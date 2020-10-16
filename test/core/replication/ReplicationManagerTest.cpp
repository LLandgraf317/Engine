
#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/access/root.h>
#include <core/access/RootManager.h>
#include <core/replication/ReplicationManager.h>
#include <core/storage/column_gen.h>

#include <core/operators/scalar/select_uncompr.h>
#include <core/operators/general_vectorized/select_compr.h>

#include <core/utils/equality_check.h>

#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>

#include <core/tracing/trace.h>

using namespace morphstore;
using namespace vectorlib;
using ps = scalar<v64<uint64_t>>;

int main( void ) {

    auto initializer = RootInitializer::getInstance();

    if ( !initializer.isNuma() ) {
        trace_l(T_EXIT, "Current setup does not support NUMA, exiting...");
        return -1;
    }

    initializer.initPmemPool(std::string("NVMDSBench"), std::string("NVMDS"), 256ul << 20);
    const auto node_number = initializer.getNumaNodeCount();
    auto repl_inst = ReplicationManager::getInstance();

    /*const column<uncompr_f> * generate_exact_number(
        size_t p_CountValues,
        size_t p_CountMatches,
        uint64_t p_ValMatch,
        uint64_t p_ValOther,
        bool p_Sorted,
        size_t numa_node_number,
        size_t p_Seed = 0*/

    auto col = generate_exact_number_pers(4096, 2048, 0, 1, false, 0, 42);
    col->setRelation("repltest");
    col->setTable("repltest");
    col->setAttribute("attribute");

    const column<uncompr_f> * initCol = col->convert();

    repl_inst.constructAll(col);
    repl_inst.joinAllThreads();

    auto status = repl_inst.getStatus("repltest", "repltest", "attribute");
    assert(status != nullptr);

    for (size_t node = 0; node < node_number; node++) {
        auto pcol  = status->getPersistentColumn(node);
        auto ptree = status->getMultiValTreeIndex(node);
        auto phash = status->getHashMapIndex(node);
        auto pskip = status->getSkipListIndex(node);

        auto colRes =  my_select_wit_t<equal, ps, uncompr_f, uncompr_f>::apply(pcol->convert(), 0);
        auto treeRes = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, persistent_ptr<MultiValTreeIndex>, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>::apply
            (ptree, 0);
        auto hashRes = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, persistent_ptr<HashMapIndex>, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>::apply
            (phash, 0);
        auto skipRes = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, persistent_ptr<SkipListIndex>, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>::apply
            (pskip, 0);

       
        auto first = equality_check(initCol, colRes); 
        auto second = equality_check(initCol, treeRes);
        auto third = equality_check(initCol, skipRes);
        auto fourth = equality_check(initCol, hashRes);

        assert(first.good());
        assert(second.good());
        assert(third.good());
        assert(fourth.good());

    }

    return 0;
}
