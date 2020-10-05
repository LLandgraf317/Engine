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

#include <iostream>
#include <chrono>
#include <numa.h>

pobj_alloc_class_desc alloc_class;

using pmem::obj::persistent_ptr;
using namespace morphstore;

const uint64_t ITER_MAX = 50000000;

void pseudo_random_access(pptr<PersistentColumn> col)
{
    auto size = col->get_count_values();
    const size_t pages = size / CL_SIZE;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, pages-1);

    uint64_t * data = col->get_data();
    uint64_t sum = 0;

    auto start = std::chrono::system_clock::now();
    for (uint64_t i = 0; i < ITER_MAX*10; i++) {

        uint64_t pos = distrib(gen) * CL_SIZE;
        uint64_t val = data[pos];
        sum += val;
    }
    auto end = std::chrono::system_clock::now();

    std::chrono::duration<double> dur = end - start;
    std::cout << dur.count() << ",";
}

template<class t_index_structure_ptr>
void pseudo_random_access_index(t_index_structure_ptr index, bool isEnd)
{
    auto size = index->getCountValues();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, size-1);
    uint64_t sum = 0;

    auto start = std::chrono::system_clock::now();
    for (uint64_t i = 0; i < ITER_MAX; i++) {
        auto buck = index->find(distrib(gen));
        sum += buck->getCountValues();
    } 
    auto end = std::chrono::system_clock::now();

    std::chrono::duration<double> dur = end - start;
    std::cout << dur.count();
    if (!isEnd)
        std::cout << ",";
    else 
        std::cout << std::endl;
}

int main( void ) {
    // L3 Cache at target system is 32MB
    // lets break it
    auto initializer = RootInitializer::getInstance();

    initializer.initPmemPool(std::string("NVMDSNuma"), std::string("NVMDS"), 32ul << 30);
    const auto node_count = initializer.getNumaNodeCount();

    RootManager& root_mgr = RootManager::getInstance();

    std::vector<persistent_ptr<PersistentColumn>> cols;
    std::vector<persistent_ptr<PersistentColumn>> largeCols;
    std::vector<persistent_ptr<CLTreeIndex>> trees;

    for (uint64_t node = 0; node < node_count; node++) {
        auto col = generate_sorted_unique_pers((64ul << 20) / sizeof(uint64_t), node);
        cols.push_back(col);
        auto largeCol = generate_sorted_unique_pers((256ul << 20) / sizeof(uint64_t), node);
        largeCols.push_back(largeCol);
        root_mgr.drainAll();

        auto pop = root_mgr.getPop(node);
        transaction::run( pop, [&] () {
            trees.push_back( make_persistent<CLTreeIndex>(node, alloc_class, std::string(""), std::string(""), std::string("")) );
        });
        IndexGen::generateFast<pptr<CLTreeIndex>, CL_SIZE>(trees[node], cols[node]);
        root_mgr.drainAll();
    }

    numa_run_on_node(0);

    for (uint64_t node = 0; node < node_count; node++) {
        pseudo_random_access(largeCols[node]);
        pseudo_random_access_index(trees[node], true);
    }

    for (uint64_t node = 0; node < node_count; node++) {
        largeCols[node]->prepareDest();
        cols[node]->prepareDest();
        trees[node]->prepareDest();
        
        auto pop = root_mgr.getPop(node);
        transaction::run( pop, [&] () {
            delete_persistent<PersistentColumn>(largeCols[node]);
            delete_persistent<PersistentColumn>(cols[node]);
            delete_persistent<CLTreeIndex>(trees[node]);
        });
    }

    return 0;
}
