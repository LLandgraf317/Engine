#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/access/root.h>
#include <core/access/RootManager.h>
#include <core/storage/column_gen.h>
#include <core/replication/ReplicationManager.h>

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

std::ostream& operator<<(std::ostream& os, const bitmask& bm)
{
    for(size_t i=0;i<bm.size;++i) {
        os << numa_bitmask_isbitset(&bm, i);
    }

    return os;
}

int main( void ) {
    // L3 Cache at target system is 32MB
    // lets break it
    auto initializer = RootInitializer::getInstance();
    if ( !initializer.isNuma() ) {
        trace_l(T_EXIT, "Current setup does not support NUMA, exiting...");
        return -1;
    }

    /*auto allowed_mems = numa_get_mems_allowed();
    auto allowed_cpus = numa_all_cpus_ptr;
    auto no_nodes = numa_no_nodes_ptr;

    std::cout << "Allowed mems bitmask " << *allowed_mems << std::endl;
    std::cout << "Allowed cpus bitmask " << *allowed_cpus << std::endl;
    std::cout << "No nodes bitmask " << *no_nodes << std::endl;*/

    auto repl_manager = ReplicationManager::getInstance();

    initializer.initPmemPool(std::string("NVMDSNuma"), std::string("NVMDS"), 512ul << 20);
    const auto node_count = initializer.getNumaNodeCount();

    RootManager& root_mgr = RootManager::getInstance();

    std::vector<const column<uncompr_f> *> volCols;
    std::vector<persistent_ptr<PersistentColumn>> cols;
    std::vector<persistent_ptr<PersistentColumn>> largeCols;
    std::vector<persistent_ptr<CLTreeIndex>> trees;

    for (uint64_t node = 0; node < node_count; node++) {
        auto col = generate_sorted_unique_pers((8ul << 20) / sizeof(uint64_t), node);
        cols.push_back(col);
        auto volCol = generate_sorted_unique( (8ul << 20) / sizeof(uint64_t), node);
        //assert(repl_manager.isLocOnNode(volCol->get_data(), node));
        auto largeCol = generate_sorted_unique_pers((8ul << 20) / sizeof(uint64_t), node);
        //assert(repl_manager.isLocOnNode(largeCol->get_data(), node));

        volCols.push_back(volCol);
        largeCols.push_back(largeCol);
        root_mgr.drainAll();

        auto pop = root_mgr.getPop(node);
        transaction::run( pop, [&] () {
            trees.push_back( make_persistent<CLTreeIndex>(node, alloc_class, std::string(""), std::string(""), std::string("")) );
        });
        IndexGen::generateFast<pptr<CLTreeIndex>, CL_SIZE>(trees[node], cols[node]);
        root_mgr.drainAll();
    }

    root_mgr.printBin();

    for (uint64_t node = 0; node < node_count; node++) {
        void* addresses[3] = { cols[node]->get_data(), volCols[node]->get_data(), largeCols[node]->get_data() };
        int status[3] = {0, 0, 0};

        numa_move_pages( 0 /*calling process this*/, 3 /* we dont move pages */, addresses, nullptr, status, 0);
        for (int i = 0; i < 3; i++)
            trace_l(T_DEBUG, "location on ", addresses[i], " is located on node ", status[i], ", requested is ", node);
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
