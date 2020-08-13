//#include <core/memory/mm_glob.h>
#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/index/TreeDef.h>
#include <core/access/root.h>
#include <core/storage/PersistentColumn.h>
#include <core/operators/operators.h>
#include <core/operators/operators_ds.h>
#include <core/storage/column_gen.h>
#include <core/tracing/trace.h>
#include <core/index/MultiValTreeIndex.hpp>
#include <core/index/PHashMap.hpp>
#include <core/utils/measure.h>

#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>
#include <core/operators/general_vectorized/agg_sum_compr.h>
#include <core/operators/general_vectorized/select_compr.h>
#include <core/operators/scalar/select_uncompr.h>

#include <libpmempool.h>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <core/operators/general_vectorized/between_compr.h>

#include <numa.h>
#include <pthread.h>
#include <unistd.h>

#include <iostream>
#include <random>
#include <numeric>
#include <functional>
#include <list>
#include <memory>

//Describes our tree, is set to be PBPTree
//#include "common.hpp"
//Define this to have access to private members for microbenchmarks
#define UNIT_TESTS

#include <nvmdatastructures/src/pbptrees/PBPTree.hpp>

using namespace dbis::pbptrees;
using namespace morphstore;
using namespace vectorlib;

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::transaction;

//TODO: Figure out way to abstract this
// Parametrization of PBPTree
using CustomKey = uint64_t;
using CustomTuple = std::tuple<uint64_t>;

using ps = scalar<v64<uint64_t>>;

constexpr auto L3 = 14080 * 1024;
constexpr auto LAYOUT = "NVMDS";
constexpr auto POOL_SIZE = 1024 * 1024 * 1024 * 4ull;  //< 4GB

constexpr uint64_t SEED = 42;
constexpr auto ARRAY_SIZE = COLUMN_SIZE / sizeof(uint64_t);
pobj_alloc_class_desc alloc_class;

struct root_retrieval {
    pool<root> pop;
    bool read_from_file_successful;
};

root_retrieval getPoolRoot(int pmemNode)
{
    root_retrieval retr;
    std::string path = (pmemNode == 0 ? dbis::gPmemPath0 : dbis::gPmemPath1) + "NVMDSBench";
    const std::string& gPmem = (pmemNode == 0 ? dbis::gPmemPath0 : dbis::gPmemPath1);

    if (access(path.c_str(), F_OK) != 0) {
        mkdir(gPmem.c_str(), 0777);
        trace_l(T_INFO, "Creating new file on ", path);
        retr.pop = pool<root>::create(path, LAYOUT, POOL_SIZE);

        retr.read_from_file_successful = false;
    }
    else {
        trace_l(T_INFO, "File already existed, opening and returning.");
        retr.pop = pool<root>::open(path, LAYOUT);
        retr.read_from_file_successful = true;
    }

    return retr;
}

void initTreeRoot(int /*pMemNode*/)
{

    //Constructing the tree
    //auto retr = getPoolRoot(pmemNode);
    /*RootManager& mgr = RootManager::getInstance();

    pool<root> pop = *std::next(mgr.getPops(), pMemNode);
    auto q = pop.root();
    auto &rootRef = *q;

    alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>(
        "heap.alloc_class.new.desc", TreeType::AllocClass);
    if (!rootRef.tree) {
        trace_l(T_DEBUG, "make_persistent TreeType");
        transaction::run(pop, [&] {
          pop.root()->tree = make_persistent<TreeType>(alloc_class);
        });
    }*/
    trace_l(T_DEBUG, "End of tree file creation");

}

void delete_from_tree(uint64_t start, uint64_t end, pptr<TreeType> tree)
{
    trace_l(T_DEBUG, "Deleting ", (end - start), " entries from tree");
    for (auto i = start; i < end; i++)
        tree->erase(i);
}

// Prepare the persistent B Tree for benchmarks
template<class T>
void preparePersistentTree( pptr<TreeType> tree, std::shared_ptr<const column<T>> primary, std::shared_ptr<const column<T>> values)
{
    TreeType& treeRef = *tree;

    //Anonymous function for insertion
    uint64_t countValues = primary->get_count_values();
    trace_l(T_DEBUG, "count of values is ", countValues);
    const uint64_t * const inData = values->get_data();

    auto insertLoop = [&]( uint64_t start, uint64_t end) {
        for (auto j = start; j < end + 1; ++j) {
            auto tup = CustomTuple(inData[j]);
            if (j % 100000 == 0) trace_l(T_DEBUG, "Inserting ", j, " and ", std::get<0>(tup));
            treeRef.insert(j, tup);
        }
    };

    insertLoop(0, countValues);
    trace_l(T_DEBUG, "End of PBPTree generation.");
}

bool numa_prechecks()
{
    return numa_available() >= 0;
}

const size_t INSERTCOUNT = 4000;

template<class T>
void seq_insert_col( T /*primCol*/, T /*valCol*/, T /*delCol*/)
{
    /*
    auto val_count = primCol->get_count_values();
    uint64_t max = 0;
    uint64_t* outPos = primCol->get_data();
    for (size_t i = 0; i < val_count; i++) {
        max = (max < outPos[i] ? outPos[i] : max);
    }

    size_t remaining_size = primCol->get_size() - val_count * sizeof(uint64_t);
    size_t val_remaining_size = valCol->get_size() - val_count * sizeof(uint64_t);
    size_t del_remaining_size = delCol->get_size() - val_count * sizeof(bool);

    if (remaining_size < INSERTCOUNT * sizeof(uint64_t)) {
        primCol->expand(INSERTCOUNT * sizeof(uint64_t) - remaining_size);
        valCol->expand(INSERTCOUNT * sizeof(uint64_t) - val_remaining_size);
        delCol->expand(INSERTCOUNT * sizeof(bool) - del_remaining_size);

        uint64_t* res = primCol->get_data();
        for (uint64_t i = 0; i < val_count - 1; i++) {
            if (res[i] != res[i+1] - 1)
                std::cout << "No equal for index " << i << ", is " << res[i] << " and " <<  res[i+1] << std::endl;
            assert(res[i] == res[i+1] - 1);
        }
    }
 
    std::random_device rd;  //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(1, 20);
  
    // set to already created index 
    bool* delPos = &reinterpret_cast<bool*>(delCol->get_data())[val_count];
    outPos = &primCol->get_data()[val_count];

    auto valPos = &valCol->get_data()[val_count];
    for (size_t i = 0; i < INSERTCOUNT; i++) {
        *outPos = max + i + 1;
        outPos++;
        *valPos = dis(gen);
        valPos++;
        *delPos = true;
        delPos++;
    }*/
}

void seq_insert_tree(pptr<TreeType> tree)
{
    uint64_t resMax = 0;
    // scan for largest key
    auto getMax = [&resMax](const uint64_t &key, const std::tuple<uint64_t> &/*val*/)
    {
        if (key > resMax) resMax = key;
    };

    trace_l(T_DEBUG, "Getting Max key from Tree");
    tree->scan(getMax);
    std::random_device rd;  //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(1, 20);

    for (size_t i = 0; i < INSERTCOUNT; i++) {
        if (i % 500 == 0)
            trace_l(T_DEBUG, "Insertion on ", i);
        tree->insert(resMax + i + 1, dis(gen));
    }
}

template< class T >
struct ThreadSelect {
public:
    pthread_t thread;
    T prim;
    T val;
    TreeType* tree;
    int thread_num;
};

void* aggregateTree(void* ptr)
{
    ThreadSelect<column<uncompr_f>*>* t = reinterpret_cast<ThreadSelect<column<uncompr_f>*>*>(ptr);
    TreeType* tree = t->tree;
    auto iter = tree->begin();
    uint64_t sum = 0;

    while (iter != tree->end()) {
        sum += std::get<0>((*iter).second);
        iter++;
    }

    return nullptr;
}

template<class T>
void* aggregateCol(void* ptr)
{
    ThreadSelect<T>* t = reinterpret_cast<ThreadSelect<T>*>(ptr);
    agg_sum<ps, uncompr_f >(t->prim);

    trace_l(T_DEBUG, "End of thread");
    
    return nullptr;
}

const size_t NUM_THREADS = 1;

template<class T>
void parallel_aggregate_col(T col)
{
    trace_l(T_DEBUG, "Aggregate col ");
    ThreadSelect<T> threads[NUM_THREADS];

    for (size_t i = 0; i < NUM_THREADS; i++) {
        trace_l(T_DEBUG, "Started Thread t = ", i);
        threads[i].prim = col;
        threads[i].thread_num = i;
        pthread_create(&threads[i].thread, nullptr, aggregateCol<T>, &threads[i]);
    }

    for (size_t i = 0; i < NUM_THREADS; i++) {
        trace_l(T_DEBUG, "Joining thread ", i);
        pthread_join(threads[i].thread, nullptr);
    }

    trace_l(T_DEBUG, "End parallel aggregate");
}

void parallel_aggregate_tree(TreeType* tree)
{
    ThreadSelect<column<uncompr_f>*> thread_infos[NUM_THREADS];

    for (size_t i = 0; i < NUM_THREADS; i++) {
        thread_infos[i].tree = tree;
        thread_infos[i].thread_num = i;
        pthread_create(&thread_infos[i].thread, nullptr, aggregateTree, &thread_infos[i]);
    }

    for (size_t i = 0; i < NUM_THREADS; i++) {
        pthread_join(thread_infos[i].thread, nullptr);
    }
}

const int SELECT_ITERATIONS = SELECTION_IT;
const int SELECTIVITY_SIZE = SELECTION_SIZE;
const int SELECTIVITY_PARALLEL = SELECTION_THREADS;

/* T must be a pointer type */
template<class T>
void* random_select_col(void* ptr)
{
    ThreadSelect<T>* data = reinterpret_cast<ThreadSelect<T>*>(ptr);
    auto seed_add = data->thread_num;
    auto prim = data->prim;
    // Lets just implement a selection on primary keys for the attribute values of val
    // Lets assume the rows are not sorted for their primary keys 
    std::srand(SEED + seed_add);

    for (int sel = 0; sel < SELECT_ITERATIONS; sel++) {

        uint64_t random_select_start = std::rand() / RAND_MAX * prim->get_count_values();
        uint64_t random_select_end = random_select_start + SELECTIVITY_SIZE - 1;

        my_between_wit_t<greaterequal, lessequal, ps, uncompr_f, uncompr_f >
            ::apply(prim.get(), random_select_start, random_select_end);
    }

    trace_l(T_DEBUG, "Returning thread ", data->thread_num);
    return nullptr;
}

template<class T>
void random_select_col_threads(T prim, T val)
{
    ThreadSelect<T> data[SELECTIVITY_PARALLEL];

    for (int i = 0; i < SELECTIVITY_PARALLEL; i++) {
        trace_l(T_DEBUG, "Starting Thread ", i);
        data[i].prim = prim;
        data[i].val = val;
        data[i].thread_num = i;
        pthread_create(&(data[i].thread), nullptr, random_select_col<T>, &data[i]);    
    }

    for (int i = 0; i < SELECTIVITY_PARALLEL; i++) {
        trace_l(T_DEBUG, "Ending Thread ", i);
        pthread_join((data[i].thread) , nullptr);
    }
}

void random_select_tree(uint64_t highest_key, TreeType* tree)
{
    std::srand(SEED);
    VolatileColumn* col = nullptr;

    for (int sel = 0; sel < SELECT_ITERATIONS; sel++) {
        col = new VolatileColumn(SELECTIVITY_SIZE * sizeof(uint64_t), 0);

        uint64_t random_select_start = std::rand() / RAND_MAX * highest_key;
        uint64_t random_select_end = random_select_start + SELECTIVITY_SIZE - 1;
        uint64_t* index = col->get_data();

        auto materialize_lambda = [&](const uint64_t& /*key*/, const std::tuple<uint64_t>& val)
        {
            *index = std::get<0>(val);
            index++;
        };

        tree->scan(random_select_start, random_select_end, materialize_lambda);

        delete col;
    }
}

template<class T>
class ArrayList : public std::list<T>
{
public:
    T& operator[](size_t index)
    {
        return *std::next(this->begin(), index);
    }
};

int main(int /*argc*/, char** /*argv*/)
{
    // Setup phase: figure out node configuration
    if (!numa_prechecks()) {
        trace_l(T_EXIT, "Current setup does not support NUMA, exiting...");
        return -1;
    }

    auto node_number = numa_max_node() + 1;

    trace_l(T_DEBUG, "Current max node number: ", node_number);

    RootManager& root_mgr = RootManager::getInstance();
    root_retrieval retr;

    ArrayList<std::shared_ptr<const column<uncompr_f>>> primColNode;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> valColNode;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> delColNode;

    ArrayList<pptr<PersistentColumn>> primColPers;
    ArrayList<pptr<PersistentColumn>> valColPers;
    ArrayList<pptr<PersistentColumn>> delColPers;

    ArrayList<std::shared_ptr<const column<uncompr_f>>> primColPersConv;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> valColPersConv;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> delColPersConv;

    ArrayList<pptr<MultiValTreeIndex>> indexes;
    ArrayList<pptr<TreeType>> trees;

    // Generation Phase
    trace_l(T_INFO, "Generating primary col with keycount ", ARRAY_SIZE, " keys...");
    //Column marks valid rows
    for (int i = 0; i < node_number; i++) {
        delColNode.push_back(  std::shared_ptr<const column<uncompr_f>>(generate_boolean_col(ARRAY_SIZE, i)));
        primColNode.push_back( std::shared_ptr<const column<uncompr_f>>(generate_sorted_unique(ARRAY_SIZE, i)));
        valColNode.push_back(  std::shared_ptr<const column<uncompr_f>>(generate_with_distr(ARRAY_SIZE,
                std::poisson_distribution<uint64_t>(20), false, i, SEED)));

        trace_l(T_INFO, "Columns for node ", i, " generated");
        retr = getPoolRoot(i);
        root_mgr.add(retr.pop);

        auto delCol = generate_boolean_col_pers(ARRAY_SIZE, i);
        auto valCol = generate_with_distr_pers(ARRAY_SIZE,
                std::poisson_distribution<uint64_t>(20), false, i, SEED);
        auto primCol = generate_sorted_unique_pers(ARRAY_SIZE, i);

        delColPers.push_back(delCol);
        valColPers.push_back(valCol);
        primColPers.push_back(primCol);

        delColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(delCol->convert()));
        valColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(valCol->convert()));
        primColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(primCol->convert()));

        initTreeRoot(i);

        trace_l(T_INFO, "Constructing MuliValTreeIndex");

        pptr<MultiValTreeIndex> index;
        alloc_class = retr.pop.ctl_set<struct pobj_alloc_class_desc>(
            "heap.alloc_class.new.desc", MultiValTree::AllocClass);
        trace_l(T_INFO, "Running transaction");
        transaction::run(retr.pop, [&] {
            index = make_persistent<MultiValTreeIndex>(i, alloc_class, std::string(""), std::string(""), std::string(""));
        });
        index->generateKeyToPos(valCol);
        indexes.push_back(index);

        trees.push_back(root_mgr.getPop(i).root()->tree);
        trace_l(T_INFO, "Building tree ", i, " from primary and value columns");
        if (!retr.read_from_file_successful)
            preparePersistentTree(trees[i], primColNode[0], valColNode[0]);
        trace_l(T_DEBUG, "Prepared tree ", i);
    }
    root_mgr.drainAll();

    std::cout << "Sizeof PBPTree: " << trees[0]->memory_footprint() << std::endl;
    std::cout << "Sizeof Column Prim: " << ARRAY_SIZE * sizeof(uint64_t) << std::endl;
    std::cout << "Sizeof Column Val: " << ARRAY_SIZE * sizeof(uint64_t) << std::endl;

    trace_l(T_INFO, "Benchmark Prototype");
    
    // Benchmark: sequential insertion
    // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile
    auto status = numa_run_on_node(0);
    trace_l(T_DEBUG, "numa_run_on_node(0) returned ", status);

#if 0
    uint64_t max_primary_key = primColNode[0]->get_count_values() - 1;

    for (int i = 0; i < node_number; i++) {
        std::cout << "Measures for node " << i << std::endl;
        measure("Durations of seq insert on volatile columns: ",
                seq_insert_col<std::shared_ptr<const column<uncompr_f>>>, primColNode[i], valColNode[i], delColNode[i]);
        measure("Duration of seq insert on local pers tree: ", seq_insert_tree, trees[i]);
        measure("Duration of seq insert on local pers column: ",
                seq_insert_col<std::shared_ptr<const column<uncompr_f>>>, primColPersConv[i],
                valColPersConv[i], delColPersConv[i]);
    }

    uint64_t momentary_max_key = primColNode[0]->get_count_values();
#endif

    // Benchmark: select range
    // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile

    //select_t< PBPTree, CustomKey, CustomTuple, BRANCHKEYS, LEAFKEYS, std::equal_to> select;
    //select_t< PBPTree, CustomKey, CustomTuple, BRANCHKEYS, LEAFKEYS, std::equal_to> select;

    //std::cout << "Measuring select times..." << std::endl;
    for (int i = 0; i < node_number; i++) {
        std::cout << "Measures for node " << i << std::endl;
        measure("Duration of selection on volatile columns: ",
                my_select_wit_t<equal, ps, uncompr_f, uncompr_f>::apply, valColNode[i].get(), 10, 0);
        measure("Duration of selection on persistent tree: ", 
                index_select_wit_t<std::equal_to, uncompr_f, uncompr_f>::apply, &(*indexes[i]), 10);
        measure("Duration of selection on volatile columns: ",
                my_select_wit_t<equal, ps, uncompr_f, uncompr_f>::apply, valColPersConv[i].get(), 10, 0);
    }

    // Benchmark: deletion
    // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile

    // Benchmark: insert and updates
    // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile

    // Benchmark: group by
    // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile
    // Projection, aggregation more interesting

    for (int i = 0; i < node_number; i++) {
        std::cout << "Measures for node " << i << std::endl;
        measure("Duration of aggregation on volatile column: ",
                parallel_aggregate_col<const column<uncompr_f>*>, valColNode[i].get());
        measure("Duration of aggregation on persistent tree: ",
                parallel_aggregate_tree, &(*trees[i]));
        measure("Duration of aggregation on volatile column: ",
                parallel_aggregate_col<const column<uncompr_f>*>, valColPersConv[i].get());
    }

    // Benchmark: random sequential selection

    for (int i = 0; i < node_number; i++) {
        std::cout << "Measures for node " << i << std::endl;
        measure("Duration of random deterministic selection on volatile column: ",
                //random_select_col_threads<std::shared_ptr<const column<uncompr_f>>>, primColNode[i], valColNode[i]);
                my_between_wit_t<greaterequal, lessequal, ps, uncompr_f, uncompr_f >
                    ::apply, valColNode[i].get(), 8, 12, 0);
        measure("Duration of random deterministic selection on persistent tree: ",

                index_between_wit_t<greaterequal, lessequal, uncompr_f, uncompr_f>
                    ::apply, indexes[i], 8, 12);
        measure("Duration of random deterministic selection on persistent column: ",
                my_between_wit_t<greaterequal, lessequal, ps, uncompr_f, uncompr_f >
                    ::apply, valColPersConv[i].get(), 8, 12, 0);
                //random_select_col_threads<std::shared_ptr<const column<uncompr_f>>>, primColPersConv[i], valColPersConv[i]);
    }

#if 0
    trace_l(T_INFO, "Doing cleanup for tree NVM DS...");
    for (int i = 0; i < node_number; i++)
        delete_from_tree(max_primary_key + 1, momentary_max_key, &(*trees[i]));
#endif

    trace_l(T_INFO, "Cleaning persistent columns");
    for (int i = 0; i < node_number; i++) {
        auto pop = root_mgr.getPop(i);
        transaction::run(pop, [&] {
            delete_persistent<PersistentColumn>(primColPers[i]);
            delete_persistent<PersistentColumn>(valColPers[i]);
            delete_persistent<PersistentColumn>(delColPers[i]);
            delete_persistent<MultiValTreeIndex>(indexes[i]);
        });
    }

    root_mgr.drainAll();
    root_mgr.closeAll();
    
    std::cout << "End of Benchmark" << std::endl;
    return 0;
}
