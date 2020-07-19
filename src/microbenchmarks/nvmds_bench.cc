//#include <core/memory/mm_glob.h>
#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/index/TreeDef.h>
#include <core/access/root.h>
#include <core/storage/PersistentColumn.h>
#include <core/operators/operators.h>
#include <core/operators/operators_ds.h>
#include <core/storage/column_gen_add.h>
#include <core/tracing/trace.h>
#include <core/utils/measure.h>

#include <libpmempool.h>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <numa.h>
#include <iostream>
#include <unistd.h>
#include <random>
#include <numeric>
#include <functional>
#include <thread>
#include <list>
#include <memory>

//Describes our tree, is set to be PBPTree
//#include "common.hpp"
//Define this to have access to private members for microbenchmarks
#define UNIT_TESTS

#include <nvmdatastructures/src/pbptrees/PBPTree.hpp>

using namespace dbis::pbptrees;

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::transaction;

using storage::PersistentColumn;

//TODO: Figure out way to abstract this
// Parametrization of PBPTree
using CustomKey = uint64_t;
using CustomTuple = std::tuple<uint64_t>;

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

void initTreeRoot(int pMemNode)
{

    //Constructing the tree
    //auto retr = getPoolRoot(pmemNode);
    RootManager& mgr = RootManager::getInstance();

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
    }
    trace_l(T_DEBUG, "End of tree file creation");

}

void delete_from_tree(uint64_t start, uint64_t end, pptr<TreeType> tree)
{
    trace_l(T_DEBUG, "Deleting ", (end - start), " entries from tree");
    for (auto i = start; i < end; i++)
        tree->erase(i);
}

// Prepare the persistent B Tree for benchmarks
void preparePersistentTree( pptr<TreeType> tree, std::shared_ptr<VolatileColumn> primary, std::shared_ptr<VolatileColumn> values)
{
    TreeType& treeRef = *tree;

    //Anonymous function for insertion
    uint64_t countValues = primary->get_count_values();
    trace_l(T_DEBUG, "count of values is ", countValues);
    auto insertLoop = [&]( uint64_t start, uint64_t end) {
        for (auto j = start; j < end + 1; ++j) {
            auto tup = CustomTuple(values->get_data()[j]);
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
void seq_insert_col( T primCol, T valCol, T delCol)
{
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
    }
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

void aggregateTree(TreeType* tree)
{
    auto iter = tree->begin();
    uint64_t sum = 0;

    while (iter != tree->end()) {
        sum += std::get<0>((*iter).second);
        iter++;
    }
}

template<class T>
void aggregateCol(T col)
{
    uint64_t* valData = col->get_data();
    size_t amount_data = col->get_count_values();

    uint64_t sum = 0;

    for (; amount_data > 0; amount_data--)
        sum += *valData++;

    trace_l(T_DEBUG, "End of thread");
}

const size_t NUM_THREADS = 1;

template<class T>
void parallel_aggregate_col(T col)
{
    trace_l(T_DEBUG, "Aggregate col ");
    std::thread threads[NUM_THREADS];

    for (size_t t = 0; t < NUM_THREADS; t++) {
        trace_l(T_DEBUG, "Started Thread t = ", t);
        threads[t] = std::thread(aggregateCol<T>, col);
    }

    for (size_t t = 0; t < NUM_THREADS; t++) {
        trace_l(T_DEBUG, "Joining thread ", t);
        threads[t].join();
    }

    trace_l(T_DEBUG, "End parallel aggregate");
}

void parallel_aggregate_tree(TreeType* tree)
{
    std::thread threads[NUM_THREADS];

    for (size_t t = 0; t < NUM_THREADS; t++) {
        threads[t] = std::thread(aggregateTree, tree);
    }

    for (size_t t = 0; t < NUM_THREADS; t++) {
        threads[t].join();
    }
}

const int SELECT_ITERATIONS = SELECTION_IT;
const int SELECTIVITY_SIZE = SELECTION_SIZE;
const int SELECTIVITY_PARALLEL = SELECTION_THREADS;

/* T must be a pointer type */
template<class T>
void random_select_col(T prim, T val, int seed_add)
{
    // Lets just implement a selection on primary keys for the attribute values of val
    // Lets assume the rows are not sorted for their primary keys 
    std::srand(SEED + seed_add);
    std::shared_ptr<VolatileColumn> col = nullptr;

    for (int sel = 0; sel < SELECT_ITERATIONS; sel++) {
        col = std::shared_ptr<VolatileColumn>(new VolatileColumn(SELECTIVITY_SIZE * sizeof(uint64_t), 0));

        uint64_t random_select_start = std::rand() / RAND_MAX * prim->get_count_values();
        uint64_t random_select_end = random_select_start + SELECTIVITY_SIZE - 1;

        uint64_t* index = prim->get_data();
        //trace_l(T_DEBUG, "Index starts at ", index, ", ends at ", index + prim->get_count_values()*sizeof(uint64_t));
        uint64_t* data = val->get_data();

        uint64_t* out_col = col->get_data();

        size_t j = 0;
        size_t prim_val = -1;
        for (size_t i = 0; i < prim->get_count_values(); i++) {
            // we assume that the prim col has incremental values
            if (*index - 1 != prim_val)
                throw std::exception();
            prim_val = *index;
            if (prim_val >= random_select_start && prim_val < random_select_end) {
                *out_col = *data;
                out_col++;
                j++;
                if (j > SELECTIVITY_SIZE)
                    throw std::exception();
            }
            index++;
            data++;
        }
    }
}

template<class T>
void random_select_col_threads(T prim, T val)
{
    std::thread t[SELECTIVITY_PARALLEL];

    for (int i = 0; i < SELECTIVITY_PARALLEL; i++) {
        t[i] = std::thread(random_select_col<T>, prim, val, i);    
    }

    for (int i = 0; i < SELECTIVITY_PARALLEL; i++)
        t[i].join();
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

    ArrayList<std::shared_ptr<VolatileColumn>> primColNode;
    ArrayList<std::shared_ptr<VolatileColumn>> valColNode;
    ArrayList<std::shared_ptr<VolatileColumn>> delColNode;

    ArrayList<pptr<PersistentColumn>> primColPers;
    ArrayList<pptr<PersistentColumn>> valColPers;
    ArrayList<pptr<PersistentColumn>> delColPers;

    ArrayList<pptr<TreeType>> trees;

    // Generation Phase
    trace_l(T_INFO, "Generating primary col with keycount ", ARRAY_SIZE, " keys...");
    //Column marks valid rows
    for (int i = 0; i < node_number; i++) {
        /*delColNode[i] = generate_boolean_col(ARRAY_SIZE, i);
        primColNode[i] = generate_sorted_unique(ARRAY_SIZE, i);
        valColNode[i] = generate_with_distr(ARRAY_SIZE,
                std::poisson_distribution<uint64_t>(20), false, i, SEED);*/
        delColNode.push_back(generate_boolean_col(ARRAY_SIZE, i));
        primColNode.push_back(generate_sorted_unique(ARRAY_SIZE, i));
        valColNode.push_back(generate_with_distr(ARRAY_SIZE,
                std::poisson_distribution<uint64_t>(20), false, i, SEED));

        trace_l(T_INFO, "Columns for node ", i, " generated");
        retr = getPoolRoot(i);
        root_mgr.add(retr.pop);

        delColPers.push_back(generate_boolean_col_pers(ARRAY_SIZE, i));
        valColPers.push_back(generate_with_distr_pers(ARRAY_SIZE,
                std::poisson_distribution<uint64_t>(20), false, i, SEED));
        primColPers.push_back(generate_sorted_unique_pers(ARRAY_SIZE, i));

        initTreeRoot(i);

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

    uint64_t max_primary_key = primColNode[0]->get_count_values() - 1;

    for (int i = 0; i < node_number; i++) {
        std::cout << "Measures for node " << i << std::endl;
        measure("Durations of seq insert on volatile columns: ",
                seq_insert_col<std::shared_ptr<VolatileColumn>>, primColNode[i], valColNode[i], delColNode[i]);
        measure("Duration of seq insert on local pers tree: ", seq_insert_tree, trees[i]);
        measure("Duration of seq insert on local pers column: ",
                seq_insert_col<pptr<PersistentColumn>>, primColPers[i],
                valColPers[i], delColPers[i]);
    }

    uint64_t momentary_max_key = primColNode[0]->get_count_values();

    // Benchmark: select range
    // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile

    select_t< PBPTree, CustomKey, CustomTuple, BRANCHKEYS, LEAFKEYS, std::equal_to> select;
    select_col_t<std::shared_ptr<VolatileColumn>, std::equal_to> select_col_vol;
    select_col_t<pptr<PersistentColumn>, std::equal_to> select_col_pers;

    //std::cout << "Measuring select times..." << std::endl;
    for (int i = 0; i < node_number; i++) {
        std::cout << "Measures for node " << i << std::endl;
        measure("Duration of selection on volatile columns: ", select_col_vol.apply, valColNode[i], 10);
        measure("Duration of selection on persistent tree: ", select.apply, &(*trees[i]), 10);
        measure("Duration of selection on volatile columns: ", select_col_pers.apply, valColPers[i], 10);
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
                parallel_aggregate_col<std::shared_ptr<VolatileColumn>>, valColNode[i]);
        measure("Duration of aggregation on persistent tree: ",
                parallel_aggregate_tree, &(*trees[i]));
        measure("Duration of aggregation on volatile column: ",
                parallel_aggregate_col<pptr<PersistentColumn>>, valColPers[i]);
    }

    // Benchmark: random sequential selection

    for (int i = 0; i < node_number; i++) {
        std::cout << "Measures for node " << i << std::endl;
        measure("Duration of random deterministic selection on volatile column: ",
                random_select_col_threads<std::shared_ptr<VolatileColumn>>, primColNode[i], valColNode[i]);
        measure("Duration of random deterministic selection on persistent tree: ", random_select_tree,
                primColNode[0]->get_count_values(), &(*trees[i]));
        measure("Duration of random deterministic selection on persistent column: ",
                random_select_col_threads<pptr<PersistentColumn>>, primColPers[i], valColPers[i]);
    }

    trace_l(T_INFO, "Doing cleanup for tree NVM DS...");
    for (int i = 0; i < node_number; i++)
        delete_from_tree(max_primary_key + 1, momentary_max_key, &(*trees[i]));

    trace_l(T_INFO, "Cleaning persistent columns");
    for (int i = 0; i < node_number; i++) {
        auto pop = root_mgr.getPop(i);
        transaction::run(pop, [&] {
            delete_persistent<PersistentColumn>(primColPers[i]);
            delete_persistent<PersistentColumn>(valColPers[i]);
            delete_persistent<PersistentColumn>(delColPers[i]);
        });
    }

    root_mgr.drainAll();
    root_mgr.closeAll();
    
    std::cout << "End of Benchmark" << std::endl;
    return 0;
}
