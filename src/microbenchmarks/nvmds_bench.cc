//#include <core/memory/mm_glob.h>
#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/access/RootManager.h>
#include <core/access/NVMStorageManager.h>
#include <core/storage/PersistentColumn.h>
#include <core/storage/column_gen.h>
#include <core/tracing/trace.h>

#include <core/index/MultiValTreeIndex.hpp>
#include <core/index/VolatileTreeIndex.hpp>
#include <core/index/SkipListIndex.hpp>
#include <core/index/HashMapIndex.hpp>
#include <core/index/index_gen.h>

#include <core/utils/measure.h>

#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>
#include <core/operators/scalar/agg_sum_uncompr.h>
#include <core/operators/general_vectorized/select_compr.h>
#include <core/operators/scalar/select_uncompr.h>
#include <core/operators/scalar/join_uncompr.h>
#include <core/operators/general_vectorized/between_compr.h>

#include <libpmempool.h>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <pthread.h>

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

using namespace morphstore;
using namespace vectorlib;

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;

//TODO: Figure out way to abstract this
// Parametrization of PBPTree
using CustomTuple = std::tuple<uint64_t>;

using ps = scalar<v64<uint64_t>>;
pobj_alloc_class_desc alloc_class;

constexpr uint64_t SEED = 42;
constexpr unsigned EXP_ITER = 100;
constexpr auto ARRAY_SIZE = COLUMN_SIZE / sizeof(uint64_t);
constexpr uint64_t JOIN_KEY_SIZE = 1000;

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

/*template< class T >
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
const int SELECTIVITY_PARALLEL = SELECTION_THREADS;*/

/* T must be a pointer type */
/*template<class T>
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
}*/

template<class T>
class ArrayList : public std::list<T>
{
public:
    T& operator[](size_t index)
    {
        return *std::next(this->begin(), index);
    }
};

inline const column<uncompr_f> * agg_sum_dua(const column<uncompr_f>* f, const column<uncompr_f>* s, size_t inExtNum)
{
    return agg_sum<ps, uncompr_f, uncompr_f, uncompr_f>(f, s, inExtNum);
}

inline std::tuple<const column<uncompr_f> *, const column<uncompr_f> *> nest_dua(const column<uncompr_f>* f, const column<uncompr_f>* s, size_t inExtNum)
{
    return nested_loop_join<ps, uncompr_f, uncompr_f>(f, s, inExtNum);
}

struct NVMDSBenchParamList {
    ArrayList<pptr<PersistentColumn>> primColPers;
    ArrayList<pptr<PersistentColumn>> valColPers;
    ArrayList<pptr<PersistentColumn>> delColPers;
    ArrayList<pptr<PersistentColumn>> forKeyColPers;

    ArrayList<pptr<PersistentColumn>> table2PrimPers;

    ArrayList<pptr<MultiValTreeIndex>> trees;
    ArrayList<pptr<SkipListIndex>> skiplists;
    ArrayList<pptr<HashMapIndex>> hashmaps;

    ArrayList<pptr<MultiValTreeIndex>> treesFor;
    ArrayList<pptr<SkipListIndex>> skiplistsFor;
    ArrayList<pptr<HashMapIndex>> hashmapsFor;

    ArrayList<pptr<MultiValTreeIndex>> treesTable2;
    ArrayList<pptr<SkipListIndex>> skiplistsTable2;
    ArrayList<pptr<HashMapIndex>> hashmapsTable2;
};

/*void generateNVMDSBenchSetup(delColPers, valColPers, primColPers, forKeyColPers, table2PrimPersConv,
            trees, skiplists, hashmaps,
            treesFor, skiplistsFor, hashmapsFor,
            treesTable2, skiplistsTable2, hashmapsTable2);*/

int main(int /*argc*/, char** /*argv*/)
{
    // Setup phase: figure out node configuration
    if (!numa_prechecks()) {
        trace_l(T_EXIT, "Current setup does not support NUMA, exiting...");
        return -1;
    }

    auto initializer = RootInitializer::getInstance();

    initializer.initPmemPool();
    auto node_number = initializer.getNumaNodeCount();

    trace_l(T_DEBUG, "Current max node number: ", node_number);

    RootManager& root_mgr = RootManager::getInstance();

    ArrayList<std::shared_ptr<const column<uncompr_f>>> primColNode;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> valColNode;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> delColNode;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> forKeyColNode;

    ArrayList<std::shared_ptr<const column<uncompr_f>>> table2PrimNode;

    ArrayList<pptr<PersistentColumn>> primColPers;
    ArrayList<pptr<PersistentColumn>> valColPers;
    ArrayList<pptr<PersistentColumn>> delColPers;
    ArrayList<pptr<PersistentColumn>> forKeyColPers;

    ArrayList<pptr<PersistentColumn>> table2PrimPers;

    ArrayList<std::shared_ptr<const column<uncompr_f>>> primColPersConv;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> valColPersConv;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> delColPersConv;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> forKeyColPersConv;

    ArrayList<std::shared_ptr<const column<uncompr_f>>> table2PrimPersConv;

    ArrayList<pptr<MultiValTreeIndex>> trees;
    ArrayList<pptr<SkipListIndex>> skiplists;
    ArrayList<pptr<HashMapIndex>> hashmaps;

    /*ArrayList<pptr<MultiValTreeIndex>> treesFor;
    ArrayList<pptr<SkipListIndex>> skiplistsFor;
    ArrayList<pptr<HashMapIndex>> hashmapsFor;

    ArrayList<pptr<MultiValTreeIndex>> treesTable2;
    ArrayList<pptr<SkipListIndex>> skiplistsTable2;
    ArrayList<pptr<HashMapIndex>> hashmapsTable2;*/

    // selectivity iteration
    for (float size = 10.0; size <= 100.0; size += 10.0) {
        ArrayList<std::shared_ptr<const column<uncompr_f>>> primColNode;
        ArrayList<std::shared_ptr<const column<uncompr_f>>> valColNode;
        ArrayList<std::shared_ptr<const column<uncompr_f>>> delColNode;
        ArrayList<std::shared_ptr<const column<uncompr_f>>> forKeyColNode;

        ArrayList<std::shared_ptr<const column<uncompr_f>>> table2PrimNode;

        ArrayList<pptr<PersistentColumn>> primColPers;
        ArrayList<pptr<PersistentColumn>> valColPers;
        ArrayList<pptr<PersistentColumn>> delColPers;
        ArrayList<pptr<PersistentColumn>> forKeyColPers;

        ArrayList<pptr<PersistentColumn>> table2PrimPers;

        ArrayList<std::shared_ptr<const column<uncompr_f>>> primColPersConv;
        ArrayList<std::shared_ptr<const column<uncompr_f>>> valColPersConv;
        ArrayList<std::shared_ptr<const column<uncompr_f>>> delColPersConv;
        ArrayList<std::shared_ptr<const column<uncompr_f>>> forKeyColPersConv;

        ArrayList<std::shared_ptr<const column<uncompr_f>>> table2PrimPersConv;

        ArrayList<pptr<MultiValTreeIndex>> trees;
        ArrayList<pptr<SkipListIndex>> skiplists;
        ArrayList<pptr<HashMapIndex>> hashmaps;

        const float selectivity = size / ARRAY_SIZE;

        trace_l(T_INFO, "Starting with a selectivity of ", size / ARRAY_SIZE);
        // Generation Phase
        trace_l(T_INFO, "Generating primary col with keycount ", ARRAY_SIZE, " keys...");
        //Column marks valid rows
        for (unsigned int i = 0; i < node_number; i++) {
            delColNode.push_back(  std::shared_ptr<const column<uncompr_f>>(generate_boolean_col(ARRAY_SIZE, i)));
            primColNode.push_back( std::shared_ptr<const column<uncompr_f>>(generate_sorted_unique(ARRAY_SIZE, i)));
            valColNode.push_back(  std::shared_ptr<const column<uncompr_f>>(generate_exact_number( ARRAY_SIZE, static_cast<size_t>(size), 0, 1, false, i, SEED)));
            forKeyColNode.push_back( std::shared_ptr<const column<uncompr_f>>(generate_with_distr(
                ARRAY_SIZE,
                std::uniform_int_distribution<uint64_t>(0, 99),
                false,
                SEED,
                   i))); 

            table2PrimNode.push_back( std::shared_ptr<const column<uncompr_f>>(generate_sorted_unique(100, i)) );

            trace_l(T_INFO, "Volatile columns for node ", i, " generated");

            /*generateNVMDSBenchSetup(delColPers, valColPers, primColPers, forKeyColPers, table2PrimPersConv,
                    trees, skiplists, hashmaps,
                    treesFor, skiplistsFor, hashmapsFor,
                    treesTable2, skiplistsTable2, hashmapsTable2);*/
            auto valCol = generate_exact_number_pers( ARRAY_SIZE, size, 0, 1, false, i, SEED);
            auto primCol = generate_sorted_unique_pers(ARRAY_SIZE, i);
            auto delCol = generate_boolean_col_pers(ARRAY_SIZE, i);
            auto forKeyCol = generate_with_distr_pers(
                ARRAY_SIZE, std::uniform_int_distribution<uint64_t>(0, 99), false, SEED, i); 

            auto table2PrimCol = generate_sorted_unique_pers(100, i);

            trace_l(T_INFO, "Persistent columns for node ", i, " generated");

            delColPers.push_back(delCol);
            valColPers.push_back(valCol);
            primColPers.push_back(primCol);
            forKeyColPers.push_back(forKeyCol);

            table2PrimPers.push_back(table2PrimCol);

            primColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(primCol->convert()));
            delColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(delCol->convert()));
            valColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(valCol->convert()));
            forKeyColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(valCol->convert()));

            table2PrimPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(table2PrimCol->convert()));

            root_mgr.drainAll();

            trace_l(T_DEBUG, "Initializing index structures");

            pptr<MultiValTreeIndex> tree;
            pptr<SkipListIndex> skiplist;
            pptr<HashMapIndex> hashmap;

            auto pop = root_mgr.getPop(i);
            transaction::run(pop, [&] {
                tree = make_persistent<MultiValTreeIndex>(i, alloc_class, std::string(""), std::string(""), std::string(""));
            });
            transaction::run(pop, [&] {
                skiplist = pmem::obj::make_persistent<SkipListIndex>(i);
            });
            transaction::run(pop, [&] {
                hashmap = pmem::obj::make_persistent<HashMapIndex>(2, i, std::string(""), std::string(""), std::string(""));
            });

            /*pptr<MultiValTreeIndex> treeFor;
            pptr<SkipListIndex> skiplistFor;
            pptr<HashMapIndex> hashmapFor;

            transaction::run(pop, [&] {
                treeFor = make_persistent<MultiValTreeIndex>(i, alloc_class, std::string(""), std::string(""), std::string(""));
            });
            transaction::run(pop, [&] {
                skiplistFor = pmem::obj::make_persistent<SkipListIndex>(i);
            });
            transaction::run(pop, [&] {
                hashmapFor = pmem::obj::make_persistent<HashMapIndex>(2, i, std::string(""), std::string(""), std::string(""));
            });

            pptr<MultiValTreeIndex> tree2;
            pptr<SkipListIndex> skiplist2;
            pptr<HashMapIndex> hashmap2;

            transaction::run(pop, [&] {
                tree2 = make_persistent<MultiValTreeIndex>(i, alloc_class, std::string(""), std::string(""), std::string(""));
            });
            transaction::run(pop, [&] {
                skiplist2 = pmem::obj::make_persistent<SkipListIndex>(i);
            });
            transaction::run(pop, [&] {
                hashmap2 = pmem::obj::make_persistent<HashMapIndex>(2, i, std::string(""), std::string(""), std::string(""));
            });*/

            try {
                trace_l(T_DEBUG, "Constructing MultiValTreeIndex");
                IndexGen<persistent_ptr<MultiValTreeIndex>>::generateKeyToPos(tree, valCol);
                root_mgr.drainAll();
                trace_l(T_DEBUG, "Constructing Skiplist");
                IndexGen<persistent_ptr<SkipListIndex>>::generateKeyToPos(skiplist, valCol);
                root_mgr.drainAll();
                trace_l(T_DEBUG, "Constructing HashMap");
                IndexGen<persistent_ptr<HashMapIndex>>::generateKeyToPos(hashmap, valCol);
                root_mgr.drainAll();

                /*trace_l(T_DEBUG, "Constructing MultiValTreeIndex");
                IndexGen<persistent_ptr<MultiValTreeIndex>>::generateKeyToPos(treeFor, forKeyCol);
                root_mgr.drainAll();
                trace_l(T_DEBUG, "Constructing Skiplist");
                IndexGen<persistent_ptr<SkipListIndex>>::generateKeyToPos(skiplistFor, forKeyCol);
                root_mgr.drainAll();
                trace_l(T_DEBUG, "Constructing HashMap");
                IndexGen<persistent_ptr<HashMapIndex>>::generateKeyToPos(hashmapFor, forKeyCol);
                root_mgr.drainAll();

                trace_l(T_DEBUG, "Constructing MultiValTreeIndex");
                IndexGen<persistent_ptr<MultiValTreeIndex>>::generateKeyToPos(tree2, table2PrimCol);
                root_mgr.drainAll();
                trace_l(T_DEBUG, "Constructing Skiplist");
                IndexGen<persistent_ptr<SkipListIndex>>::generateKeyToPos(skiplist2, table2PrimCol);
                root_mgr.drainAll();
                trace_l(T_DEBUG, "Constructing HashMap");
                IndexGen<persistent_ptr<HashMapIndex>>::generateKeyToPos(hashmap2, table2PrimCol);
                root_mgr.drainAll();*/

                trees.push_back(tree);
                skiplists.push_back(skiplist);
                hashmaps.push_back(hashmap);

                /*treesFor.push_back(treeFor);
                skiplistsFor.push_back(skiplistFor);
                hashmapsFor.push_back(hashmapFor);

                treesTable2.push_back(tree2);
                skiplistsTable2.push_back(skiplist2);
                hashmapsTable2.push_back(hashmap2);*/
            }
            catch (...) {

            }
        }
        root_mgr.drainAll();

        // Benchmark: sequential insertion
        // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile
        auto status = numa_run_on_node(0);
        trace_l(T_INFO, "numa_run_on_node(0) returned ", status);

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
        std::cout << "Operator,Node number,Volatile columns,Persistent tree,Persistent skiplist,Persistent hashmap,Persistent columns,Selectivity\n";

        for (unsigned int i = 0; i < node_number; i++) {
            for (unsigned j = 0; j < EXP_ITER; j++ ) {
                std::cout << "Select," << i << ",";
                measure("Duration of selection on volatile columns: ",
                        my_select_wit_t<equal, ps, uncompr_f, uncompr_f>::apply, valColNode[i].get(), 0, 0);
                measure("Duration of selection on persistent tree: ", 
                        index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, persistent_ptr<MultiValTreeIndex>, persistent_ptr<NodeBucketList<uint64_t>>>::apply, &(*trees[i]), 0);
                measure("Duration of selection on persistent skiplist: ", 
                        index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, persistent_ptr<SkipListIndex>, persistent_ptr<NodeBucketList<uint64_t>>>::apply, &(*skiplists[i]), 0);
                measure("Duration of selection on persistent hashmaps: ", 
                        index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, persistent_ptr<HashMapIndex>, persistent_ptr<NodeBucketList<uint64_t>>>::apply, &(*hashmaps[i]), 0);
                /*measureEnd("Duration of selection on persistent columns: ",
                        index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, VolatileTreeIndex *, VNodeBucketList<uint64_t> *>::apply, &(*vtrees[i]), 0);*/
                measureEnd("Duration of selection on persistent columns: ",
                        my_select_wit_t<equal, ps, uncompr_f, uncompr_f>::apply, valColPersConv[i].get(), 0, 0);
                std::cout << selectivity;
                std::cout << "\n";
            }
        }

        // Benchmark: deletion
        // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile

        // Benchmark: insert and updates
        // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile

        // Benchmark: group by
        // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile
        // Projection, aggregation more interesting

        for (unsigned int i = 0; i < node_number; i++) {
            for (unsigned j = 0; j < EXP_ITER; j++ ) {
                std::cout << "Aggregate," << i << ",";
                measure("Duration of aggregation on volatile column: ",
                        agg_sum_dua, valColNode[i].get(), primColNode[i].get(), 21);
                measureTuple("Duration of aggregation on persistent tree: ",
                        group_agg_sum<persistent_ptr<MultiValTreeIndex>, persistent_ptr<NodeBucketList<uint64_t>>>, &(*trees[i]), 21);
                measureTuple("Duration of aggregation on persistent tree: ",
                        group_agg_sum<persistent_ptr<SkipListIndex>, persistent_ptr<NodeBucketList<uint64_t>>>, &(*skiplists[i]), 21);
                measureTuple("Duration of aggregation on persistent tree: ",
                        group_agg_sum<persistent_ptr<HashMapIndex>, persistent_ptr<NodeBucketList<uint64_t>>>, &(*hashmaps[i]), 21);
                measure("Duration of aggregation on persistent column: ",
                        agg_sum_dua, valColPersConv[i].get(), primColPersConv[i].get(), 21);
                std::cout << selectivity;
                std::cout << "\n";
            }
        }

        // Benchmark: random sequential selection

        for (unsigned int i = 0; i < node_number; i++) {
            for (unsigned j = 0; j < EXP_ITER; j++ ) {
                std::cout << "Between," << i << ",";
                measure("Duration of between selection on volatile column: ",
                        my_between_wit_t<greaterequal, lessequal, ps, uncompr_f, uncompr_f >
                            ::apply, valColNode[i].get(), 0, 0, 0);
                measure("Duration of between selection on persistent tree: ",
                        index_between_wit_t<std::greater_equal, std::less_equal, uncompr_f, uncompr_f, persistent_ptr<MultiValTreeIndex>>
                            ::apply, trees[i], 0, 0);
                measure("Duration of between selection on persistent tree: ",
                        index_between_wit_t<std::greater_equal, std::less_equal, uncompr_f, uncompr_f, persistent_ptr<SkipListIndex>>
                            ::apply, skiplists[i], 0, 0);
                measure("Duration of between selection on persistent tree: ",
                        index_between_wit_t<std::greater_equal, std::less_equal, uncompr_f, uncompr_f, persistent_ptr<HashMapIndex>>
                            ::apply, hashmaps[i], 0, 0);
                measure("Duration of between selection on persistent column: ",
                        my_between_wit_t<greaterequal, lessequal, ps, uncompr_f, uncompr_f >
                            ::apply, valColPersConv[i].get(), 0, 0, 0);
                std::cout << selectivity;
                std::cout << "\n";
            }
        }

        /*for (unsigned int i = 0; i < node_number; i++) {
            for (unsigned j = 0; j < EXP_ITER/10; j++ ) {
                std::cout << "Join," << i << ",";
                measureTuple("Duration of join on volatile column: ",
                        nest_dua
                            , forKeyColNode[i].get(), table2PrimNode[i].get(), ARRAY_SIZE*100);
                measureTuple("Duration of join on persistent tree: ",
                        ds_join<pptr<MultiValTreeIndex>, pptr<MultiValTreeIndex>, persistent_ptr<NodeBucketList<uint64_t>>, persistent_ptr<NodeBucketList<uint64_t>>>
                            , treesFor[i], treesTable2[i]);
                measureTuple("Duration of join on persistent tree: ",
                        ds_join<pptr<SkipListIndex>, pptr<SkipListIndex>, persistent_ptr<NodeBucketList<uint64_t>>, persistent_ptr<NodeBucketList<uint64_t>>>
                            , skiplistsFor[i], skiplistsTable2[i]);
                measureTuple("Duration of join on persistent tree: ",
                        ds_join<pptr<HashMapIndex>, pptr<HashMapIndex>, persistent_ptr<NodeBucketList<uint64_t>>, persistent_ptr<NodeBucketList<uint64_t>>>
                            , hashmapsFor[i], hashmapsTable2[i]);
                measureTupleEnd("Duration of join on persistent column: ",
                        nest_dua
                            , forKeyColPersConv[i].get(), table2PrimPersConv[i].get(), ARRAY_SIZE*100);

                std::cout << "\n";
            }
        }*/

        trace_l(T_DEBUG, "Cleaning persistent columns");
        for (unsigned int i = 0; i < node_number; i++) {
            auto pop = root_mgr.getPop(i);

            primColPers[i]->prepareDest();
            valColPers[i]->prepareDest();
            delColPers[i]->prepareDest();

            /*forKeyColPers[i]->prepareDest();
            table2PrimPers[i]->prepareDest();*/

            transaction::run(pop, [&] {
                delete_persistent<PersistentColumn>(primColPers[i]);
                delete_persistent<PersistentColumn>(valColPers[i]);
                delete_persistent<PersistentColumn>(delColPers[i]);

                delete_persistent<PersistentColumn>(forKeyColPers[i]);

                delete_persistent<PersistentColumn>(table2PrimPers[i]);

                delete_persistent<MultiValTreeIndex>(trees[i]);
                delete_persistent<SkipListIndex>(skiplists[i]);
                delete_persistent<HashMapIndex>(hashmaps[i]);

                /*delete_persistent<MultiValTreeIndex>(treesFor[i]);
                delete_persistent<SkipListIndex>(skiplistsFor[i]);
                delete_persistent<HashMapIndex>(hashmapsFor[i]);
                
                delete_persistent<MultiValTreeIndex>(treesTable2[i]);
                delete_persistent<SkipListIndex>(skiplistsTable2[i]);
                delete_persistent<HashMapIndex>(hashmapsTable2[i]);*/
            });
        }
    }

    root_mgr.drainAll();
    root_mgr.closeAll();
    
    trace_l(T_INFO, "End of Benchmark");
    return 0;
}
