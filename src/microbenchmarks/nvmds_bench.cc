//#include <core/memory/mm_glob.h>
#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/access/RootManager.h>
#include <core/access/NVMStorageManager.h>
#include <core/replication/ReplicationManager.h>
#include <core/storage/PersistentColumn.h>
#include <core/storage/column_gen.h>
#include <core/tracing/trace.h>

#include <core/index/index_gen.h>
#include <core/index/IndexDef.h>

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
#include <math.h>

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
//constexpr uint64_t JOIN_KEY_SIZE = 1000;

template<class T>
void seq_insert_col( T /*primCol*/, T /*valCol*/, T /*delCol*/)
{
    /*
    const size_t INSERTCOUNT = 4000;
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

/*void seq_insert_tree(pptr<TreeType> tree)
{
    uint64_t resMax = 0;
    // scan for largest key
    auto getMax = [&resMax](const uint64_t &key, const std::tuple<uint64_t> &val)
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

////////////////////////////////////////////////////////////////////////TREADING FOR DS GENERATION ////////////////////////////////////////////////////////
// Threaded DS Creation
uint64_t thread_num_counter = 0;

struct thread_info {
    pthread_t thread_id;
    int thread_num;
};

// Array of thread_info
thread_info* thread_infos = nullptr;

template<class index_structure>
struct CreateIndexArgs {
    uint64_t node;
    pptr<index_structure> index;
    pptr<PersistentColumn> valCol;
};

template< class index_structure>
void * generate( void * argPtr )
{
    CreateIndexArgs<index_structure> * indexArgs = (CreateIndexArgs<index_structure>*) argPtr;

    numa_run_on_node(indexArgs->node);

    IndexGen::generateFast<pptr<index_structure>, OSP_SIZE>(indexArgs->index, indexArgs->valCol);

    RootManager& root_mgr = RootManager::getInstance();
    root_mgr.drainAll();

    free(argPtr);

    return nullptr;
}

void joinAllPThreads() {
    for (unsigned i = 0; i < thread_num_counter; i++) {
        trace_l(T_INFO, "Joined generation thread ", i);
        pthread_join(thread_infos[i].thread_id, nullptr);
    }
}

////////////////////////////////////////////////////////////////////////GENERATION FUNCTIONS////////////////////////////////////////////////////////
//
struct NVMDSBenchParamList {
    ArrayList<pptr<PersistentColumn>> &primColPers;
    ArrayList<pptr<PersistentColumn>> &valColPers;

    ArrayList<pptr<MultiValTreeIndex>> &trees;
    ArrayList<pptr<SkipListIndex>> &skiplists;
    ArrayList<pptr<HashMapIndex>> &hashmaps;

    std::vector<sel_and_val> & sel_distr;
};

void printMemoryFootprint(NVMDSBenchParamList & list, size_t nodeCount)
{
    auto lambda = [&] (const uint64_t & key, const pptr<NodeBucketList<uint64_t, 4096>> & val) {
        trace_l(T_INFO, "Key: ", key, ", bucket count: ", val->count_buckets(), ", count values: ", val->getCountValues());
    };

    trace_l(T_INFO, "Printing memory footprints");
    for (size_t i = 0; i < nodeCount; i++) {
        trace_l(T_INFO, "Size of data structures on node ", i, ":");
        trace_l(T_INFO, "Persistent column: ", list.valColPers[i]->memory_footprint(), " bytes");
        trace_l(T_INFO, "Persistent tree: ", list.trees[i]->memory_footprint(), " bytes");
        list.trees[i]->scan(lambda);

        trace_l(T_INFO, "Persistent skiplist: ", list.skiplists[i]->memory_footprint(), " bytes");
        list.skiplists[i]->scan(lambda);

        trace_l(T_INFO, "Persistent hashmap: ", list.hashmaps[i]->memory_footprint(), " bytes");
        list.hashmaps[i]->scan(lambda);

        trace_l(T_INFO, "");
    }
}

void generateNVMDSBenchSetup(NVMDSBenchParamList & list, size_t pmemNode)
{
    numa_run_on_node(pmemNode);

    auto primCol = generate_sorted_unique_pers( ARRAY_SIZE, pmemNode);
    primCol->setRelation("test");
    primCol->setTable("1");
    primCol->setAttribute("prim");

    trace_l(T_INFO, "Testing prim naming: ", primCol->getRelation(), ", ", primCol->getTable(), ", ", primCol->getAttribute());

    auto valCol = generate_share_vector_pers( ARRAY_SIZE, list.sel_distr, pmemNode);
    valCol->setRelation("test");
    valCol->setTable("1");
    valCol->setAttribute("val");

    NVMStorageManager::pushPersistentColumn(primCol);
    NVMStorageManager::pushPersistentColumn(valCol);

    trace_l(T_INFO, "Persistent columns for node ", pmemNode, " generated");

    list.valColPers.push_back(valCol);
    list.primColPers.push_back(primCol);

    trace_l(T_DEBUG, "Initializing index structures");

    pptr<MultiValTreeIndex> tree;
    pptr<SkipListIndex> skiplist;
    pptr<HashMapIndex> hashmap;

    RootManager& root_mgr = RootManager::getInstance();
    auto pop = root_mgr.getPop(pmemNode);

    transaction::run(pop, [&] {
        tree = make_persistent<MultiValTreeIndex>(pmemNode, alloc_class, std::string("test"), std::string("1"), std::string("valPos"));
    });
    transaction::run(pop, [&] {
        skiplist = pmem::obj::make_persistent<SkipListIndex>(pmemNode, std::string("test"), std::string("1"), std::string("valPos"));
    });
    transaction::run(pop, [&] {
        hashmap = pmem::obj::make_persistent<HashMapIndex>(2, pmemNode, std::string("test"), std::string("1"), std::string("valPos"));
    });

    NVMStorageManager::pushMultiValTreeIndex(tree);
    NVMStorageManager::pushSkipListIndex(skiplist);
    NVMStorageManager::pushHashMapIndex(hashmap);

    try {
        {
            trace_l(T_DEBUG, "Constructing MultiValTreeIndex");
            CreateIndexArgs<MultiValTreeIndex>* args = new CreateIndexArgs<MultiValTreeIndex>();
            args->node = pmemNode;
            args->index = tree;
            args->valCol = valCol;
            pthread_create(&thread_infos[thread_num_counter].thread_id, nullptr, generate<MultiValTreeIndex>, args);
            thread_num_counter++;
            //IndexGen<persistent_ptr<MultiValTreeIndex>>::generateKeyToPos(tree, valCol);
        }

        {
            trace_l(T_DEBUG, "Constructing Skiplist");
            CreateIndexArgs<SkipListIndex>* args = new CreateIndexArgs<SkipListIndex>();
            args->node = pmemNode;
            args->index = skiplist;
            args->valCol = valCol;
            pthread_create(&thread_infos[thread_num_counter].thread_id, nullptr, generate<SkipListIndex>, args);
            thread_num_counter++;
            //IndexGen<persistent_ptr<SkipListIndex>>::generateKeyToPos(skiplist, valCol);
        }

        {
            trace_l(T_DEBUG, "Constructing HashMap");
            CreateIndexArgs<HashMapIndex>* args = new CreateIndexArgs<HashMapIndex>();
            args->node = pmemNode;
            args->index = hashmap;
            args->valCol = valCol;
            pthread_create(&thread_infos[thread_num_counter].thread_id, nullptr, generate<HashMapIndex>, args);
            thread_num_counter++;
            //IndexGen<persistent_ptr<HashMapIndex>>::generateKeyToPos(hashmap, valCol);
        }
    }
    catch (const std::exception& e) {

        std::cerr << e.what();
        trace_l(T_WARN, "Warning: exception during generation thrown");
    }

    list.trees.push_back(tree);
    list.skiplists.push_back(skiplist);
    list.hashmaps.push_back(hashmap);
}

void cleanAllDS(NVMDSBenchParamList & list)
{
    trace_l(T_DEBUG, "Cleaning persistent columns");

    auto initializer = RootInitializer::getInstance();
    auto node_number = initializer.getNumaNodeCount();
    RootManager& root_mgr = RootManager::getInstance();

    for (unsigned int i = 0; i < node_number; i++) {
        auto pop = root_mgr.getPop(i);

        list.valColPers[i]->prepareDest();
        list.primColPers[i]->prepareDest();

        transaction::run(pop, [&] {
            delete_persistent<PersistentColumn>(list.valColPers[i]);
            delete_persistent<PersistentColumn>(list.primColPers[i]);

            delete_persistent<MultiValTreeIndex>(list.trees[i]);
            delete_persistent<SkipListIndex>(list.skiplists[i]);
            delete_persistent<HashMapIndex>(list.hashmaps[i]);

        });
    }
}

int main(int /*argc*/, char** /*argv*/)
{
    // Setup phase: figure out node configuration
    auto initializer = RootInitializer::getInstance();

    if ( !initializer.isNuma() ) {
        trace_l(T_EXIT, "Current setup does not support NUMA, exiting...");
        return -1;
    }


    initializer.initPmemPool(std::string("NVMDSBench"), std::string("NVMDS"));
    auto node_number = initializer.getNumaNodeCount();

    thread_infos = (thread_info*) calloc( 6 * node_number, sizeof(thread_info) );

    trace_l(T_DEBUG, "Current max node number: ", node_number);

    std::vector<sel_and_val> sel_distr;
    const unsigned MAX_SEL_ATTR = 10;
    for (unsigned i = 1; i < MAX_SEL_ATTR + 1; i++) {
        trace_l(T_DEBUG, "pow is ", pow(0.5f, MAX_SEL_ATTR - i + 2));
        sel_distr.push_back(sel_and_val(pow(0.5f, MAX_SEL_ATTR - i + 2 ) , i));
    }

    RootManager& root_mgr = RootManager::getInstance();
    ReplicationManager &repl_mgr = ReplicationManager::getInstance();

    ArrayList<std::shared_ptr<const column<uncompr_f>>> primColNode;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> valColNode;
    //ArrayList<std::shared_ptr<const column<uncompr_f>>> delColNode;

    ArrayList<std::shared_ptr<VolatileTreeIndex>> vtrees;
    ArrayList<std::shared_ptr<VSkipListIndex>> vskiplists;
    ArrayList<std::shared_ptr<VHashMapIndex>> vhashmaps;

    ArrayList<pptr<PersistentColumn>> primColPers;
    ArrayList<pptr<PersistentColumn>> valColPers;
    //ArrayList<pptr<PersistentColumn>> delColPers;

    ArrayList<std::shared_ptr<const column<uncompr_f>>> primColPersConv;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> valColPersConv;
    //ArrayList<std::shared_ptr<const column<uncompr_f>>> delColPersConv;

    ArrayList<pptr<MultiValTreeIndex>> trees;
    ArrayList<pptr<SkipListIndex>> skiplists;
    ArrayList<pptr<HashMapIndex>> hashmaps;


    // Generation Phase
    trace_l(T_INFO, "Generating primary col with keycount ", ARRAY_SIZE, " keys...");
    //Column marks valid rows
    for (unsigned int i = 0; i < node_number; i++) {
        auto status = numa_run_on_node(i);

        primColNode.push_back( std::shared_ptr<const column<uncompr_f>>(generate_sorted_unique( ARRAY_SIZE, i) ));
        valColNode.push_back(  std::shared_ptr<const column<uncompr_f>>(generate_share_vector( ARRAY_SIZE, sel_distr, i) ));

        trace_l(T_INFO, "Volatile columns for node ", i, " generated");

        if (initializer.isNVMRetrieved(i)) {
            auto primCol  = NVMStorageManager::getPersistentColumn(  std::string("test"), std::string("1"), std::string("prim"), i);
            auto valCol   = NVMStorageManager::getPersistentColumn(  std::string("test"), std::string("1"), std::string("val"), i);
            auto tree     = NVMStorageManager::getMultiValTreeIndex(    std::string("test"), std::string("1"), std::string("valPos"), i);
            auto skiplist = NVMStorageManager::getSkipListIndex(std::string("test"), std::string("1"), std::string("valPos"), i);
            auto hashmap  = NVMStorageManager::getHashMapIndex( std::string("test"), std::string("1"), std::string("valPos"), i);

            // validate
            
            
            // push
            primColPers.push_back(primCol);
            valColPers.push_back(valCol);

            trees.push_back(tree);
            skiplists.push_back(skiplist);
            hashmaps.push_back(hashmap);

            primColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(primCol->convert()));
            valColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(valCol->convert()));
            trace_l(T_INFO, "Retrieved data structures from NVM");
        }
        else {
            NVMDSBenchParamList l = {primColPers, valColPers, trees, skiplists, hashmaps, sel_distr};
            generateNVMDSBenchSetup(l, i);

            primColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(primColPers[i]->convert()));
            valColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(valColPers[i]->convert()));
        }

        /*vtrees.push_back( std::shared_ptr<VolatileTreeIndex>( new VolatileTreeIndex(i, std::string("test"), std::string("1"), std::string("valPos")) ) );
        IndexGen<VolatileTreeIndex*>::generateKeyToPos( &*vtrees[i], valColPers[i]);

        vskiplists.push_back( std::shared_ptr<VSkipListIndex>( new VSkipListIndex(i, std::string("test"), std::string("1"), std::string("valPos")) ) );
        IndexGen<VSkipListIndex*>::generateKeyToPos( &*vskiplists[i], valColPers[i]);

        vhashmaps.push_back( std::shared_ptr<VHashMapIndex>( new VHashMapIndex(MAX_SEL_ATTR, i, std::string("test"), std::string("1"), std::string("valPos")) ) );
        IndexGen<VHashMapIndex*>::generateKeyToPos( &*vhashmaps[i], valColPers[i]);*/
    }

    joinAllPThreads();
    root_mgr.drainAll();

    // Checks for numa location property
    for (uint64_t i = 0; i < node_number; i++) {
        trace_l(T_INFO, "Locations of volatile columns, should be ", i);
        assert(repl_mgr.isLocOnNode(primColNode[i]->get_data(), i));
        assert(repl_mgr.isLocOnNode(valColNode[i]->get_data(), i));

        trace_l(T_INFO, "Locations of persistent columns, should be ", i);
        assert(repl_mgr.isLocOnNode(primColPersConv[i]->get_data(), i));
        assert(repl_mgr.isLocOnNode(valColPersConv[i]->get_data(), i));

        trace_l(T_INFO, "Locations of datastructures, should be ", i);
        assert(repl_mgr.isLocOnNode(trees[i].get(), i));
        assert(repl_mgr.isLocOnNode(skiplists[i].get(), i));
        assert(repl_mgr.isLocOnNode(hashmaps[i].get(), i));
    }

    // Benchmark: sequential insertion
    // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile
    auto status = numa_run_on_node(0);

    NVMDSBenchParamList l = {primColPers, valColPers, trees, skiplists, hashmaps, sel_distr};
    trace_l(T_INFO, "numa_run_on_node(0) returned ", status);

    trace_l(T_INFO, "entries per bucket: ", NodeBucketList<uint64_t, 4096>::max_entries_per_bucket());
    
    printMemoryFootprint(l, node_number);

    // Benchmark: select range
    // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile
    std::cout << "Operator,Node number,Volatile columns,Persistent tree,Persistent skiplist,Persistent hashmap,Persistent columns,VolTree,VolSkipList,VolHashmap,Selectivity\n";

    for (uint64_t sel_attr = 0; sel_attr < MAX_SEL_ATTR; sel_attr++ ) {
        const float selectivity = sel_distr[sel_attr].selectivity;
        const uint64_t attr_value = sel_distr[sel_attr].attr_value;

        for (unsigned int i = 0; i < node_number; i++) {
            for (unsigned j = 0; j < EXP_ITER; j++ ) {
                std::cout << "Select," << i << ",";
                measure("Duration of selection on volatile columns: ",
                        my_select_wit_t<equal, ps, uncompr_f, uncompr_f>::apply, valColNode[i].get(), attr_value, 0);
                measure("Duration of selection on persistent tree: ", 
                        index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, persistent_ptr<MultiValTreeIndex>, persistent_ptr<NodeBucketList<uint64_t>>>::apply, &(*trees[i]), attr_value);
                measure("Duration of selection on persistent skiplist: ", 
                        index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, persistent_ptr<SkipListIndex>, persistent_ptr<NodeBucketList<uint64_t>>>::apply, &(*skiplists[i]), attr_value);
                measure("Duration of selection on persistent hashmaps: ", 
                        index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, persistent_ptr<HashMapIndex>, persistent_ptr<NodeBucketList<uint64_t>>>::apply, &(*hashmaps[i]), attr_value);
                measure("Duration of selection on persistent columns: ",
                        my_select_wit_t<equal, ps, uncompr_f, uncompr_f>::apply, valColPersConv[i].get(), attr_value, 0);
                /*measure("Duration of selection on volatile tree: ",
                        index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, VolatileTreeIndex *, VNodeBucketList<uint64_t> *>::apply, &(*vtrees[i]), 0);
                measure("Duration of selection on volatile skiplist: ",
                        index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, VSkipListIndex *, VNodeBucketList<uint64_t> *>::apply, &(*vskiplists[i]), 0);
                measure("Duration of selection on volatile hashmap: ",
                        index_select_wit_t<std::equal_to, uncompr_f, uncompr_f, VHashMapIndex *, VNodeBucketList<uint64_t> *>::apply, &(*vhashmaps[i]), 0);*/
                std::cout << selectivity;
                std::cout << "\n";
            }
        }

        // Benchmark: deletion
        // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile

        // Benchmark: insert and updates
        // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile

        // Benchmark: random sequential selection

        for (unsigned int i = 0; i < node_number; i++) {
            for (unsigned j = 0; j < EXP_ITER; j++ ) {
                std::cout << "Between," << i << ",";
                measure("Duration of between selection on volatile column: ",
                        my_between_wit_t<greaterequal, lessequal, ps, uncompr_f, uncompr_f >
                            ::apply, valColNode[i].get(), attr_value, attr_value, 0);
                measure("Duration of between selection on persistent tree: ",
                        index_between_wit_t<std::greater_equal, std::less_equal, uncompr_f, uncompr_f, persistent_ptr<MultiValTreeIndex>>
                            ::apply, trees[i], attr_value, attr_value);
                measure("Duration of between selection on persistent tree: ",
                        index_between_wit_t<std::greater_equal, std::less_equal, uncompr_f, uncompr_f, persistent_ptr<SkipListIndex>>
                            ::apply, skiplists[i], attr_value, attr_value);
                measure("Duration of between selection on persistent tree: ",
                        index_between_wit_t<std::greater_equal, std::less_equal, uncompr_f, uncompr_f, persistent_ptr<HashMapIndex>>
                            ::apply, hashmaps[i], attr_value, attr_value);
                measure("Duration of between selection on persistent column: ",
                        my_between_wit_t<greaterequal, lessequal, ps, uncompr_f, uncompr_f >
                            ::apply, valColPersConv[i].get(), attr_value, attr_value, 0);
                /*measure("Duration of between selection on volatile tree: ",
                        index_between_wit_t<std::greater_equal, std::less_equal, uncompr_f, uncompr_f, VolatileTreeIndex *>
                            ::apply, &(*vtrees[i]), attr_value, attr_value);
                measure("Duration of between selection on volatile skiplist: ",
                        index_between_wit_t<std::greater_equal, std::less_equal, uncompr_f, uncompr_f, VSkipListIndex *>
                            ::apply, &(*vskiplists[i]), attr_value, attr_value);
                measure("Duration of between selection on volatile hashmap: ",
                        index_between_wit_t<std::greater_equal, std::less_equal, uncompr_f, uncompr_f, VHashMapIndex *>
                            ::apply, &(*vhashmaps[i]), attr_value, attr_value);*/
                std::cout << selectivity;
                std::cout << "\n";
            }
        }

    }

    // Benchmark: group by
    // Configurations: local column, remote column, local B Tree Persistent, remote DRAM B Tree volatile
    // Projection, aggregation more interesting

    for (unsigned int i = 0; i < node_number; i++) {
        for (unsigned j = 0; j < EXP_ITER; j++ ) {
            std::cout << "Aggregate," << i << ",";
            measure("Duration of aggregation on volatile column: ",
                    agg_sum_dua, valColNode[i].get(), primColNode[i].get(), MAX_SEL_ATTR);
            measureTuple("Duration of aggregation on persistent tree: ",
                    group_agg_sum<persistent_ptr<MultiValTreeIndex>, persistent_ptr<NodeBucketList<uint64_t>>>, &(*trees[i]), MAX_SEL_ATTR + 1);
            measureTuple("Duration of aggregation on persistent tree: ",
                    group_agg_sum<persistent_ptr<SkipListIndex>, persistent_ptr<NodeBucketList<uint64_t>>>, &(*skiplists[i]), MAX_SEL_ATTR + 1);
            measureTuple("Duration of aggregation on persistent tree: ",
                    group_agg_sum<persistent_ptr<HashMapIndex>, persistent_ptr<NodeBucketList<uint64_t>>>, &(*hashmaps[i]), MAX_SEL_ATTR + 1);
            measure("Duration of aggregation on persistent column: ",
                    agg_sum_dua, valColPersConv[i].get(), primColPersConv[i].get(), MAX_SEL_ATTR);
            /*measureTuple("Duration of aggregation on volatile tree: ",
                    group_agg_sum<VolatileTreeIndex*, VNodeBucketList<uint64_t>*>, &(*vtrees[i]), MAX_SEL_ATTR + 1);
            measureTuple("Duration of aggregation on volatile skiplist: ",
                    group_agg_sum<VSkipListIndex*, VNodeBucketList<uint64_t>*>, &(*vskiplists[i]), MAX_SEL_ATTR + 1);
            measureTuple("Duration of aggregation on volatile tree: ",
                    group_agg_sum<VHashMapIndex*, VNodeBucketList<uint64_t>*>, &(*vhashmaps[i]), MAX_SEL_ATTR + 1);*/
            std::cout << 1;
            std::cout << "\n";
        }
    }


/*    trace_l(T_DEBUG, "Cleaning persistent columns");
    for (unsigned int i = 0; i < node_number; i++) {
        auto pop = root_mgr.getPop(i);

        primColPers[i]->prepareDest();
        valColPers[i]->prepareDest();
        delColPers[i]->prepareDest();


        transaction::run(pop, [&] {
            delete_persistent<PersistentColumn>(primColPers[i]);
            delete_persistent<PersistentColumn>(valColPers[i]);
            delete_persistent<PersistentColumn>(delColPers[i]);

            delete_persistent<PersistentColumn>(forKeyColPers[i]);

            delete_persistent<PersistentColumn>(table2PrimPers[i]);

            delete_persistent<MultiValTreeIndex>(trees[i]);
            delete_persistent<SkipListIndex>(skiplists[i]);
            delete_persistent<HashMapIndex>(hashmaps[i]);

        });
    }*/

    root_mgr.drainAll();
    root_mgr.closeAll();
    
    trace_l(T_INFO, "End of Benchmark");
    return 0;
}
