#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>
#include <core/memory/constants.h>

#include <core/access/RootManager.h>
#include <core/access/NVMStorageManager.h>
#include <core/storage/PersistentColumn.h>
#include <core/storage/column_gen.h>
#include <core/tracing/trace.h>

#include <core/index/IndexDef.h>
#include <core/index/index_gen.h>

#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>
#include <core/operators/scalar/agg_sum_uncompr.h>
#include <core/operators/general_vectorized/select_compr.h>
#include <core/operators/scalar/select_uncompr.h>
#include <core/operators/scalar/join_uncompr.h>
#include <core/operators/general_vectorized/between_compr.h>

#include <core/utils/measure.h>
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

using namespace morphstore;
using namespace vectorlib;

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;
using ps = scalar<v64<uint64_t>>;

pobj_alloc_class_desc alloc_class;
constexpr auto ARRAY_SIZE = COLUMN_SIZE / sizeof(uint64_t);
constexpr uint64_t SEED = 42;
constexpr uint64_t MAX_ATTR = 1000;
constexpr unsigned EXP_ITER = 10;

template<class T>
class ArrayList : public std::list<T>
{
public:
    T& operator[](size_t index)
    {
        return *std::next(this->begin(), index);
    }
};

    /*trace_l(T_DEBUG, "Constructing MultiValTreeIndex");
    IndexGen<persistent_ptr<MultiValTreeIndex>>::generateFast(treeFor, forKeyColPers);
    root_mgr.drainAll();
    trace_l(T_DEBUG, "Constructing Skiplist");
    IndexGen<persistent_ptr<SkipListIndex>>::generateFast(skiplistFor, forKeyColPers);
    root_mgr.drainAll();
    trace_l(T_DEBUG, "Constructing HashMap");
    IndexGen<persistent_ptr<HashMapIndex>>::generateFast(hashmapFor, forKeyColPers);
    root_mgr.drainAll();

    trace_l(T_DEBUG, "Constructing MultiValTreeIndex");
    IndexGen<persistent_ptr<MultiValTreeIndex>>::generateFast(tree2, table2PrimCol);
    root_mgr.drainAll();
    trace_l(T_DEBUG, "Constructing Skiplist");
    IndexGen<persistent_ptr<SkipListIndex>>::generateFast(skiplist2, table2PrimCol);
    root_mgr.drainAll();
    trace_l(T_DEBUG, "Constructing HashMap");
    IndexGen<persistent_ptr<HashMapIndex>>::generateFast(hashmap2, table2PrimCol);*/

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
    pptr<index_structure> index;
    pptr<PersistentColumn> valCol;
};

template<class index_structure, uint64_t t_bucket_size>
void * generate( void * argPtr )
{
    CreateIndexArgs<index_structure> * indexArgs = (CreateIndexArgs<index_structure>*) argPtr;

    IndexGen::generateFast<pptr<index_structure>, t_bucket_size>(indexArgs->index, indexArgs->valCol);

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

struct JoinBenchParamList {
    ArrayList<pptr<PersistentColumn>> &forKeyColPers;
    ArrayList<pptr<PersistentColumn>> &table2PrimPers;

    ArrayList<pptr<MultiValTreeIndex>> &treesFor;
    ArrayList<pptr<SkipListIndex>> &skiplistsFor;
    ArrayList<pptr<HashMapIndex>> &hashmapsFor;

    ArrayList<pptr<CLTreeIndex>> &treesTable2;
    ArrayList<pptr<CLSkipListIndex>> &skiplistsTable2;
    ArrayList<pptr<CLHashMapIndex>> &hashmapsTable2;
};

std::string rel_name = "jointest";
std::string table1 = "1";
std::string table2 = "2";
std::string attrFor = "forkey";
std::string attr2 = "primkey";

void generateJoinBenchSetup(JoinBenchParamList & list, size_t pmemNode) {

    RootManager& root_mgr = RootManager::getInstance();
    auto pop = root_mgr.getPop(pmemNode);

    auto forKeyColPers = generate_with_distr_pers(
        ARRAY_SIZE, std::uniform_int_distribution<uint64_t>(0, MAX_ATTR-1), false, SEED, pmemNode); 
    auto table2PrimCol = generate_sorted_unique_pers(MAX_ATTR, pmemNode);

    list.forKeyColPers.push_back(forKeyColPers);
    list.table2PrimPers.push_back(table2PrimCol);

    pptr<MultiValTreeIndex> treeFor;
    pptr<SkipListIndex> skiplistFor;
    pptr<HashMapIndex> hashmapFor;

    pptr<CLTreeIndex> tree2;
    pptr<CLSkipListIndex> skiplist2;
    pptr<CLHashMapIndex> hashmap2;

    transaction::run(pop, [&] {
        treeFor = make_persistent<MultiValTreeIndex>(pmemNode, alloc_class, rel_name, table1, attrFor);
    });
    transaction::run(pop, [&] {
        skiplistFor = pmem::obj::make_persistent<SkipListIndex>(pmemNode, rel_name, table1, attrFor);
    });
    transaction::run(pop, [&] {
        hashmapFor = pmem::obj::make_persistent<HashMapIndex>(2, pmemNode, rel_name, table1, attrFor);
    });

    transaction::run(pop, [&] {
        tree2 = make_persistent<CLTreeIndex>(pmemNode, alloc_class, rel_name, table2, attr2);
    });
    transaction::run(pop, [&] {
        skiplist2 = pmem::obj::make_persistent<CLSkipListIndex>(pmemNode, rel_name, table2, attr2);
    });
    transaction::run(pop, [&] {
        hashmap2 = pmem::obj::make_persistent<CLHashMapIndex>(2, pmemNode, rel_name, table2, attr2);
    });

    try {
        {
            trace_l(T_DEBUG, "Constructing MultiValTreeIndex");
            CreateIndexArgs<MultiValTreeIndex>* args = new CreateIndexArgs<MultiValTreeIndex>();
            args->index = treeFor;
            args->valCol = forKeyColPers;
            pthread_create(&thread_infos[thread_num_counter].thread_id, nullptr, generate<MultiValTreeIndex, OSP_SIZE>, args);
            thread_num_counter++;
            //IndexGen<persistent_ptr<MultiValTreeIndex>>::generateKeyToPos(tree, valCol);
        }

        {
            trace_l(T_DEBUG, "Constructing Skiplist");
            CreateIndexArgs<SkipListIndex>* args = new CreateIndexArgs<SkipListIndex>();
            args->index = skiplistFor;
            args->valCol = forKeyColPers;
            pthread_create(&thread_infos[thread_num_counter].thread_id, nullptr, generate<SkipListIndex, OSP_SIZE>, args);
            thread_num_counter++;
            //IndexGen<persistent_ptr<SkipListIndex>>::generateKeyToPos(skiplist, valCol);
        }

        {
            trace_l(T_DEBUG, "Constructing HashMap");
            CreateIndexArgs<HashMapIndex>* args = new CreateIndexArgs<HashMapIndex>();
            args->index = hashmapFor;
            args->valCol = forKeyColPers;
            pthread_create(&thread_infos[thread_num_counter].thread_id, nullptr, generate<HashMapIndex, OSP_SIZE>, args);
            thread_num_counter++;
            //IndexGen<persistent_ptr<HashMapIndex>>::generateKeyToPos(hashmap, valCol);
        }

        {
            trace_l(T_DEBUG, "Constructing MultiValTreeIndex");
            CreateIndexArgs<CLTreeIndex>* args = new CreateIndexArgs<CLTreeIndex>();
            args->index = tree2;
            args->valCol = table2PrimCol;
            pthread_create(&thread_infos[thread_num_counter].thread_id, nullptr, generate<CLTreeIndex, CL_SIZE>, args);
            thread_num_counter++;
            //IndexGen<persistent_ptr<MultiValTreeIndex>>::generateKeyToPos(tree, valCol);
        }

        {
            trace_l(T_DEBUG, "Constructing Skiplist");
            CreateIndexArgs<CLSkipListIndex>* args = new CreateIndexArgs<CLSkipListIndex>();
            args->index = skiplist2;
            args->valCol = table2PrimCol;
            pthread_create(&thread_infos[thread_num_counter].thread_id, nullptr, generate<CLSkipListIndex, CL_SIZE>, args);
            thread_num_counter++;
            //IndexGen<persistent_ptr<SkipListIndex>>::generateKeyToPos(skiplist, valCol);
        }

        {
            trace_l(T_DEBUG, "Constructing HashMap");
            CreateIndexArgs<CLHashMapIndex>* args = new CreateIndexArgs<CLHashMapIndex>();
            args->index = hashmap2;
            args->valCol = table2PrimCol;
            pthread_create(&thread_infos[thread_num_counter].thread_id, nullptr, generate<CLHashMapIndex, CL_SIZE>, args);
            thread_num_counter++;
            //IndexGen<persistent_ptr<HashMapIndex>>::generateKeyToPos(hashmap, valCol);
        }
    }
    catch (const std::exception& e) {

        std::cerr << e.what();
        trace_l(T_WARN, "Warning: exception during generation thrown");
    }


    joinAllPThreads();

    list.treesFor.push_back(treeFor);
    list.skiplistsFor.push_back(skiplistFor);
    list.hashmapsFor.push_back(hashmapFor);

    list.treesTable2.push_back(tree2);
    list.skiplistsTable2.push_back(skiplist2);
    list.hashmapsTable2.push_back(hashmap2);

    root_mgr.drainAll();
}

void deleteAll(JoinBenchParamList list)
{
    auto initializer = RootInitializer::getInstance();
    auto node_number = initializer.getNumaNodeCount();
    RootManager& root_mgr = RootManager::getInstance();

    for (uint64_t i = 0; i < node_number; i++) {
        auto pop = root_mgr.getPop(i);
        list.forKeyColPers[i]->prepareDest();
        list.table2PrimPers[i]->prepareDest();

        transaction::run(pop, [&] {
            delete_persistent<MultiValTreeIndex>(list.treesFor[i]);
            delete_persistent<SkipListIndex>(list.skiplistsFor[i]);
            delete_persistent<HashMapIndex>(list.hashmapsFor[i]);
            
            delete_persistent<CLTreeIndex>(list.treesTable2[i]);
            delete_persistent<CLSkipListIndex>(list.skiplistsTable2[i]);
            delete_persistent<CLHashMapIndex>(list.hashmapsTable2[i]);
        });
    }
}

inline std::tuple<const column<uncompr_f> *, const column<uncompr_f> *> nest_dua(const column<uncompr_f>* f, const column<uncompr_f>* s, size_t inExtNum)
{
    return nested_loop_join<ps, uncompr_f, uncompr_f>(f, s, inExtNum);
}

int main( void ) {
    auto initializer = RootInitializer::getInstance();
    initializer.initPmemPool(std::string("NVMDSJoin"), std::string("NVMDS"));
    const auto node_number = initializer.getNumaNodeCount();
    thread_infos = (thread_info*) calloc( 6 * node_number, sizeof(thread_info) );

    ArrayList<std::shared_ptr<const column<uncompr_f>>> forKeyColNode;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> table2PrimNode;

    ArrayList<pptr<PersistentColumn>> forKeyColPers;
    ArrayList<pptr<PersistentColumn>> table2PrimPers;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> forKeyColPersConv;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> table2PrimPersConv;

    ArrayList<pptr<MultiValTreeIndex>> treesFor;
    ArrayList<pptr<SkipListIndex>> skiplistsFor;
    ArrayList<pptr<HashMapIndex>> hashmapsFor;

    ArrayList<pptr<CLTreeIndex>> treesTable2;
    ArrayList<pptr<CLSkipListIndex>> skiplistsTable2;
    ArrayList<pptr<CLHashMapIndex>> hashmapsTable2;

    for (unsigned i = 0; i < node_number; i++) {
        forKeyColNode.push_back( std::shared_ptr<const column<uncompr_f>>(generate_with_distr(
            ARRAY_SIZE,
            std::uniform_int_distribution<uint64_t>(0, MAX_ATTR-1),
            false,
            SEED,
               i))); 
        table2PrimNode.push_back( std::shared_ptr<const column<uncompr_f>>(generate_sorted_unique(MAX_ATTR, i)) );

        if (initializer.isNVMRetrieved(i)) {
            auto forKey  = NVMStorageManager::getPersistentColumn(  rel_name, table1, attrFor, i);
            auto table2prim = NVMStorageManager::getPersistentColumn(  rel_name, table2, attr2, i);

            auto treeFor        = NVMStorageManager::getMultiValTreeIndex(    rel_name, table1, attrFor, i);
            auto skiplistFor    = NVMStorageManager::getSkipListIndex(rel_name, table1, attrFor, i);
            auto hashmapFor     = NVMStorageManager::getHashMapIndex( rel_name, table1, attrFor, i);

            auto tree2        = NVMStorageManager::getCLTreeIndex(    rel_name, table2, attr2, i);
            auto skiplist2    = NVMStorageManager::getCLSkipListIndex(rel_name, table2, attr2, i);
            auto hashmap2     = NVMStorageManager::getCLHashMapIndex( rel_name, table2, attr2, i);

            forKeyColPers.push_back(forKey);
            table2PrimPers.push_back(table2prim);

            treesFor.push_back(treeFor);
            skiplistsFor.push_back(skiplistFor);
            hashmapsFor.push_back(hashmapFor);

            treesTable2.push_back(tree2);
            skiplistsTable2.push_back(skiplist2);
            hashmapsTable2.push_back(hashmap2);

            trace_l(T_INFO, "Retrieved data structures from NVM");
        }
        else {
            JoinBenchParamList list = {forKeyColPers, table2PrimPers, treesFor, skiplistsFor, hashmapsFor, treesTable2, skiplistsTable2, hashmapsTable2};
            generateJoinBenchSetup(list, i);

            forKeyColPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(forKeyColPers[i]->convert()));
            table2PrimPersConv.push_back(std::shared_ptr<const column<uncompr_f>>(table2PrimPers[i]->convert()));
        }
    }
    
    trace_l(T_INFO, "Starting join benchmark");

    for (unsigned int i = 0; i < node_number; i++) {
        for (unsigned j = 0; j < EXP_ITER; j++ ) {
            std::cout << "Join," << i << ",";
            measureTuple("Duration of join on volatile column: ",
                    nest_dua
                        , forKeyColNode[i].get(), table2PrimNode[i].get(), ARRAY_SIZE*100);
            measureTuple("Duration of join on persistent tree: ",
                    ds_join<pptr<MultiValTreeIndex>, pptr<CLTreeIndex>, persistent_ptr<NodeBucketList<uint64_t>>, persistent_ptr<NodeBucketList<uint64_t>>>
                        , treesFor[i], treesTable2[i]);
            measureTuple("Duration of join on persistent tree: ",
                    ds_join<pptr<SkipListIndex>, pptr<CLSkipListIndex>, persistent_ptr<NodeBucketList<uint64_t>>, persistent_ptr<NodeBucketList<uint64_t>>>
                        , skiplistsFor[i], skiplistsTable2[i]);
            measureTuple("Duration of join on persistent tree: ",
                    ds_join<pptr<HashMapIndex>, pptr<CLHashMapIndex>, persistent_ptr<NodeBucketList<uint64_t>>, persistent_ptr<NodeBucketList<uint64_t>>>
                        , hashmapsFor[i], hashmapsTable2[i]);
            measureTupleEnd("Duration of join on persistent column: ",
                    nest_dua
                        , forKeyColPersConv[i].get(), table2PrimPersConv[i].get(), ARRAY_SIZE*100);

            std::cout << "\n";
        }
    }

    return 0;
}
