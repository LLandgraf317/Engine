
template<class T>
class ArrayList : public std::list<T>
{
public:
    T& operator[](size_t index)
    {
        return *std::next(this->begin(), index);
    }
};

struct JoinBenchParamList {
    ArrayList<pptr<PersistentColumn>> &forKeyColPers;
    ArrayList<pptr<PersistentColumn>> &table2PrimPers;

    ArrayList<pptr<MultiValTreeIndex>> &treesFor;
    ArrayList<pptr<SkipListIndex>> &skiplistsFor;
    ArrayList<pptr<HashMapIndex>> &hashmapsFor;

    ArrayList<pptr<MultiValTreeIndex>> &treesTable2;
    ArrayList<pptr<SkipListIndex>> &skiplistsTable2;
    ArrayList<pptr<HashMapIndex>> &hashmapsTable2;
};

void generateJoinBenchSetup(JoinBenchParamList & list, size_t pmemNode) {

    RootManager& root_mgr = RootManager::getInstance();
    auto pop = root_mgr.getPop(pmemNode);

    auto forKeyColPers = generate_with_distr_pers(
        ARRAY_SIZE, std::uniform_int_distribution<uint64_t>(0, 99), false, SEED, pmemNode); 
    auto table2PrimCol = generate_sorted_unique_pers(100, pmemNode);

    list.forKeyColPers.push_back(forKeyColPers);
    list.table2PrimPers.push_back(table2PrimCol);

    pptr<MultiValTreeIndex> treeFor;
    pptr<SkipListIndex> skiplistFor;
    pptr<HashMapIndex> hashmapFor;

    transaction::run(pop, [&] {
        treeFor = make_persistent<MultiValTreeIndex>(pmemNode, alloc_class, std::string(""), std::string(""), std::string(""));
    });
    transaction::run(pop, [&] {
        skiplistFor = pmem::obj::make_persistent<SkipListIndex>(pmemNode);
    });
    transaction::run(pop, [&] {
        hashmapFor = pmem::obj::make_persistent<HashMapIndex>(2, pmemNode, std::string(""), std::string(""), std::string(""));
    });

    pptr<MultiValTreeIndex> tree2;
    pptr<SkipListIndex> skiplist2;
    pptr<HashMapIndex> hashmap2;

    transaction::run(pop, [&] {
        tree2 = make_persistent<MultiValTreeIndex>(pmemNode, alloc_class, std::string(""), std::string(""), std::string(""));
    });
    transaction::run(pop, [&] {
        skiplist2 = pmem::obj::make_persistent<SkipListIndex>(pmemNode);
    });
    transaction::run(pop, [&] {
        hashmap2 = pmem::obj::make_persistent<HashMapIndex>(2, pmemNode, std::string(""), std::string(""), std::string(""));
    });

    trace_l(T_DEBUG, "Constructing MultiValTreeIndex");
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
    IndexGen<persistent_ptr<HashMapIndex>>::generateFast(hashmap2, table2PrimCol);

    list.treesFor.push_back(treeFor);
    list.skiplistsFor.push_back(skiplistFor);
    list.hashmapsFor.push_back(hashmapFor);

    list.treesTable2.push_back(tree2);
    list.skiplistsTable2.push_back(skiplist2);
    list.hashmapsTable2.push_back(hashmap2);

    root_mgr.drainAll();
}

inline std::tuple<const column<uncompr_f> *, const column<uncompr_f> *> nest_dua(const column<uncompr_f>* f, const column<uncompr_f>* s, size_t inExtNum)
{
    return nested_loop_join<ps, uncompr_f, uncompr_f>(f, s, inExtNum);
}


int main( void ) {
    auto initializer = RootInitializer::getInstance();

    initializer.initPmemPool(std::string("NVMDSJoin"), std::string("NVMDS"));
    auto node_number = initializer.getNumaNodeCount();

        /*forKeyColNode.push_back( std::shared_ptr<const column<uncompr_f>>(generate_with_distr(
            ARRAY_SIZE,
            std::uniform_int_distribution<uint64_t>(0, 99),
            false,
            SEED,
               i))); 
        table2PrimNode.push_back( std::shared_ptr<const column<uncompr_f>>(generate_sorted_unique(100, i)) );*/
    ArrayList<std::shared_ptr<const column<uncompr_f>>> forKeyColNode;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> table2PrimNode;

    ArrayList<std::shared_ptr<const column<uncompr_f>>> forKeyColPersConv;
    ArrayList<std::shared_ptr<const column<uncompr_f>>> table2PrimPersConv;

    ArrayList<pptr<PersistentColumn>> forKeyColPers;
    ArrayList<pptr<PersistentColumn>> table2PrimPers;

    ArrayList<pptr<MultiValTreeIndex>> treesFor;
    ArrayList<pptr<SkipListIndex>> skiplistsFor;
    ArrayList<pptr<HashMapIndex>> hashmapsFor;

    ArrayList<pptr<MultiValTreeIndex>> treesTable2;
    ArrayList<pptr<SkipListIndex>> skiplistsTable2;
    ArrayList<pptr<HashMapIndex>> hashmapsTable2;

    for (unsigned int i = 0; i < node_number; i++) {
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
    }


    forKeyColPers[i]->prepareDest();
    table2PrimPers[i]->prepareDest();

    transaction::run(pop, [&] {
        delete_persistent<MultiValTreeIndex>(treesFor[i]);
        delete_persistent<SkipListIndex>(skiplistsFor[i]);
        delete_persistent<HashMapIndex>(hashmapsFor[i]);
        
        delete_persistent<MultiValTreeIndex>(treesTable2[i]);
        delete_persistent<SkipListIndex>(skiplistsTable2[i]);
        delete_persistent<HashMapIndex>(hashmapsTable2[i]);
    });
}
