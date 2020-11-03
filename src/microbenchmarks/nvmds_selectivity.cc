//#include <core/memory/mm_glob.h>
#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/storage/column_gen.h>
#include <core/access/RootManager.h>
#include <core/access/NVMStorageManager.h>
#include <core/replication/ReplicationManager.h>
#include <core/storage/PersistentColumn.h>

#include <core/operators/general_vectorized/project_compr.h>
#include <core/operators/general_vectorized/select_compr.h>
#include <core/operators/scalar/agg_sum_uncompr.h>
#include <core/operators/scalar/select_uncompr.h>

#include <core/utils/measure.h>

#include <numa.h>
#include <vector>

using namespace morphstore;
using namespace pmem::obj;
using namespace vectorlib;

using ps = scalar<v64<uint64_t>>;

char const * RELATION = "sel";
char const * TABLE = "distr";
char const * X = "x";
char const * Y = "y";
char const * Z = "z";

constexpr auto ARRAY_SIZE = COLUMN_SIZE / sizeof(uint64_t);

class Main {
public:
    Main() : repl_mgr(ReplicationManager::getInstance())
    {
    }

    ReplicationManager & repl_mgr;

    const unsigned MAX_SEL_X = 999;
    const unsigned MAX_SEL_Y = 10;

    void initData() {
        auto initializer = RootInitializer::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        repl_mgr.init(node_number);

        std::vector<sel_and_val> sel_distr_x;
        for (unsigned i = 1; i < MAX_SEL_X + 1; i++) {
            trace_l(T_DEBUG, "pow is ", pow(0.5f, MAX_SEL_X - i + 2));
            sel_distr_x.push_back(sel_and_val(0.001f, i));
        }

        std::vector<sel_and_val> sel_distr_y;
        for (unsigned i = 1; i < MAX_SEL_Y + 1; i++) {
            trace_l(T_DEBUG, "pow is ", pow(0.5f, MAX_SEL_Y - i + 2));
            sel_distr_y.push_back(sel_and_val(pow(0.5f, MAX_SEL_Y - i + 2 ) , i));
        }

        if (!repl_mgr.containsAll( ARRAY_SIZE, RELATION, TABLE, X)) {
            repl_mgr.deleteAll(RELATION, TABLE, X);

            auto xCol = generate_share_vector_pers( ARRAY_SIZE, sel_distr_x, 0 );
            xCol->setRelation(RELATION);
            xCol->setTable(TABLE);
            xCol->setAttribute(X);

            repl_mgr.constructAll(xCol);
        }
        else {
            auto status = repl_mgr.getStatus(RELATION, TABLE, X);
            auto pCol = status->getPersistentColumn(0);
            trace_l(T_INFO, "Got persistent column with r: ", pCol->getRelation(), ", t: ", pCol->getTable(), ", a: ", pCol->getAttribute());

            for (size_t i = 0; i < node_number; i++) {
                auto vCol = repl_mgr.constructVColumnAsync(i, pCol, pCol->get_count_values() * sizeof(uint64_t), i);
                repl_mgr.insert(vCol);
            }
        }

        if (!repl_mgr.containsAll(ARRAY_SIZE, RELATION, TABLE, Y)) {
            repl_mgr.deleteAll(RELATION, TABLE, Y);

            auto yCol = generate_share_vector_pers( ARRAY_SIZE, sel_distr_y, 0);
            yCol->setRelation(RELATION);
            yCol->setTable(TABLE);
            yCol->setAttribute(Y);

            repl_mgr.constructAll(yCol);
        }
        else {
            auto status = repl_mgr.getStatus(RELATION, TABLE, Y);
            auto pCol = status->getPersistentColumn(0);

            for (size_t i = 0; i < node_number; i++) {
                auto vCol = repl_mgr.constructVColumnAsync(i, pCol, pCol->get_count_values() * sizeof(uint64_t), i);
                repl_mgr.insert(vCol);
            }
        }

        repl_mgr.joinAllThreads();
    }

    std::chrono::time_point<std::chrono::system_clock> starttime;
    std::chrono::time_point<std::chrono::system_clock> endtime;

    inline void start()
    {
        starttime = std::chrono::system_clock::now();
    }

    inline void end()
    {
        endtime = std::chrono::system_clock::now();
    }

    void outCsv()
    {
        std::chrono::duration<double> dur = endtime - starttime;

        std::cout << dur.count();
    }

    void printUnit()
    {
        std::cout << "seconds";
    }

    void comma()
    {
        std::cout << ",";
    }

    void printColumnSize()
    {
        std::cout << ARRAY_SIZE;
    }

    void printNode(size_t node)
    {
        std::cout << node;
    }

    void printSelectivity(persistent_ptr<MultiValTreeIndex> tree, const uint64_t val)
    {
        auto buck = tree->find(val);
        size_t all = tree->getCountValues();
        size_t countBuck = buck->getCountValues();

        float sel = static_cast<float>(countBuck) / all;
        std::cout << sel;
    }

    void nextCsvRow()
    {
        std::cout << std::endl;
    }

    // Select sum(x) from r where y = c
    template< typename index_structure_ptr >
    void runIndex(const column<uncompr_f> * xCol, index_structure_ptr index, const uint64_t selection, size_t runnode1, size_t runnode2)
    {
        numa_run_on_node(runnode1);
        start();
        auto select = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( index, selection);
        numa_run_on_node(runnode2);
        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, select);

        auto res = agg_sum<ps, uncompr_f>(projection);
        end();
        outCsv();

        delete select;
        delete projection;
        delete res;
    }

    template< typename col_ptr >
    void runCol(const column<uncompr_f> * xCol, col_ptr col, const uint64_t selection, size_t runnode1, size_t runnode2)
    {
        numa_run_on_node(runnode1);
        start();
        auto select = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( col, selection);
        numa_run_on_node(runnode2);
        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, select);
        auto res = agg_sum<ps, uncompr_f>(projection);

        end();
        outCsv();

        delete select;
        delete projection;
        delete res;
    }

    using TempColPtr = std::unique_ptr<const column<uncompr_f>>;
    void main() {
        auto initializer = RootInitializer::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        auto yStatus = repl_mgr.getStatus(RELATION, TABLE, Y);
        auto xStatus = repl_mgr.getStatus(RELATION, TABLE, X);
        std::cout << "Column Size in Tuples,Measure Unit,Selectivity,Volatile column,Persistent column,Persistent Tree,Persistent Hashmap,Persistent skiplist" << std::endl;
        numa_run_on_node(0);

        for (size_t node = 0; node < node_number; node++) {
            auto xCol = xStatus->getPersistentColumn(node)->convert();
            auto yTree = yStatus->getMultiValTreeIndex(node);
            auto yHash = yStatus->getHashMapIndex(node);
            auto ySkip = yStatus->getSkipListIndex(node);
            auto yPCol = yStatus->getPersistentColumn(node)->convert();
            auto yVCol = yStatus->getVColumn(node);

            for (uint64_t iterations = 0; iterations < 40; iterations++) {
                for (size_t val = 1; val < MAX_SEL_Y + 1; val++) {
                    printColumnSize();
                    comma();
                    printUnit();
                    comma();
                    printSelectivity(yTree, val);
                    comma();
                    printNode(node);
                    comma();
                    runCol  (xCol, yVCol, val, 0, 0);
                    comma();
                    runCol  (xCol, yPCol, val, 0, 0);
                    comma();
                    runIndex(xCol, yTree, val, 0, 0); 
                    comma();
                    runIndex(xCol, yHash, val, 0, 0);
                    comma();
                    runIndex(xCol, ySkip, val, 0, 0);
                    nextCsvRow();
                }
            }

            delete xCol;
            delete yPCol;
        }

        // Mixed node workload
        auto xCol = xStatus->getPersistentColumn(0)->convert();

        auto yTree = yStatus->getMultiValTreeIndex(1);
        auto yHash = yStatus->getHashMapIndex(1);
        auto ySkip = yStatus->getSkipListIndex(1);
        auto yPCol = yStatus->getPersistentColumn(1)->convert();
        auto yVCol = yStatus->getVColumn(1);

        for (uint64_t iterations = 0; iterations < 40; iterations++) {
            for (size_t val = 1; val < MAX_SEL_Y + 1; val++) {
                printColumnSize();
                comma();
                printUnit();
                comma();
                printSelectivity(yTree, val);
                comma();
                printNode(3);
                comma();
                runCol  (xCol, yVCol, val, 1, 0);
                comma();
                runCol  (xCol, yPCol, val, 1, 0);
                comma();
                runIndex(xCol, yTree, val, 1, 0); 
                comma();
                runIndex(xCol, yHash, val, 1, 0);
                comma();
                runIndex(xCol, ySkip, val, 1, 0);
                nextCsvRow();
            }
        }

        delete xCol;
        delete yPCol;
    }

};

int main( void ) {
    // Setup phase: figure out node configuration
    auto initializer = RootInitializer::getInstance();

    if ( !initializer.isNuma() ) {
        trace_l(T_EXIT, "Current setup does not support NUMA, exiting...");
        return -1;
    }
    initializer.initPmemPool(std::string("NVMDSBench"), std::string("NVMDS"));

    Main prog;
    prog.initData();
    prog.main();


    return 0;
}