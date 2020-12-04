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
char const * Y0 = "y0";
char const * Y1 = "y1";
char const * Y2 = "y2";
char const * Y3 = "y3";
char const * Y4 = "y4";
char const * Z = "z";

constexpr auto ARRAY_SIZE = COLUMN_SIZE / sizeof(uint64_t);

class Main {
public:
    Main() : repl_mgr(ReplicationManager::getInstance())
    {
    }

    ReplicationManager & repl_mgr;

    using Distr = std::vector<sel_and_val>;
    std::vector<std::tuple<ReplicationStatus*, Distr>> y_status_and_distr;

    void constructBySel(std::vector<sel_and_val> distr, size_t elem_count, std::string relation, std::string table, std::string attribute)
    {
        auto & initializer = RootInitializer::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        if (!repl_mgr.containsAll(elem_count, relation, table, attribute)) {
            repl_mgr.deleteAll(relation, table, attribute);

            auto col = generate_share_vector_pers( elem_count, distr, 0);
            col->setRelation(relation);
            col->setTable(table);
            col->setAttribute(attribute);

            repl_mgr.constructAll(col);
        }
        else {
            auto status = repl_mgr.getStatus(relation, table, attribute);
            auto pCol = status->getPersistentColumn(0);

            for (size_t i = 0; i < node_number; i++) {
                auto vCol = repl_mgr.constructVColumnAsync(i, pCol, pCol->get_count_values() * sizeof(uint64_t), i);
                repl_mgr.insert(vCol);
            }
        }
    }

    const unsigned MAX_SEL_X = 999;
    const unsigned MAX_SEL_Y = 10;

    void initData() {
        auto & initializer = RootInitializer::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        repl_mgr.init(node_number);

        std::vector<sel_and_val> sel_distr_x;
        for (unsigned i = 1; i < MAX_SEL_X + 1; i++) {
            sel_distr_x.push_back(sel_and_val(0.001f, i));
        }

        std::vector<sel_and_val> sel_distr_y0;
        for (unsigned i = 1; i < MAX_SEL_Y + 1; i++) {
            sel_distr_y0.push_back(sel_and_val(pow(0.5f, MAX_SEL_Y - i + 2 ) , i));
        }

        std::vector<sel_and_val> sel_distr_y1;
        sel_distr_y1.push_back(sel_and_val(0.125f, 2));
        sel_distr_y1.push_back(sel_and_val(0.375f, 3));
        sel_distr_y1.push_back(sel_and_val(0.5f, 4));

        std::vector<sel_and_val> sel_distr_y2;
        sel_distr_y2.push_back(sel_and_val(0.1875f, 1));
        sel_distr_y2.push_back(sel_and_val(0.625f, 2));

        std::vector<sel_and_val> sel_distr_y3;
        sel_distr_y3.push_back(sel_and_val(0.75f, 1));

        std::vector<sel_and_val> sel_distr_y4;
        sel_distr_y4.push_back(sel_and_val(0.875f, 1));

        trace_l(T_DEBUG, "Constructing x DSes");
        constructBySel(sel_distr_x, ARRAY_SIZE, RELATION, TABLE, X);
        trace_l(T_DEBUG, "Constructing y0 DSes");
        constructBySel(sel_distr_y0, ARRAY_SIZE, RELATION, TABLE, Y0);
        trace_l(T_DEBUG, "Constructing y1 DSes");
        constructBySel(sel_distr_y1, ARRAY_SIZE, RELATION, TABLE, Y1);
        trace_l(T_DEBUG, "Constructing y2 DSes");
        constructBySel(sel_distr_y2, ARRAY_SIZE, RELATION, TABLE, Y2);
        trace_l(T_DEBUG, "Constructing y3 DSes");
        constructBySel(sel_distr_y3, ARRAY_SIZE, RELATION, TABLE, Y3);
        trace_l(T_DEBUG, "Constructing y4 DSes");
        constructBySel(sel_distr_y4, ARRAY_SIZE, RELATION, TABLE, Y4);

        repl_mgr.joinAllThreads();

        y_status_and_distr.emplace_back(repl_mgr.getStatus(RELATION, TABLE, Y0), sel_distr_y0);
        y_status_and_distr.emplace_back(repl_mgr.getStatus(RELATION, TABLE, Y1), sel_distr_y1);
        y_status_and_distr.emplace_back(repl_mgr.getStatus(RELATION, TABLE, Y2), sel_distr_y2);
        y_status_and_distr.emplace_back(repl_mgr.getStatus(RELATION, TABLE, Y3), sel_distr_y3);
        y_status_and_distr.emplace_back(repl_mgr.getStatus(RELATION, TABLE, Y4), sel_distr_y4);
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
        auto & initializer = RootInitializer::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        /*auto y0Status = repl_mgr.getStatus(RELATION, TABLE, Y0);
        auto y1Status = repl_mgr.getStatus(RELATION, TABLE, Y1);
        auto y2Status = repl_mgr.getStatus(RELATION, TABLE, Y2);
        auto y3Status = repl_mgr.getStatus(RELATION, TABLE, Y3);
        auto y4Status = repl_mgr.getStatus(RELATION, TABLE, Y4);*/

        auto xStatus = repl_mgr.getStatus(RELATION, TABLE, X);
        std::cout << "Column Size in Tuples,Measure Unit,Selectivity,Volatile column,Persistent column,Persistent Tree,Persistent Hashmap,Persistent skiplist" << std::endl;
        numa_run_on_node(0);

        for (auto i : y_status_and_distr) {
            auto yStatus = std::get<0>(i);
            std::vector<sel_and_val> distr = std::get<1>(i);

            for (size_t node = 0; node < node_number; node++) {
                auto xCol = xStatus->getPersistentColumn(node)->convert();
                auto yTree = yStatus->getMultiValTreeIndex(node);
                auto yHash = yStatus->getHashMapIndex(node);
                auto ySkip = yStatus->getSkipListIndex(node);
                auto yPCol = yStatus->getPersistentColumn(node)->convert();
                auto yVCol = yStatus->getVColumn(node);

                for (uint64_t iterations = 0; iterations < 40; iterations++) {
                    for (auto j : distr) {
                        uint64_t val = j.attr_value;
                        //trace_l(T_INFO, "Selectivity is supposed to be, ", j.selectivity);
                        //for (size_t val = 1; val < MAX_SEL_Y + 2; val++) {
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
        }

        // Mixed node workload
        /*auto xCol = xStatus->getPersistentColumn(0)->convert();

        auto yTree = yStatus->getMultiValTreeIndex(1);
        auto yHash = yStatus->getHashMapIndex(1);
        auto ySkip = yStatus->getSkipListIndex(1);
        auto yPCol = yStatus->getPersistentColumn(1)->convert();
        auto yVCol = yStatus->getVColumn(1);

        for (uint64_t iterations = 0; iterations < 40; iterations++) {
            for (size_t val = 1; val < MAX_SEL_Y + 2; val++) {
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
        delete yPCol;*/
    }

};

int main( void ) {
    // Setup phase: figure out node configuration
    auto & initializer = RootInitializer::getInstance();

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
