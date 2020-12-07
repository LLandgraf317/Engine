
//#include <core/memory/mm_glob.h>
#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>
#include <core/queryprocessing/QueryBookkeeper.h>

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
    Main()
    {
    }

    using Distr = std::vector<sel_and_val>;
    std::vector<std::tuple<ReplicationStatus*, Distr>> y_status_and_distr;

    void constructBySel(std::vector<sel_and_val> distr, size_t elem_count, std::string relation, std::string table, std::string attribute)
    {
        auto & initializer = RootInitializer::getInstance();
        auto & repl_mgr = ReplicationManager::getInstance();
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
        auto & repl_mgr = ReplicationManager::getInstance();
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

    void printSelectivity(persistent_ptr<MultiValTreeIndex> tree, const uint64_t val)
    {
        auto buck = tree->find(val);
        size_t all = tree->getCountValues();
        size_t countBuck = buck->getCountValues();

        float sel = static_cast<float>(countBuck) / all;
        std::cout << sel;
    }

    using TempColPtr = std::unique_ptr<const column<uncompr_f>>;
    void main() {
        auto & optimizer = Optimizer::getInstance();

        for (auto i : y_status_and_distr) {
            auto distr = std::get<1>(i);
            auto status = std::get<0>(i);

            for (auto sv : distr) {
                uint64_t val = sv.attr_value;
                // Warmup
                for (uint64_t i = 0; i < 10; i++)
                    optimizer.executeAllSelectSum(val, RELATION, TABLE, status->getAttribute());
            }
        }

        for (auto i : y_status_and_distr) {
            auto distr = std::get<1>(i);
            auto status = std::get<0>(i);

            for (auto sv : distr) {
                uint64_t val = sv.attr_value;
                optimizer.optimizeSelectSum(val, RELATION, TABLE, status->getAttribute());
            }
        }

        for (auto i : y_status_and_distr) {
            auto distr = std::get<1>(i);
            auto status = std::get<0>(i);

            for (auto sv : distr) {
                uint64_t val = sv.attr_value;
                optimizer.runNaiveSelectSumTree(val, RELATION, TABLE, status->getAttribute());
            }
        }
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
