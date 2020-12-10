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
char const * X0 = "x";
char const * Y0 = "y0";
char const * Y1 = "y1";
char const * Y2 = "y2";
char const * Y3 = "y3";
char const * Z0 = "z0";
char const * Z1 = "z1";
char const * Z2 = "z2";
char const * Z3 = "z3";

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

    void initData() {
        auto & initializer = RootInitializer::getInstance();
        auto & repl_mgr = ReplicationManager::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        repl_mgr.init(node_number);

        std::vector<sel_and_val> sel_distr_0;
        sel_distr_0.push_back(sel_and_val(0.015625f, 1));
        sel_distr_0.push_back(sel_and_val(0.03125f, 2));
        sel_distr_0.push_back(sel_and_val(0.0625f, 3));
        sel_distr_0.push_back(sel_and_val(0.125f, 4));
        sel_distr_0.push_back(sel_and_val(0.25f, 5));
        sel_distr_0.push_back(sel_and_val(0.5f, 6));

        /*std::vector<sel_and_val> sel_distr_1;
        sel_distr_y2.push_back(sel_and_val(0.625f, 2));

        std::vector<sel_and_val> sel_distr_2;
        sel_distr_y3.push_back(sel_and_val(0.75f, 1));

        std::vector<sel_and_val> sel_distr_3;
        sel_distr_y4.push_back(sel_and_val(0.875f, 1));*/

        trace_l(T_DEBUG, "Constructing x DSes");
        constructBySel(sel_distr_0, ARRAY_SIZE, RELATION, TABLE, X0);

        trace_l(T_DEBUG, "Constructing y0 DSes");
        constructBySel(sel_distr_0, ARRAY_SIZE, RELATION, TABLE, Y0);

        trace_l(T_DEBUG, "Constructing y0 DSes");
        constructBySel(sel_distr_0, ARRAY_SIZE, RELATION, TABLE, Z0);

        repl_mgr.joinAllThreads();

        y_status_and_distr.emplace_back(repl_mgr.getStatus(RELATION, TABLE, Y0), sel_distr_0);

    }

    void printSelectivity(persistent_ptr<MultiValTreeIndex> tree, const uint64_t val)
    {
        auto buck = tree->find(val);
        size_t all = tree->getCountValues();
        size_t countBuck = buck->getCountValues();

        float sel = static_cast<float>(countBuck) / all;
        std::cout << sel;
    }

    void warmup()
    {
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
    }

    void mainBest() {
        PlacementAdvisor & adv = PlacementAdvisor::getInstance();

        std::vector<double> y_selectivities;
        std::vector<double> z_selectivities;

        /*sel_distr_0.push_back(sel_and_val(0.015625f, 1));
        sel_distr_0.push_back(sel_and_val(0.03125f, 2));
        sel_distr_0.push_back(sel_and_val(0.0625f, 3));
        sel_distr_0.push_back(sel_and_val(0.125f, 4));
        sel_distr_0.push_back(sel_and_val(0.25f, 5));
        sel_distr_0.push_back(sel_and_val(0.5f, 6));*/

        uint64_t numThreads = 5;
        for (uint64_t i = 0; i < numThreads; i++) {
            y_selectivities.push_back(0.015625f);
            z_selectivities.push_back(0.5f);
        }

        std::vector<std::vector<double>> selectivitiesPerAttr;
        selectivitiesPerAttr.push_back(y_selectivities);
        selectivitiesPerAttr.push_back(z_selectivities);

        ReplicationDecision * plc = adv.calculatePlacementForWorkload(ARRAY_SIZE, selectivitiesPerAttr);

        auto & optimizer = Optimizer::getInstance();
        optimizer.optimizeDoubleSelectSum(numThreads, plc,
                1, RELATION, TABLE, Y0,
                6, RELATION, TABLE, Z0);

        delete plc;
    }
   
    void mainHi() {
        PlacementAdvisor & adv = PlacementAdvisor::getInstance();

        std::vector<double> y_selectivities;
        std::vector<double> z_selectivities;

        /*sel_distr_0.push_back(sel_and_val(0.015625f, 1));
        sel_distr_0.push_back(sel_and_val(0.03125f, 2));
        sel_distr_0.push_back(sel_and_val(0.0625f, 3));
        sel_distr_0.push_back(sel_and_val(0.125f, 4));
        sel_distr_0.push_back(sel_and_val(0.25f, 5));
        sel_distr_0.push_back(sel_and_val(0.5f, 6));*/

        uint64_t numThreads = 10;
        for (uint64_t i = 0; i < numThreads; i++) {
            y_selectivities.push_back(0.015625f);
            z_selectivities.push_back(0.015625f);
        }

        std::vector<std::vector<double>> selectivitiesPerAttr;
        selectivitiesPerAttr.push_back(y_selectivities);
        selectivitiesPerAttr.push_back(z_selectivities);

        ReplicationDecision * plc = adv.calculatePlacementForWorkload(ARRAY_SIZE, selectivitiesPerAttr);

        auto & optimizer = Optimizer::getInstance();
        optimizer.optimizeDoubleSelectSum(numThreads, plc,
                1, RELATION, TABLE, Y0,
                1, RELATION, TABLE, Z0);

        delete plc;
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
    //prog.warmup();

    prog.mainBest();
    prog.mainHi();


    return 0;
}
