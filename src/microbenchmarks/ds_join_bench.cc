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
#include <core/operators/scalar/join_uncompr.h>

#include <core/utils/measure.h>

#include <numa.h>
#include <vector>

using namespace morphstore;
using namespace pmem::obj;
using namespace vectorlib;

using ps = scalar<v64<uint64_t>>;

char const * RELATION = "join";

char const * TABLE1 = "r";
char const * TABLE2 = "s";

char const * X = "x";
char const * Y = "y";
char const * Z = "z";

constexpr auto ARRAY_SIZE = COLUMN_SIZE / sizeof(uint64_t);
constexpr size_t ATTR_DIST = ARRAY_SIZE / 4096;

class Main {
public:
    Main() : repl_mgr(ReplicationManager::getInstance())
    {
    }

    using Distr = std::vector<sel_and_val>;
    std::vector<std::tuple<ReplicationStatus*, Distr>> y_status_and_distr;

    ReplicationManager & repl_mgr;

    // Select r.x, s.z from r, s where r.y = s.y

    void initData() {
        auto initializer = RootInitializer::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        repl_mgr.init(node_number);

        // R.x
        repl_mgr.deleteAll(RELATION, TABLE1, X);

        auto r_x = generate_sorted_unique_pers(ARRAY_SIZE, 0);
        r_x->setRelation(RELATION);
        r_x->setTable(TABLE1);
        r_x->setAttribute(X);
        repl_mgr.insert(r_x);


        const uint64_t iters = 10;
        std::vector<sel_and_val> sel_distr_y[iters];
        for (unsigned j = 0; j < iters; j++) {
            Distr temp;
            for (unsigned i = 1; i < ATTR_DIST / (j + 1); i++) {
                temp.push_back(sel_and_val(1.0f / ATTR_DIST * j, i));
            }
            sel_distr_y[j] = temp;
            repl_mgr.deleteAll(RELATION, TABLE1, std::string(Y) + std::to_string(j));

            auto r_y = generate_share_vector_pers( ARRAY_SIZE, temp, 0 );
            r_y->setRelation(RELATION);
            r_y->setTable(TABLE1);
            r_y->setAttribute( std::string(Y) + std::to_string(j));

            repl_mgr.constructAll(r_y);
            y_status_and_distr.emplace_back(repl_mgr.getStatus(RELATION, TABLE1, std::string(Y) + std::to_string(j)), temp);
        }

        repl_mgr.deleteAll(RELATION, TABLE2, Y);

        // S.y
        auto s_y = generate_sorted_unique_pers( ATTR_DIST, 0 );
        s_y->setRelation(RELATION);
        s_y->setTable(TABLE2);
        s_y->setAttribute(Y);

        repl_mgr.constructAll(s_y);

        // S.z
        auto s_z = generate_sorted_unique_pers( ARRAY_SIZE, 0);
        s_z->setRelation(RELATION);
        s_z->setTable(TABLE2);
        s_z->setAttribute(Z);
        repl_mgr.insert(s_z);

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

    void printSelectivity(Distr distr)
    {
        std::cout << distr[0].selectivity;
    }

    void nextCsvRow()
    {
        std::cout << std::endl;
    }

    // Select sum(x) from r where y = c
    /*template< typename index_structure_ptr >
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
    }*/

    using BucketPtr = persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>;

    template< typename index_structure_ptr1, typename index_structure_ptr2 >
    void runIndex(persistent_ptr<PersistentColumn> r_x,
            index_structure_ptr1 r_y,
            index_structure_ptr2 s_y,
            persistent_ptr<PersistentColumn> s_z)
    {
        // Conversion to column<uncompr_f> to use implemented operators
        auto r_x_copy = r_x->convert();
        auto s_z_copy = s_z->convert();

        start();
        auto resTuple = ds_join<index_structure_ptr1, index_structure_ptr2, BucketPtr, BucketPtr>(r_y, s_y);
        auto r_x_res = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(r_x_copy, std::get<0>(resTuple));
        auto s_z_res = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(s_z_copy, std::get<1>(resTuple));
        end();
        outCsv();

        delete r_x_copy;
        delete s_z_copy;

        delete r_x_res;
        delete s_z_res;

        delete std::get<0>(resTuple);
        delete std::get<1>(resTuple);
    }

    void runCol(persistent_ptr<PersistentColumn> r_x,
            const column<uncompr_f> * r_y,
            const column<uncompr_f> * s_y,
            persistent_ptr<PersistentColumn> s_z)
    {
        // Conversion to column<uncompr_f> to use implemented operators
        auto r_x_copy = r_x->convert();
        auto s_z_copy = s_z->convert();

        start();
        auto resTuple = nested_loop_join<ps, uncompr_f, uncompr_f, uncompr_f, uncompr_f>(r_y, s_y);
        auto r_x_res = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(r_x_copy, std::get<0>(resTuple));
        auto s_z_res = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(s_z_copy, std::get<1>(resTuple));
        end();
        outCsv();

        delete r_x_copy;
        delete s_z_copy;

        delete r_x_res;
        delete s_z_res;

        delete std::get<0>(resTuple);
        delete std::get<1>(resTuple);
    }

    using TempColPtr = std::unique_ptr<const column<uncompr_f>>;
    void main() {
        auto initializer = RootInitializer::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        auto r_x = repl_mgr.getStatus(RELATION, TABLE1, X)->getPersistentColumn(0);
        auto s_z = repl_mgr.getStatus(RELATION, TABLE2, Z)->getPersistentColumn(0);

        //auto r_yStatus = repl_mgr.getStatus(RELATION, TABLE1, Y);
        auto s_yStatus = repl_mgr.getStatus(RELATION, TABLE2, Y);
        std::cout << "Column Size in Tuples,Measure Unit,Selectivity,Volatile column,Persistent column,Persistent Tree,Persistent Hashmap,Persistent skiplist" << std::endl;
        numa_run_on_node(0);

        for (auto i : y_status_and_distr) {
            auto r_yStatus = std::get<0>(i);
            std::vector<sel_and_val> distr = std::get<1>(i);

            for (size_t node = 0; node < node_number; node++) {
                auto r_yTree = r_yStatus->getMultiValTreeIndex(node);
                auto r_yHash = r_yStatus->getHashMapIndex(node);
                auto r_ySkip = r_yStatus->getSkipListIndex(node);
                auto r_yPCol = r_yStatus->getPersistentColumn(node)->convert();
                auto r_yVCol = r_yStatus->getVColumn(node);

                auto s_yTree = s_yStatus->getMultiValTreeIndex(node);
                auto s_yHash = s_yStatus->getHashMapIndex(node);
                auto s_ySkip = s_yStatus->getSkipListIndex(node);
                auto s_yPCol = s_yStatus->getPersistentColumn(node)->convert();
                auto s_yVCol = s_yStatus->getVColumn(node);

                for (uint64_t iterations = 0; iterations < 40; iterations++) {
                    printColumnSize();
                    comma();
                    printUnit();
                    comma();
                    printSelectivity(distr);
                    comma();
                    printNode(node);
                    comma();
                    runCol  (r_x, r_yVCol, s_yVCol, s_z);
                    comma();
                    runCol  (r_x, r_yPCol, s_yPCol, s_z);
                    comma();
                    runIndex(r_x, r_yTree, s_yTree, s_z);
                    comma();
                    runIndex(r_x, r_yHash, s_yHash, s_z);
                    comma();
                    runIndex(r_x, r_ySkip, s_ySkip, s_z);
                    nextCsvRow();
                }

                delete r_yPCol;
                delete s_yPCol;
            }
        }

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
