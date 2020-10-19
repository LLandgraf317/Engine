
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
#include <core/operators/general_vectorized/intersect_uncompr.h>
#include <core/operators/scalar/agg_sum_uncompr.h>
#include <core/operators/scalar/select_uncompr.h>

#include <core/utils/measure.h>

#include <numa.h>
#include <vector>

using namespace morphstore;
using namespace pmem::obj;
using namespace vectorlib;

using ps = scalar<v64<uint64_t>>;

char const * RELATION = "sel2";
char const * TABLE = "distr";
char const * X = "x";
char const * Y = "y";
char const * Z = "z";

constexpr auto ARRAY_SIZE = COLUMN_SIZE / sizeof(uint64_t);

class Main {
public:
    /*static Main & getInstance() {
        static Main instance;
        return instance;
    }*/

    const unsigned MAX_SEL_X = 999;
    const unsigned MAX_SEL_Y = 10;
    const unsigned MAX_SEL_Z = 10;

    Main() : repl_mgr(ReplicationManager::getInstance())
    {
    }

    ReplicationManager & repl_mgr;

    void initData() {

        std::vector<sel_and_val> sel_distr_x;
        for (unsigned i = 1; i < MAX_SEL_X + 1; i++) {
            //trace_l(T_DEBUG, "Inserting in X Distr: key ", i, ", share is ", pow(0.5f, MAX_SEL_X - i + 2));
            sel_distr_x.push_back(sel_and_val(0.001f, i));
        }

        std::vector<sel_and_val> sel_distr_y;
        for (unsigned i = 1; i < MAX_SEL_Y + 1; i++) {
            //trace_l(T_DEBUG, "Inserting in Y Distr: key ", i, ", share is ", pow(0.5f, MAX_SEL_Y - i + 2));
            sel_distr_y.push_back(sel_and_val(pow(0.5f, MAX_SEL_Y - i + 2 ) , i));
        }

        std::vector<sel_and_val> sel_distr_z;
        for (unsigned i = 1; i < MAX_SEL_Z + 1; i++) {
            trace_l(T_DEBUG, "Inserting in Z Distr: key ", i, ", share is ", pow(0.5f, MAX_SEL_Z - i + 2));
            sel_distr_z.push_back(sel_and_val(pow(0.5f, MAX_SEL_Z - i + 2 ) , i));
        }

        auto xCol = generate_share_vector_pers( ARRAY_SIZE, sel_distr_x, 0 );
        xCol->setRelation(RELATION);
        xCol->setTable(TABLE);
        xCol->setAttribute(X);

        auto yCol = generate_share_vector_pers( ARRAY_SIZE, sel_distr_y, 0);
        yCol->setRelation(RELATION);
        yCol->setTable(TABLE);
        yCol->setAttribute(Y);

        auto zCol = generate_share_vector_pers( ARRAY_SIZE, sel_distr_z, 0);
        zCol->setRelation(RELATION);
        zCol->setTable(TABLE);
        zCol->setAttribute(Z);

        repl_mgr.constructAll(xCol);
        repl_mgr.constructAll(yCol);
        repl_mgr.constructAll(zCol);
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

        std::cout << 1.0f/dur.count();
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

    // Select sum(x) from r where y = c and z = b
    template< typename index_structure_ptr >
    void runIndex(const column<uncompr_f> * xCol, index_structure_ptr yIndex, persistent_ptr<MultiValTreeIndex> zTree, const uint64_t selection1, const uint64_t selection2)
    {
        start();
        auto select1 = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( yIndex, selection1);
        auto select2 = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            persistent_ptr<MultiValTreeIndex>, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( zTree, selection2);
        auto intersect = intersect_sorted<ps, uncompr_f, uncompr_f, uncompr_f >(select1, select2);

        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, intersect);

        auto res = agg_sum<ps, uncompr_f>(projection);
        end();
        outCsv();

        delete select1;
        delete select2;
        delete intersect;
        delete projection;
        delete res;
    }

    template< typename col_ptr >
    void runCol(const column<uncompr_f> * xCol, col_ptr zCol, persistent_ptr<MultiValTreeIndex> zTree, const uint64_t selection1, const uint64_t selection2)
    {
        start();
        auto select1 = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( zCol, selection1);
        /*auto select2 = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( col, selection2);*/
        auto select2 = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            persistent_ptr<MultiValTreeIndex>, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( zTree, selection2);
        auto intersect = intersect_sorted<ps, uncompr_f, uncompr_f, uncompr_f >(select1, select2);

        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, intersect);
        auto res = agg_sum<ps, uncompr_f>(projection);

        end();
        outCsv();

        delete select1;
        delete select2;
        delete intersect;
        delete projection;
        delete res;
    }

    using TempColPtr = std::unique_ptr<const column<uncompr_f>>;
    void main() {
        //auto repl_mgr = ReplicationManager::getInstance();
        auto initializer = RootInitializer::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        std::cout << "Column Size in tuples,Selectivity,Node,Volatile column,Persistent column,Persistent Tree,Persistent Hashmap,Persistent skiplist" << std::endl;
        auto zStatus = repl_mgr.getStatus(RELATION, TABLE, Z);
        auto yStatus = repl_mgr.getStatus(RELATION, TABLE, Y);
        auto xStatus = repl_mgr.getStatus(RELATION, TABLE, X);

        numa_run_on_node(0);

        for (size_t node = 0; node < node_number; node++) {
            auto xCol = xStatus->getPersistentColumn(node)->convert();
            auto zTree = zStatus->getMultiValTreeIndex(node);

            auto yTree = yStatus->getMultiValTreeIndex(node);
            auto yHash = yStatus->getHashMapIndex(node);
            auto ySkip = yStatus->getSkipListIndex(node);
            auto yPCol = yStatus->getPersistentColumn(node)->convert();
            auto yVCol = yStatus->getVColumn(node);

            const uint64_t val2 = 2;

            for (uint64_t iterations = 0; iterations < 20; iterations++) {
                for (size_t val = 1; val < MAX_SEL_Y; val++) {
                    printColumnSize();
                    comma();
                    printSelectivity(yTree, val);
                    comma();
                    printNode(node);
                    comma();
                    runCol  (xCol, yVCol, zTree, val, val2);
                    comma();
                    runCol  (xCol, yPCol, zTree, val, val2);
                    comma();
                    runIndex(xCol, yTree, zTree, val, val2); 
                    comma();
                    runIndex(xCol, yHash, zTree, val, val2);
                    comma();
                    runIndex(xCol, ySkip, zTree, val, val2);
                    nextCsvRow();
                }
            }

            delete xCol;
            delete yPCol;
        }
    }

};

int main( void ) {
    // Setup phase: figure out node configuration
    auto initializer = RootInitializer::getInstance();
    ReplicationManager::getInstance();

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
