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

constexpr unsigned MAX_SEL_X = 20000;
constexpr unsigned MAX_ITER = 20;

constexpr auto ARRAY_SIZE = COLUMN_SIZE / sizeof(uint64_t);

class Main {
public:
    const unsigned MAX_SEL_Y = 10;
    const unsigned MAX_SEL_Z = 10;

    Main() : repl_mgr(ReplicationManager::getInstance())
    {
    }

    ReplicationManager & repl_mgr;

    void initData() {

        size_t distinct_values = ARRAY_SIZE / NodeBucket<uint64_t, OSP_SIZE>::MAX_ENTRIES;
        std::vector<sel_and_val> sel_distr_x;
        for (unsigned i = 1; i < distinct_values + 1; i++) {
            //trace_l(T_DEBUG, "Inserting in X Distr: key ", i, ", share is ", pow(0.5f, MAX_SEL_X - i + 2));
            sel_distr_x.push_back(sel_and_val( 1.0f / distinct_values, i));
        }

        auto xCol = generate_share_vector_pers( ARRAY_SIZE, sel_distr_x, 0 );
        xCol->setRelation(RELATION);
        xCol->setTable(TABLE);
        xCol->setAttribute(X);

        repl_mgr.constructAllCL(xCol);
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

    void printSelectivity()
    {
       std::cout << "0.001";
    }

    void nextCsvRow()
    {
        std::cout << std::endl;
    }

    // Select sum(x) from r where y = c and z = b
    template< typename index_structure_ptr >
    void runIndex(index_structure_ptr xIndex, size_t runnode)
    {

        column<uncompr_f> * selectRes[MAX_SEL_X];
        numa_run_on_node(runnode);
        start();
        for (uint64_t i = 0; i < MAX_ITER; i++) {
            selectRes[i] = 
                const_cast<column<uncompr_f> *>( index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
                index_structure_ptr, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                    ::apply( xIndex, i*100) );

        }
        end();
        outCsv();

        for (uint64_t i = 0; i < MAX_SEL_X; i++) {
            delete selectRes[i];
        } 
    }

    template< typename col_ptr >
    void runCol(col_ptr xCol, size_t runnode)
    {
        column<uncompr_f> * selectRes[MAX_SEL_X];
        numa_run_on_node(runnode);
        start();
        for (uint64_t i = 0; i < MAX_ITER; i++ ) {
            selectRes[i] = 
                const_cast<column<uncompr_f> *>( my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                    ::apply( xCol, i*100) );

        }
        end();
        outCsv();

        for (uint64_t i = 0; i < MAX_SEL_X; i++) {
            delete selectRes[i];
        } 
    }

    using TempColPtr = std::unique_ptr<const column<uncompr_f>>;
    void main() {
        //auto repl_mgr = ReplicationManager::getInstance();
        auto initializer = RootInitializer::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        std::cout << "Column Size in Tuples,Measure Unit,Selectivity,Volatile column,Persistent column,Persistent Tree,Persistent Hashmap,Persistent skiplist" << std::endl;
        auto xStatus = repl_mgr.getStatus(RELATION, TABLE, X);

        numa_run_on_node(0);

        for (size_t node = 0; node < node_number; node++) {
            auto xTree = xStatus->getMultiValTreeIndex(node);
            auto xHash = xStatus->getHashMapIndex(node);
            auto xSkip = xStatus->getSkipListIndex(node);
            auto xPCol = xStatus->getPersistentColumn(node)->convert();
            auto xVCol = xStatus->getVColumn(node);

            for (uint64_t iterations = 0; iterations < 20; iterations++) {
                printColumnSize();
                comma();
                printUnit();
                comma();
                printSelectivity();
                comma();
                printNode(node);
                comma();
                runCol  (xVCol, 0);
                comma();
                runCol  (xPCol, 0);
                comma();
                runIndex(xTree, 0); 
                comma();
                runIndex(xHash, 0);
                comma();
                runIndex(xSkip, 0);
                nextCsvRow();
            }

            delete xPCol;
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
