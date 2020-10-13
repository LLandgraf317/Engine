//#include <core/memory/mm_glob.h>
#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>

#include <core/storage/column_gen.h>
#include <core/access/RootManager.h>
#include <core/access/NVMStorageManager.h>
#include <core/storage/PersistentColumn.h>

#include <vector>

using namespace morphstore;

class Main {
public:
    static Main & getInstance() {
        static Main instance;
        return instance;
    }

    void initData() {
        auto initializer = RootInitializer::getInstance();
        auto node_number = initializer.getNumaNodeCount();

        std::vector<sel_and_val> sel_distr;
        const unsigned MAX_SEL_ATTR = 999;
        for (unsigned i = 1; i < MAX_SEL_ATTR + 1; i++) {
            trace_l(T_DEBUG, "pow is ", pow(0.5f, MAX_SEL_ATTR - i + 2));
            sel_distr.push_back(sel_and_val(0.001f, i));
        }

        for (size_t pmemNode = 0; pmemNode < node_number; pmemNode++) {
            auto attrPers = generate_share_vector_pers( ARRAY_SIZE, sel_distr, pmemNode);

            attrColPers.push_back(attrPers);
            attrCols.push_back(generate_share_vector( ARRAY_SIZE, sel_distr, pmemNode);
            addAll(attrPers, pmemNode);
        }

    }

    void main() {

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
    auto node_number = initializer.getNumaNodeCount();

    std::vector<sel_and_val> sel_distr;
    const unsigned MAX_SEL_ATTR = 999;
    for (unsigned i = 1; i < MAX_SEL_ATTR + 1; i++) {
        trace_l(T_DEBUG, "pow is ", pow(0.5f, MAX_SEL_ATTR - i + 2));
        sel_distr.push_back(sel_and_val(0.001f, i));
    }

    auto attrPers = generate_share_vector_pers( ARRAY_SIZE, sel_distr, pmemNode);



    return 0;
}
