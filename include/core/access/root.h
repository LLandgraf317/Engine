#pragma once

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <core/access/RootManager.h>
#include <core/tracing/trace.h>

#include <nvmdatastructures/src/pbptrees/PBPTree.hpp>

#include <unistd.h>
#include <numa.h>

namespace morphstore {
using namespace dbis::pbptrees;

struct root {
    pmem::obj::persistent_ptr<uint64_t> col_count;
};

struct root_retrieval {
    pmem::obj::pool<root> pop;
    bool read_from_file_successful;
};

class RootInitializer {

    static constexpr auto LAYOUT = "NVMDS";
    static constexpr auto POOL_SIZE = 1024 * 1024 * 1024 * 1ull;  //< 4GB

    ~RootInitializer()
    {
        trace_l(T_DEBUG, "Destroying RootInitializer");
    }

public:

    static RootInitializer& getInstance()
    {
        static RootInitializer instance;

        return instance;
    }

    static root_retrieval getPoolRoot(int pmemNode)
    {
        root_retrieval retr;
        std::string path = (pmemNode == 0 ? dbis::gPmemPath0 : dbis::gPmemPath1) + "NVMDSBench";
        const std::string& gPmem = (pmemNode == 0 ? dbis::gPmemPath0 : dbis::gPmemPath1);

        if (access(path.c_str(), F_OK) != 0) {
            mkdir(gPmem.c_str(), 0777);
            trace_l(T_INFO, "Creating new file on ", path);
            retr.pop = pmem::obj::pool<root>::create(path, LAYOUT, POOL_SIZE);

            retr.read_from_file_successful = false;
        }
        else {
            trace_l(T_INFO, "File already existed, opening and returning.");
            retr.pop = pmem::obj::pool<root>::open(path, LAYOUT);
            retr.read_from_file_successful = true;
        }

        return retr;
    }

    static void initPmemPool()
    {
        unsigned node_number = numa_max_node() + 1;

        trace_l(T_DEBUG, "Current max node number: ", node_number);

        RootManager& root_mgr = RootManager::getInstance();
        root_retrieval retr;
        for (unsigned int i = 0; i < node_number; i++) {
            retr = getPoolRoot(i);
            root_mgr.add(retr.pop);
        }
    }

    static size_t getNumaNodeCount()
    {
        return numa_max_node() + 1;
    }
};

} // namespace morphstore
