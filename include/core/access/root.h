#pragma once

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/container/vector.hpp>

#include <core/access/RootManager.h>
#include <core/tracing/trace.h>

#include "config.h"

#include <unistd.h>
#include <numa.h>

namespace morphstore {

class PersistentColumn;
class MultiValTreeIndex;
class HashMapIndex;
class SkipListIndex;

struct root {
    pmem::obj::persistent_ptr<pmem::obj::vector<pmem::obj::persistent_ptr<PersistentColumn>>> cols;

    pmem::obj::persistent_ptr<pmem::obj::vector<pmem::obj::persistent_ptr<MultiValTreeIndex>>> treeIndeces;
    pmem::obj::persistent_ptr<pmem::obj::vector<pmem::obj::persistent_ptr<SkipListIndex>>> skipListIndeces;
    pmem::obj::persistent_ptr<pmem::obj::vector<pmem::obj::persistent_ptr<HashMapIndex>>> hashMapIndeces;
};

struct root_retrieval {
    pmem::obj::pool<root> pop;
    bool read_from_file_successful;
};

class RootInitializer {

    static constexpr auto LAYOUT = "NVMDS";
    static constexpr uint64_t POOL_SIZE = 1024 * 1024 * 1024ul * ENV_POOL_SIZE;  //< 4GB

public:
    ~RootInitializer()
    {
        trace_l(T_DEBUG, "Destroying RootInitializer");
    }

    static RootInitializer& getInstance()
    {
        static RootInitializer instance;

        return instance;
    }

    static root_retrieval getPoolRoot(int pmemNode)
    {
        root_retrieval retr;
        std::string path = (pmemNode == 0 ? gPmemPath0 : gPmemPath1) + "NVMDSBench";
        const std::string& gPmem = (pmemNode == 0 ? gPmemPath0 : gPmemPath1);

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
