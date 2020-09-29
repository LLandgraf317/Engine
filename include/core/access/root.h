#pragma once

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/container/vector.hpp>

#include <core/access/RootManager.h>
#include <core/tracing/trace.h>
#include <core/index/IndexDef.h>

#include "config.h"

#include <unistd.h>
#include <numa.h>

#include <vector>

namespace morphstore {

class PersistentColumn;

using pmem::obj::persistent_ptr;
using pmem::obj::vector;
using pmem::obj::make_persistent;

struct root {
    persistent_ptr<vector<persistent_ptr<PersistentColumn>>> cols;

    persistent_ptr<vector<persistent_ptr<MultiValTreeIndex>>> treeIndeces;
    persistent_ptr<vector<persistent_ptr<SkipListIndex>>> skipListIndeces;
    persistent_ptr<vector<persistent_ptr<HashMapIndex>>> hashMapIndeces;

    persistent_ptr<vector<persistent_ptr<CLTreeIndex>>> clTreeIndeces;
    persistent_ptr<vector<persistent_ptr<CLSkipListIndex>>> clSkipListIndeces;
    persistent_ptr<vector<persistent_ptr<CLHashMapIndex>>> clHashMapIndeces;
};

struct root_retrieval {
    pmem::obj::pool<root> pop;
    bool read_from_file_successful;
};

class RootInitializer {

    std::string m_LayoutName;
    std::string m_FileName;
    uint64_t m_PoolSize;
    //static constexpr auto LAYOUT = "NVMDS";
    static constexpr uint64_t POOL_SIZE = 1024 * 1024 * 1024ul * ENV_POOL_SIZE;  //< 4GB

    std::vector<bool> m_ReadSuccessful;

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

    bool isNVMRetrieved(size_t pmemNode)
    {
        return m_ReadSuccessful[pmemNode];
    }

    root_retrieval getPoolRoot(int pmemNode)
    {
        root_retrieval retr;
        std::string path = (pmemNode == 0 ? gPmemPath0 : gPmemPath1) + m_FileName;
        const std::string& gPmem = (pmemNode == 0 ? gPmemPath0 : gPmemPath1);

        if (access(path.c_str(), F_OK) != 0) {
            mkdir(gPmem.c_str(), 0777);
            trace_l(T_INFO, "Creating new file on ", path);
            retr.pop = pmem::obj::pool<root>::create(path, m_LayoutName, m_PoolSize);

            retr.read_from_file_successful = false;

            pmem::obj::transaction::run(retr.pop, [&]() {
                retr.pop.root()->cols            = make_persistent<vector<persistent_ptr<PersistentColumn>>>();
                retr.pop.root()->skipListIndeces = make_persistent<vector<persistent_ptr<SkipListIndex>>>();
                retr.pop.root()->treeIndeces     = make_persistent<vector<persistent_ptr<MultiValTreeIndex>>>();
                retr.pop.root()->hashMapIndeces  = make_persistent<vector<persistent_ptr<HashMapIndex>>>();
            });
        }
        else {
            trace_l(T_INFO, "File already existed, opening and returning.");
            retr.pop = pmem::obj::pool<root>::open(path, m_LayoutName);

            retr.read_from_file_successful = true;
        }

        return retr;
    }

    void cleanUp()
    {
        std::string path0 = gPmemPath0 + m_FileName;
        std::string path1 = gPmemPath1 + m_FileName;

        remove(path0.c_str());
        remove(path1.c_str());
    }

    void initPmemPool(std::string filename, std::string layoutname, uint64_t poolSize = POOL_SIZE)
    {
        m_FileName = filename;
        m_LayoutName = layoutname;
        m_PoolSize = poolSize;

        unsigned node_number = numa_max_node() + 1;

        trace_l(T_DEBUG, "Current max node number: ", node_number);

        RootManager& root_mgr = RootManager::getInstance();
        root_retrieval retr;
        for (unsigned int i = 0; i < node_number; i++) {
            retr = getPoolRoot(i);
            root_mgr.add(retr.pop);

            m_ReadSuccessful.push_back(retr.read_from_file_successful);
        }
    }

    static size_t getNumaNodeCount()
    {
        return numa_max_node() + 1;
    }
};

}
