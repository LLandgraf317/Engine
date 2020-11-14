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
private:
    std::string m_LayoutName;
    std::string m_FileName;
    uint64_t m_PoolSize;
    
    static constexpr uint64_t POOL_SIZE = 1024 * 1024 * 1024ul * ENV_POOL_SIZE;  //< 4GB
    const std::string m_PmemPath = "/mnt/pmem";
    const std::string m_DirName = "";

    std::vector<bool> m_ReadSuccessful;

    RootInitializer() {}

public:
    RootInitializer(RootInitializer const&)               = delete;
    void operator=(RootInitializer const&)  = delete;

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

    bool isNuma()
    {
        return numa_available() >= 0;
    }

    void printPool(pmem::obj::pool<root> pop)
    {
        uint64_t * addr = reinterpret_cast<uint64_t*>(&pop);

        size_t c = 0;
        do {
            std::cerr << std::hex << addr[c] << " ";
            c++;
        } while (c < sizeof(pmem::obj::pool<root>));
        std::cerr << std::endl;
    }

    pmem::obj::pool<root> * getPoolRoot(size_t pmemNode)
    {
        pmem::obj::pool<root> * pop = new pmem::obj::pool<root>();

        std::string path = getDirectory(pmemNode) + m_FileName;
        const std::string& gPmem = getDirectory(pmemNode);

        if (m_ReadSuccessful.size() <= pmemNode)
            m_ReadSuccessful.resize(pmemNode + 1);

        if (access(path.c_str(), F_OK) != 0) {
            mkdir(gPmem.c_str(), 0777);
            trace_l(T_INFO, "Creating new file on ", path);

            *pop = pmem::obj::pool<root>::create(path, m_LayoutName + std::to_string(pmemNode), m_PoolSize);
            m_ReadSuccessful[pmemNode] = false;

            pmem::obj::transaction::run(*pop, [&]() {
                pop->root()->cols            = make_persistent<vector<persistent_ptr<PersistentColumn>>>();
                pop->root()->skipListIndeces = make_persistent<vector<persistent_ptr<SkipListIndex>>>();
                pop->root()->treeIndeces     = make_persistent<vector<persistent_ptr<MultiValTreeIndex>>>();
                pop->root()->hashMapIndeces  = make_persistent<vector<persistent_ptr<HashMapIndex>>>();
            });

        }
        else {
            trace_l(T_INFO, "File ", path, " already existed, opening and returning.");
            *pop = pmem::obj::pool<root>::open(path, m_LayoutName + std::to_string(pmemNode));

            m_ReadSuccessful[pmemNode] = true;
        }
        //std::cerr << "Binary of pop for node " << pmemNode << std::endl;
        //printPool(*pop);

        return pop;
    }

    std::string getDirectory(size_t numaNode)
    {
        return m_PmemPath + std::to_string(numaNode) + "/" + m_DirName;
    }

    void cleanUp()
    {
        for (uint64_t node = 0; node < getNumaNodeCount(); node++)
            remove( (getDirectory(node) + m_FileName).c_str() );

    }

    void initPmemPool(std::string filename, std::string layoutname, uint64_t poolSize = POOL_SIZE)
    {
        //numa_set_strict(1);

        m_FileName = filename;
        m_LayoutName = layoutname;
        m_PoolSize = poolSize;

        unsigned node_number = numa_max_node() + 1;

        trace_l(T_DEBUG, "Current max node number: ", node_number);

        RootManager& root_mgr = RootManager::getInstance();
        for (unsigned int i = 0; i < node_number; i++) {
            pmem::obj::pool<root> * pop;
            pop = getPoolRoot(i);

            //std::cerr << "Binary of pop for node " << i << std::endl;
            //printPool(*pop);
            root_mgr.set(pop, i);
        }
    }

    static size_t getNumaNodeCount()
    {
        return numa_max_node() + 1;
    }
};

}
