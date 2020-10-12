#pragma once

#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>
#include <core/tracing/trace.h>

#include <vector>

#include <libpmemobj++/pool.hpp>

namespace morphstore {

struct root;

class RootManager {

private:
    std::vector<pmem::obj::pool<root> *> m_pops;
    bool m_init = false;

    RootManager() : m_pops() {}

public:
    static RootManager& getInstance()
    {
        static RootManager instance;

        return instance;
    }

    ~RootManager()
    {
    }

    pmem::obj::pool<root> & getPop(size_t node_number)
    {
        //trace_l(T_DEBUG, "Getting pop for node ", node_number);
        return *m_pops[node_number];
    }

    void drainAll()
    {
        for (auto i : m_pops )
            i->drain();
    }

    void closeAll()
    {
        for (auto i : m_pops )
            i->close();
    }

    void printBin()
    {
        size_t node = 0;
        for (auto i : m_pops) {
            uint64_t * addr = reinterpret_cast<uint64_t*>(i);

            size_t c = 0;
            std::cerr << "Binary of pop for node " << node << std::endl;
            do {
                std::cerr << std::hex << addr[c] << " ";
                c++;
            } while (c < sizeof(pmem::obj::pool<root>));
            std::cerr << std::endl;
            node++;
        }
    }

    void set(pmem::obj::pool<root> * pop, size_t index)
    {
        //trace_l(T_DEBUG, "Set pop for node ", index);
        if (m_pops.size() <= index)
            m_pops.reserve(index+1);
        auto iter = m_pops.begin();
        iter = std::next(iter, index);

        m_pops.insert(iter, pop);
    }
};

} // namespace morphstore
