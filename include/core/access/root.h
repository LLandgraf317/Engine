#pragma once

#include <list>

#include <core/index/TreeDef.h>
#include <libpmemobj++/p.hpp>

using pmem::obj::pool;

namespace storage {
class PersistentColumn;
}

struct root {
    pptr<TreeType> tree;
    pptr<storage::PersistentColumn[]> col;
    pptr<uint64_t> col_count;
};

class RootManager {

private:
    std::list<pool<root>> m_pops = {};
    bool m_init = false;

public:
    static RootManager& getInstance()
    {
        static RootManager instance;

        return instance;
    }

    std::list<pool<root>>::iterator getPops()
    {
        if (!m_init)
            throw std::exception();

        return m_pops.begin();
    }

    pool<root> getPop(uint64_t node_number)
    {
        if (node_number >= m_pops.size())
            throw std::exception();

        return *std::next(m_pops.begin(), node_number);
    }

    void drainAll()
    {
        for (auto i : m_pops)
            i.drain();
    }

    void closeAll()
    {
        for (auto i : m_pops)
            i.close();
    }

    void add(pool<root> pop)
    {
        if (!m_init)
            m_init = true;
        m_pops.push_back(pop); 
    }
};
