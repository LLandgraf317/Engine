#pragma once

#include <libpmemobj/ctl.h>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#include <core/storage/column_helper.h>
#include <core/access/root.h>
#include <core/tracing/trace.h>

namespace storage {

using pmem::obj::persistent_ptr;
using pmem::obj::make_persistent;
using pmem::obj::pool;

using morphstore::column_meta_data;

template <typename Object>
using pptr = persistent_ptr<Object>;

// No Polymorphism because PMDK disallows it
class PersistentColumn /*: public Column*/ {

public:
    PersistentColumn(
        std::string table_name, std::string attr_name, size_t byteSize, size_t numa_node)
        :
        m_MetaData{ 0, 0, 0, byteSize }
    {
        RootManager& mgr = RootManager::getInstance();
        trace(T_INFO, "Creating column for table ", table_name, " and attr name ", attr_name);
        pool<root> pop = *mgr.getPops();

        m_byteSize = byteSize;
        m_persistentData = pmemobj_tx_alloc(byteSize, 0);
        m_numaNode = numa_node;
        m_table = make_persistent<char[]>(table_name.length() + 1);
        pop.memcpy_persist(m_table.raw_ptr(), table_name.c_str(), table_name.length() + 1);

        m_attribute = make_persistent<char[]>(attr_name.length() + 1);
        pop.memcpy_persist(m_attribute.raw_ptr(), attr_name.c_str(), table_name.length() + 1);
    }

    void expand(size_t expansionSize)
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_numaNode);
        //m_testVol = numa_realloc(m_testVol, getAbsoluteSize(), getAbsoluteSize() + expansionSize);

        transaction::run( pop, [&] {
            m_persistentData = pmemobj_tx_realloc(m_persistentData.raw(), getAbsoluteSize() + expansionSize, 0);
            m_byteSize = getAbsoluteSize() + expansionSize;
            set_meta_data(m_byteSize / (sizeof(uint64_t)), m_byteSize);
        });
    }

    void shrink(size_t shrinkSize)
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_numaNode);

        transaction::run( pop, [&] {
            m_persistentData = pmemobj_tx_realloc(m_persistentData.raw(), getAbsoluteSize() - shrinkSize, 0);
            m_byteSize = getAbsoluteSize() - shrinkSize;
            set_meta_data(m_byteSize / (sizeof(uint64_t)), m_byteSize);
        });
    }

    inline uint64_t* get_data()
    {
        //TODO: fix this you lazy code monkey
        // type safety issues etc
        return &(m_persistentData[0]);
    }

    size_t getAbsoluteSize()
    {
        return m_byteSize;
    }

    inline void set_meta_data(
         size_t p_CountValues,
         size_t p_SizeUsedByte,
         size_t p_SizeComprByte
             )  {

        m_MetaData.m_CountLogicalValues = p_CountValues;
        m_MetaData.m_SizeUsedByte = p_SizeUsedByte;
        m_MetaData.m_SizeComprByte = p_SizeComprByte;
    }

    inline void set_meta_data( size_t p_CountValues, size_t p_SizeUsedByte )  {
        set_meta_data(p_CountValues, p_SizeUsedByte, 0);
    }

    size_t get_size()
    {
        return m_byteSize;
    }

    inline size_t get_count_values( void )  {
       return m_MetaData.m_CountLogicalValues;
    }

    inline size_t get_size_used_byte( void )  {
       return m_MetaData.m_SizeUsedByte;
    }
private:
    void* m_testVol;
    pptr<size_t[]> m_persistentData;
    pptr<char[]> m_table;
    pptr<char[]> m_attribute;

    column_meta_data m_MetaData;

    size_t m_byteSize;
    size_t m_entries;
    size_t m_fieldLength;

    size_t m_numaNode;

};

}
