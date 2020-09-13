#pragma once

#include <libpmemobj/ctl.h>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#include <core/utils/printing.h>
#include <core/utils/basic_types.h>
#include <core/utils/helper_types.h>
#include <core/storage/column_helper.h>
#include <core/access/root.h>
#include <core/access/RootManager.h>
#include <core/tracing/trace.h>

namespace morphstore {

using pmem::obj::persistent_ptr;
using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::make_persistent_atomic;
using pmem::obj::delete_persistent_atomic;
using pmem::obj::pool;
using pmem::obj::transaction;
using pmem::obj::p;

using morphstore::column_meta_data;

// No Polymorphism because PMDK disallows it
class PersistentColumn /*: public Column*/ {
public:
    PersistentColumn(bool /*isPersistent*/, size_t p_SizeAllocatedByte, int numa_node ) : PersistentColumn(
            std::string("null"), std::string("null"), std::string("null"), p_SizeAllocatedByte, numa_node)
      {
         //
      };

    PersistentColumn(
        std::string relation, std::string table_name, std::string attr_name, size_t byteSize, size_t numa_node)
    {
        RootManager& mgr = RootManager::getInstance();
        trace(T_INFO, "Creating column for table ", table_name, " and attr name ", attr_name);
        pool<root> pop = mgr.getPop(numa_node);

        m_byteSize = byteSize;
        m_persistentData = pmemobj_tx_alloc(byteSize, 0);
        m_numaNode = numa_node;

        m_relation = make_persistent<char[]>(relation.length() + 1);
        m_rl = relation.length() + 1;
        pop.memcpy_persist(m_relation.raw_ptr(), relation.c_str(), relation.length() + 1);

        m_table = make_persistent<char[]>(table_name.length() + 1);
        m_tl = table_name.length() + 1;
        pop.memcpy_persist(m_table.raw_ptr(), table_name.c_str(), table_name.length() + 1);

        m_attribute = make_persistent<char[]>(attr_name.length() + 1);
        m_al = attr_name.length() + 1;
        pop.memcpy_persist(m_attribute.raw_ptr(), attr_name.c_str(), table_name.length() + 1);
    }

    void prepareDest()
    {
        delete_persistent_atomic<char[]>(m_relation, m_rl);
        delete_persistent_atomic<char[]>(m_table, m_tl);
        delete_persistent_atomic<char[]>(m_attribute, m_al);

        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = *std::next(mgr.getPops(), m_numaNode);

        transaction::run(pop, [&] {
            pmemobj_tx_free( m_persistentData.raw() );
        });
    }

    const column<uncompr_f>* convert()
    {
        auto col = new column<uncompr_f>(this->getAbsoluteSize(), m_numaNode, this->get_data());
        col->set_meta_data(this->get_count_values(), this->m_byteSize);
        return col; 
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

    inline voidptr_t get_data( void ) const {
        return &(m_persistentData[0]);
    }

    inline void set(size_t index, uint64_t value)
    {
        RootManager& mgr = RootManager::getInstance();
        pool<root> pop = mgr.getPop(m_numaNode);

        persistent_ptr<uint64_t> ptr = &((uint64_t*) get_data())[index];
        pmem::obj::make_persistent_atomic<uint64_t>(pop, ptr, value); 
    }

    size_t getAbsoluteSize()
    {
        return m_byteSize;
    }

    inline void set_meta_data(
         size_t p_CountValues,
         size_t p_SizeUsedByte,
         size_t /*p_SizeComprByte*/
             )  {

        m_entries = p_CountValues;
        m_byteSize = p_SizeUsedByte;
        //m_MetaData.m_SizeComprByte = p_SizeComprByte;
    }

    inline void set_meta_data( size_t p_CountValues, size_t p_SizeUsedByte )  {
        m_entries = p_CountValues;
        m_byteSize = p_SizeUsedByte;
    }

    size_t get_size()
    {
        return m_byteSize;
    }

    inline size_t get_count_values( void )  {
       return m_entries;
    }

    inline size_t get_size_used_byte( void )  {
       return m_byteSize;
    }
private:
    pmem::obj::persistent_ptr<size_t[]> m_persistentData;
    pmem::obj::persistent_ptr<char[]> m_relation;
    pmem::obj::persistent_ptr<char[]> m_table;
    pmem::obj::persistent_ptr<char[]> m_attribute;

    //column_meta_data m_MetaData;

    p<size_t> m_byteSize;
    p<size_t> m_entries;
    p<size_t> m_fieldLength;

    p<size_t> m_rl;
    p<size_t> m_tl;
    p<size_t> m_al;

    p<size_t> m_numaNode;

};

}

