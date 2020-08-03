#pragma once

#include <numa.h>
#include <core/storage/column_helper.h>

using morphstore::column_meta_data;

namespace storage {

// Note: PMDK does not support polymorphism, we need to use ducktyping
class VolatileColumn /*: public Column*/ {

public:
    VolatileColumn(
        size_t byteSize, size_t numa_node) :
        m_MetaData{ 0, 0, 0, byteSize, false }
    {
        m_byteSize = byteSize;
        m_columnData = numa_alloc_onnode(byteSize, numa_node);
    }

    void expand(size_t expansionSize)
    {
        m_columnData = numa_realloc(m_columnData, getAbsoluteSize(), getAbsoluteSize() + expansionSize);
        m_byteSize = getAbsoluteSize() + expansionSize;
        set_meta_data(m_byteSize / (sizeof(uint64_t)), m_byteSize);
    }

    void shrink(size_t shrinkSize)
    {
        m_columnData = numa_realloc(m_columnData, getAbsoluteSize(), getAbsoluteSize() - shrinkSize);
        m_byteSize = getAbsoluteSize() - shrinkSize;
        set_meta_data(m_byteSize / (sizeof(uint64_t)), m_byteSize);
    }

    uint64_t* get_data()
    {
        return (uint64_t*) m_columnData;
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
    void* m_columnData;

    column_meta_data m_MetaData;

    size_t m_byteSize;
    size_t m_entries;
    size_t m_fieldLength;
};

}
