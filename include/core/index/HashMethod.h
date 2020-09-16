#pragma once


template<class VectorExtension>
struct multiply_mod_hash {

    size_t m_ModSize;

    multiply_mod_hash(size_t modsize) : m_ModSize(modsize) {}

    size_t apply(size_t num)
    {
        return num * num % m_ModSize; 
    }
};

