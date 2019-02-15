/**********************************************************************************************
 * Copyright (C) 2019 by MorphStore-Team                                                      *
 *                                                                                            *
 * This file is part of MorphStore - a compression aware vectorized column store.             *
 *                                                                                            *
 * This program is free software: you can redistribute it and/or modify it under the          *
 * terms of the GNU General Public License as published by the Free Software Foundation,      *
 * either version 3 of the License, or (at your option) any later version.                    *
 *                                                                                            *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;  *
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  *
 * See the GNU General Public License for more details.                                       *
 *                                                                                            *
 * You should have received a copy of the GNU General Public License along with this program. *
 * If not, see <http://www.gnu.org/licenses/>.                                                *
 **********************************************************************************************/

/**
 * @file column_gen.h
 * @brief A collection of functions for creating uncompressed columns and
 * initializing them with synthetically generated data.
 * @todo TODOS?
 */

#ifndef MORPHSTORE_CORE_STORAGE_COLUMN_GEN_H
#define MORPHSTORE_CORE_STORAGE_COLUMN_GEN_H

#include "column.h"
#include "../morphing/format.h"
#include "../utils/basic_types.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <random>
#include <stdexcept>
#include <vector>

namespace morphstore {
    
/**
 * Creates an uncompressed column and copies the contents of the given vector
 * into that column's data buffer. This is a convenience function for creating
 * small toy example columns. To prevent its usage for non-toy examples, it
 * throws an exception if the given vector contains more than 20 elements.
 * @param vec The vector to initialize the column with.
 * @return An uncompressed column containing a copy of the data in the given
 * vector.
 */
const column<uncompr_f> * make_column(const std::vector<uint64_t> & vec) {
    const size_t count = vec.size();
    if(count > 20)
        throw std::runtime_error(
                "make_column() is an inefficient convenience function and "
                "should only be used for very small columns"
        );
    const size_t size = count * sizeof(uint64_t);
    auto resCol = new column<uncompr_f>(size);
    memcpy(resCol->get_data(), vec.data(), size);
    resCol->set_meta_data(count, size);
    return resCol;
}

/**
 * Creates an uncompressed column and fills its data buffer with sorted unique
 * data elements. Can be used to generate primary key columns.
 * @param countValues The number of data elements to generate.
 * @param start The first data element.
 * @param step The difference between two consecutive data elements.
 * @return A column whose i-th data element is start + i * step .
 */
const column<uncompr_f> * generate_sorted_unique(
        size_t countValues,
        uint64_t start = 0,
        uint64_t step = 1
) {
    const size_t allocationSize = countValues * sizeof(uint64_t);
    auto resCol = new column<uncompr_f>(allocationSize);
    uint64_t * const res = resCol->get_data();
    
    for(unsigned i = 0; i < countValues; i++)
        res[i] = start + i * step;
    
    resCol->set_meta_data(countValues, allocationSize);
    
    return resCol;
}

/**
 * Creates an uncompressed column and fills its data buffer with values drawn
 * from the given random distribution. Suitable distributions can be found in
 * the <random> header. In particular, the following distributions are
 * supported:
 * - std::uniform_int_distribution
 * - std::binomial_distribution
 * - std::geometric_distribution
 * - std::negative_binomial_distribution
 * - std::poisson_distribution
 * - std::discrete_distribution
 * Optionally, the generated data can be sorted as an additional step.
 * @param countValues The number of data elements to generate.
 * @param distr The random distribution to draw the data elements from.
 * @param sorted Whether the generated data shall be sorted.
 * @return An uncompressed column containing the generated data elements.
 * @todo Support also the random distributions returning real values, e.g., 
 * std::normal_distribution .
 */
template<template<typename> class t_distr>
const column<uncompr_f> * generate_with_distr(
        size_t countValues,
        t_distr<uint64_t> distr,
        bool sorted
) {
    const size_t allocationSize = countValues * sizeof(uint64_t);
    auto resCol = new column<uncompr_f>(allocationSize);
    uint64_t * const res = resCol->get_data();
    
    std::default_random_engine generator(
            std::chrono::high_resolution_clock::now().time_since_epoch().count()
    );
    for(unsigned i = 0; i < countValues; i++)
        res[i] = distr(generator);
    
    resCol->set_meta_data(countValues, allocationSize);
    
    if(sorted)
        std::sort(res, res + countValues);
    
    return resCol;
}

}
#endif //MORPHSTORE_CORE_STORAGE_COLUMN_GEN_H