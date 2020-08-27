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
 * @file select_uncompr.h
 * @brief Template specialization of the select-operator for uncompressed
 * inputs and outputs using the scalar processing style. Note that these are
 * simple reference implementations not tailored for efficiency.
 * @todo TODOS?
 */

#ifndef MORPHSTORE_CORE_OPERATORS_SCALAR_SELECT_UNCOMPR_H
#define MORPHSTORE_CORE_OPERATORS_SCALAR_SELECT_UNCOMPR_H

#include <core/operators/interfaces/select.h>
#include <core/morphing/format.h>
#include <core/storage/column.h>
#include <core/utils/basic_types.h>
#include <core/index/MultiValTreeIndex.hpp>
#include <core/index/NodeBucketList.h>
#include <vector/scalar/extension_scalar.h>

#include <cstdint>

namespace morphstore {
    
template<template<typename> class t_op>
struct select_t<t_op, vectorlib::scalar<vectorlib::v64<uint64_t>>, uncompr_f, uncompr_f> {
    static
    const column<uncompr_f> * apply(
            const column<uncompr_f> * const inDataCol,
            const uint64_t val,
            const size_t outPosCountEstimate = 0
    ) {
        const size_t inDataCount = inDataCol->get_count_values();
        const uint64_t * const inData = inDataCol->get_data();

        // If no estimate is provided: Pessimistic allocation size (for
        // uncompressed data), reached only if all input data elements pass the
        // selection.
        auto outPosCol = new column<uncompr_f>(
                bool(outPosCountEstimate)
                // use given estimate
                ? (outPosCountEstimate * sizeof(uint64_t))
                // use pessimistic estimate
                : inDataCol->get_size_used_byte()
        );
        
        t_op<uint64_t> op;
        uint64_t * outPos = outPosCol->get_data();
        const uint64_t * const initOutPos = outPos;


        for(unsigned i = 0; i < inDataCount; i++)
            if(op(inData[i], val)) {
                *outPos = i;
                outPos++;
            }

        const size_t outPosCount = outPos - initOutPos;
        outPosCol->set_meta_data(outPosCount, outPosCount * sizeof(uint64_t));

        return outPosCol;
    }
};

template<template<typename> class t_op,
        class t_out_pos_f,
        class t_in_data_f,
        class index_structure
>
struct index_select_wit_t {
    static
    const column<t_out_pos_f> * apply(
            pptr<index_structure> inDataIndex,
            const uint64_t key
    ) {
        std::list<
            pptr<NodeBucketList<uint64_t>>
            > bucket_lists_list;
        size_t sum_count_values = 0;
        t_op<uint64_t> op;

        //This might be slow, check back with execution times
        auto materialize_lambda = [&](const uint64_t& given_key, const pptr<NodeBucketList<uint64_t>> val)
        {
            trace_l(T_INFO, "called operation on ", given_key);
            if (op(given_key, key)) {
                bucket_lists_list.push_back(val);
                trace_l(T_INFO, "Adding values: ", val->getCountValues());
                sum_count_values += val->getCountValues();
            }
        };
        inDataIndex->scan(materialize_lambda);

        column<uncompr_f> * valueCol = new column<uncompr_f>(sizeof(uint64_t) * sum_count_values);
        uint64_t* value_data = valueCol->get_data();

        for (auto bucketListIter = bucket_lists_list.begin(); bucketListIter != bucket_lists_list.end(); bucketListIter++) {
            for (auto iter = (*bucketListIter)->first; iter != nullptr; iter = iter->getNext()) {
                for (size_t i = 0; i < iter->fill_count; i++) {
                    *value_data = iter->getBucketEntry(i);
                    value_data++;
                }
            }
        }

        valueCol->set_meta_data(sum_count_values, sum_count_values * sizeof(uint64_t));

        return valueCol;
    }
};

}
#endif //MORPHSTORE_CORE_OPERATORS_SCALAR_SELECT_UNCOMPR_H
