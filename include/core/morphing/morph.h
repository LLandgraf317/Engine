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
 * @file morph.h
 * @brief The template-based interface of the morph-operator.
 * @todo One generic column-level morph-operator for compression algorithms and
 * one such operator for decompression algorithms which handle the subdivision
 * of the data into a compressable main part and a non-compressible (too small)
 * remainder. Currently, this functionality is duplicated into the existing
 * morph-operator implementations. The current implementations should become
 * "internal" functions. --> This could be addressed using the new morph_batch
 * operator.
 * @todo Complete the documentation.
 */

#ifndef MORPHSTORE_CORE_MORPHING_MORPH_H
#define MORPHSTORE_CORE_MORPHING_MORPH_H

#include <core/storage/column.h>

namespace morphstore {

// ****************************************************************************
// Column-level
// ****************************************************************************

/**
 * @brief A struct wrapping the actual morph-operator.
 * 
 * This is necessary to enable partial template specialization, which is
 * required, since some compressed formats have their own template parameters.
 */
template<
        class t_vector_extension,
        class t_dst_f,
        class t_src_f
>
struct morph_t {
    /**
     * @brief Morph-operator. Changes the (compressed) format of the given
     * column from the source format `t_src_f` to the destination format
     * `t_dst_f` without logically changing the data.
     * 
     * This function is deleted by default, to guarantee that using this struct
     * with a format combination it is not specialized for causes a compiler
     * error, not a linker error.
     * 
     * @param inCol The data represented in the source format.
     * @return The same data represented in the destination format.
     */
    static
    const column<t_dst_f> *
    apply(const column<t_src_f> * inCol) = delete;
};

/**
 * @brief A template specialization of the morph-operator handling the case
 * when the source and the destination format are the same.
 * 
 * It merely returns the given column without doing any work.
 */
template<
        class t_vector_extension,
        class t_f
>
struct morph_t<t_vector_extension, t_f, t_f> {
    static
    const column<t_f> *
    apply(const column<t_f> * inCol) {
        return inCol;
    };
};

/**
 * A convenience function wrapping the morph-operator.
 * 
 * Changes the (compressed) format of the given column from the source format
 * `t_src_f` to the destination format `t_dst_f` without logically changing the
 * data. 
 * 
 * @param inCol The data represented in the source format.
 * @return The same data represented in the destination format.
 */
template<
        class t_vector_extension,
        class t_dst_f,
        class t_src_f
>
const column<t_dst_f> * morph(const column<t_src_f> * inCol) {
    return morph_t<t_vector_extension, t_dst_f, t_src_f>::apply(inCol);
}

// ****************************************************************************
// Batch-level
// ****************************************************************************

template<
        class t_vector_extension, class t_dst_f, class t_src_f
>
struct morph_batch_t {
    static void apply(
            // @todo Maybe we should use base_t instead of uint8_t. This could
            // save us some casting in several places.
            const uint8_t * & in8, uint8_t * & out8, size_t countLog
    ) = delete;
};

template<
        class t_vector_extension, class t_dst_f, class t_src_f
>
void morph_batch(const uint8_t * & in8, uint8_t * & out8, size_t countLog) {
    return morph_batch_t<t_vector_extension, t_dst_f, t_src_f>::apply(
            in8, out8, countLog
    );
}

}

#endif //MORPHSTORE_CORE_MORPHING_MORPH_H
