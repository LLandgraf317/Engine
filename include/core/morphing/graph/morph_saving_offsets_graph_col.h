/**********************************************************************************************
 * Copyright (C) 2020 by MorphStore-Team                                                      *
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
 * @file morph_saving_offsets_graph_col.h
 * @brief helper for `morph_saving_offsets()` graph column (template-free column). Basically need to cast to template
 * column as it cannot be derieved
 * @todo Remove this helper and make graph formats accept templates (can use normal `morph_saving_offsets()` then)
 */

#ifndef MORPHSTORE_GRAPH_MORPH_SAVING_OFFSETS_GRAPH_COL_H
#define MORPHSTORE_GRAPH_MORPH_SAVING_OFFSETS_GRAPH_COL_H

#include <core/storage/column.h>
#include <core/storage/column_with_blockoffsets.h>
#include <core/storage/graph/graph_compr_format.h>

#include <core/morphing/morph_saving_offsets.h>
// for simple decompression (very likely removeable)
#include <core/morphing/graph/morph_graph_col.h>

#include <memory>

namespace morphstore {

    // casting the column to the actual column type before morphing (as compiler could not derive it)
    // delete_old_col -> delete input column after morphing (if the result is not the input column)
    column_with_blockoffsets_base *morph_saving_offsets_graph_col(column_with_blockoffsets_base *col_with_offsets,
                                                                  const GraphCompressionFormat src_f,
                                                                  const GraphCompressionFormat trg_f,
                                                                  bool delete_in_col = false) {
        if (src_f == trg_f) {
            return col_with_offsets;
        }

        column_with_blockoffsets_base *result = col_with_offsets;

        auto col = col_with_offsets->get_column();

        switch (src_f) {
        case GraphCompressionFormat::UNCOMPRESSED: {
            const column_uncompr *old_col = dynamic_cast<const column_uncompr *>(col);
            switch (trg_f) {
            case GraphCompressionFormat::DELTA:
                result = morph_saving_offsets<ve, default_delta, uncompr_f>(old_col);
                break;
            case GraphCompressionFormat::FOR:
                result = morph_saving_offsets<ve, default_for, uncompr_f>(old_col);
                break;
            case GraphCompressionFormat::DYNAMIC_VBP:
                result = morph_saving_offsets<ve, default_vbp, uncompr_f>(old_col);
                break;
            case GraphCompressionFormat::UNCOMPRESSED:
                // handled by src_f == trg_f
                break;
            }
            break;
        }
        case GraphCompressionFormat::DELTA: {
            if (trg_f == GraphCompressionFormat::UNCOMPRESSED) {
                const column_delta *old_col = dynamic_cast<const column_delta *>(col);
                result = morph_saving_offsets<ve, uncompr_f, default_delta>(old_col);
            } else {
                // as direct morphing is not yet supported .. go via decompressing first
                auto uncompr_col = morph_saving_offsets_graph_col(col_with_offsets, src_f,
                                                                  GraphCompressionFormat::UNCOMPRESSED, false);
                result =
                    morph_saving_offsets_graph_col(uncompr_col, GraphCompressionFormat::UNCOMPRESSED, trg_f, true);
            }
            break;
        }
        case GraphCompressionFormat::FOR: {
            if (trg_f == GraphCompressionFormat::UNCOMPRESSED) {
                const column_for *old_col = dynamic_cast<const column_for *>(col);
                result = morph_saving_offsets<ve, uncompr_f, default_for>(old_col);
            } else {
                // as direct morphing is not yet supported .. go via decompressing first
                auto uncompr_col = morph_saving_offsets_graph_col(col_with_offsets, src_f,
                                                                  GraphCompressionFormat::UNCOMPRESSED, false);
                result =
                    morph_saving_offsets_graph_col(uncompr_col, GraphCompressionFormat::UNCOMPRESSED, trg_f, true);
            }
            break;
        }
        case GraphCompressionFormat::DYNAMIC_VBP: {
            if (trg_f == GraphCompressionFormat::UNCOMPRESSED) {
                const column_dyn_vbp *old_col = dynamic_cast<const column_dyn_vbp *>(col);
                result = morph_saving_offsets<ve, uncompr_f, default_vbp>(old_col);
            } else {
                // as direct morphing is not yet supported .. go via decompressing first
                auto uncompr_col = morph_saving_offsets_graph_col(col_with_offsets, src_f,
                                                                  GraphCompressionFormat::UNCOMPRESSED, false);
                // delete_in_col = true as temporary uncompr_col should always be deleted
                result =
                    morph_saving_offsets_graph_col(uncompr_col, GraphCompressionFormat::UNCOMPRESSED, trg_f, true);
            }
            break;
        }
        }

        // free input column if possible
        if (result != col_with_offsets && delete_in_col) {
            delete col_with_offsets;
        }

        if (result == nullptr) {
            throw std::runtime_error("Did not handle src: " + graph_compr_f_to_string(src_f) +
                                     " trg: " + graph_compr_f_to_string(trg_f));
        }

        return result;
    }

/*     const column_uncompr *decompress_part_of_graph_col(const column_base *col, const GraphCompressionFormat src_f) {
        // TODO
        throw std::runtime_error("Not implemented decompressing a single block");
    } */

    // TODO: also consider size of blockoffset vector?
    double compression_ratio(column_with_blockoffsets_base *col_with_offsets, GraphCompressionFormat col_format) {
        auto col = col_with_offsets->get_column();
        auto uncompr_col = decompress_graph_col(col, col_format);
        auto ratio = uncompr_col->get_size_used_byte() / (double)col->get_size_used_byte();

        if (col != uncompr_col) {
            delete uncompr_col;
        }

        return ratio;
    }
} // namespace morphstore

#endif // MORPHSTORE_GRAPH_MORPH_SAVING_OFFSETS_GRAPH_COL_H