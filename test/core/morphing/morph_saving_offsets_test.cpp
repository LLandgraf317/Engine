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
 * @file morph_saving_offsets_test.cpp
 * @brief Tests morph_saving_offsets.
 */

#include <core/memory/mm_glob.h>
#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>
#include <core/storage/column.h>
#include <core/storage/column_with_blockoffsets.h>
#include <core/storage/column_gen.h>
#include <core/utils/basic_types.h>
#include <core/morphing/morph_saving_offsets.h>
#include <core/morphing/morph_batch.h>
#include <core/morphing/default_formats.h>
#include <core/morphing/delta.h>
#include <core/morphing/dynamic_vbp.h>
#include <core/utils/printing.h>
#include <core/utils/equality_check.h>

#include <assert.h>

using namespace morphstore;
using namespace vectorlib;

using ve = scalar<v64<uint64_t>>; 
using compr_f = DEFAULT_DELTA_DYNAMIC_VBP_F(ve);

int main(void) {
    // 3 whole blocks 
    // TODO: also check for partial block 
    // for last block only morph_batch if it is complete ... (as incomplete block are still uncompressed)
    auto origCol = generate_sorted_unique(3072);

    // !! morph saving offsets needs to look if last block can be actually morphed (if not complet -> undefined behaviour?)

    auto col_with_offsets = morph_saving_offsets<ve, compr_f, uncompr_f>(origCol);

    assert(col_with_offsets->get_block_offsets()->size() == 3);

    auto decompr_col = morph_saving_offsets<ve, uncompr_f, compr_f>(col_with_offsets->get_column())->get_column();

    // asserting correctness of operator
    equality_check ec0(decompr_col, origCol);
    std::cout << ec0;

    assert(ec0.m_CountValuesEqual);
    assert(ec0.m_SizeUsedByteEqual);

    uint64_t* expected =  origCol->get_data();
    uint64_t* actual =  decompr_col->get_data();

    // for finding point of error
    for (uint64_t i = 0; i < origCol->get_count_values(); i++) {
        bool equals = expected[i] == actual[i];
        if (!equals) {
            std::cout << "actual: " << actual[i] << " expected: " << expected[i] << std::endl;
            std::cout.flush();
        }
    }
    
    // findings: block 0 correctly decompressed, block 1 is off by 1023, block 2 is again correctly decompressed
/*     print_columns(
          print_buffer_base::decimal,
          decompr_col,                              
          "whole decompressed column");  */

    
    //assert(ec0.good());
 

    // asserting correctness of decompressing a single block
    auto block_size = col_with_offsets->get_block_size();
    auto alloc_size = block_size * sizeof(uint64_t);
    for (uint64_t block = 0; block < col_with_offsets->get_block_offsets()->size(); block++) {
        std::cout << "Checking block " << block << "range: " << block * block_size << " .. " << ((block +1) * block_size) - 1  << std::endl;
        auto expected_col = generate_sorted_unique(1024, block * 1024);

        const uint8_t *block_offset = col_with_offsets->get_block_offset(block);
        auto decompr_col_block = new column<uncompr_f>(alloc_size);
        decompr_col_block->set_meta_data(block_size, alloc_size);
        uint8_t *out8 = decompr_col_block->get_data();

        // decompress a single block of a column
        morph_batch<ve, uncompr_f, compr_f>(block_offset, out8, block_size);

        equality_check ec_block(expected_col, decompr_col_block);

        std::cout << ec_block;

        if (!ec_block.good()) {
            print_columns(print_buffer_base::decimal, decompr_col_block, expected_col, "actual", "expected");
            assert(ec_block.good());
        }
    }

    return 0;
}