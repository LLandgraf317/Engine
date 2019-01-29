/**********************************************************************************************
 * Copyright (C) 2019 by Patrick Damme                                                        *
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
 * @file binary_io_test.cpp
 * @brief A short test and example usage of persistence::binary_io .
 * @author Patrick Damme
 * @todo TODOS?
 */

#include "../../../include/core/memory/mm_glob.h"
#include "../../../include/core/persistence/binary_io.h"
#include "../../../include/core/storage/column.h"

#include <cstddef>
#include <cstdint>
#include <cstring>

const char * okStr( bool isOk ) {
    return isOk ? "ok" : "not ok";
}

int main( void ) {
    using namespace std;
    using namespace morphstore::storage;
    using namespace morphstore::persistence;
    
    // Parameters.
    const size_t origCountValues = 100 * 1000;
    const size_t origSizeUsedByte = origCountValues * sizeof( uint64_t );
    const std::string fileName = "binary_io_test__testcol123";
    
    // Create the column.
    column * origCol = new column( origSizeUsedByte );
    uint64_t * origData = reinterpret_cast< uint64_t * >( origCol->data( ) );
    for( unsigned i = 0; i < origCountValues; i++ )
        origData[ i ] = i;
    origCol->count_values( origCountValues );
    origCol->size_used_byte( origSizeUsedByte );
    
    // Store the column.
    // TODO maybe we should delete the file afterwards
    binary_io::store( origCol, fileName );
    
    // Reload the column and compare it to the original one.
    const column * reloCol = binary_io::load( fileName );
    const size_t reloCountValues = reloCol->count_values( );
    const size_t reloSizeUsedByte = reloCol->size_used_byte( );
    
    // Compare the original column to the reloaded column.
    const bool okCountValues  = origCountValues  == reloCountValues;
    const bool okSizeUsedByte = origSizeUsedByte == reloSizeUsedByte;
    const bool okData = !memcmp( origData, reloCol->data( ), min( origSizeUsedByte, reloSizeUsedByte ) );
    
    // Print the result.
    cout << "countValues: "  << okStr( okCountValues )  << " (expected " << origCountValues  << ", found " << reloCountValues  << ')' << endl;
    cout << "sizeUsedByte: " << okStr( okSizeUsedByte ) << " (expected " << origSizeUsedByte << ", found " << reloSizeUsedByte << ')' << endl;
    cout << "data: " << okStr( okData ) << " (this check is only valid, if countValues and sizeUsedByte are ok)" << endl;
    
    return 0;
}