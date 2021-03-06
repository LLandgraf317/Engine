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
 * @file operator.cpp
 * @brief Brief description
 * @todo TODOS?
 */

#include <iostream>
#include <core/storage/storage_container_uncompressed.h>

int main( void ) {

    std::cout << "Test filter started" << std::endl;
    
    int count_results=0;
    storage_container_uncompressed<int> * myStore=new storastorage_container_uncompressed<int>();
    for (int i=0;i<myStore->count_values();i++){
        if (myStore->data[i]<100) count_results++;
    }
    
    std::cout << "Found " << count_results << " values" << std::endl;
    std::cout << "Test filter finished" << std::endl;
   return 0;
}
