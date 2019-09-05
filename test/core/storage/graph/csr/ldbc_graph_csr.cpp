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
 * @file ldbc_graph_csr.cpp
 * @brief Test for generating social network graph in CSR format
 * @todo
 */

#include <core/storage/graph/ldbc_import.h>
#include <core/storage/graph/formats/csr.h>
#include <core/operators/graph/bfs_naive.h>

#include <chrono>  // for high_resolution_clock

int main( void ){

    // ------------------------------------ LDBC-IMPORT TEST ------------------------------------
    /*
    std::cout << "\n";
    std::cout << "**********************************************************" << std::endl;
    std::cout << "* MorphStore-Storage-Test: Compressed Row Storage Format *" << std::endl;
    std::cout << "**********************************************************" << std::endl;
    std::cout << "\n";
     */

    // when using server with ssh pfeiffer@141.76.47.9: directory = "/home/pfeiffer/social_network/"
    // NEVER FORGET THE LAST / in address!!!
    // std::unique_ptr<morphstore::LDBCImport> ldbcImport = std::make_unique<morphstore::LDBCImport>(("/home/pfeiffer/social_network/"));
    std::unique_ptr<morphstore::LDBCImport> ldbcImport = std::make_unique<morphstore::LDBCImport>("/opt/ldbc_snb_datagen-0.2.8/social_network/social_network_1/");

    // Graph init:
    std::unique_ptr<morphstore::Graph> g1 = std::make_unique<morphstore::CSR>();

    // start measuring import time:
    auto startImportTime = std::chrono::high_resolution_clock::now(); // For measuring the execution time

    // generate vertices & edges from LDBC files and insert into graph
    ldbcImport->import(*g1);

    // measuring time:
    auto finishImportTime = std::chrono::high_resolution_clock::now(); // For measuring the execution time
    auto elapsedImportTime = std::chrono::duration_cast< std::chrono::milliseconds >( finishImportTime - startImportTime ).count();

    // size of graph in bytes:
    size_t size = g1->get_size_of_graph();
    std::cout << "Size: " << size << " bytes\n";

    //g1->statistics();
    std::cout << "Import: " << elapsedImportTime << " millisec.\n";

    /* Test Vertex, which contains edges with properties (SERVER):
     *
    g1->print_vertex_by_id(1035174);
    g1->print_neighbors_of_vertex(1035174);
    */

    /* Test Vertex, which contains edges with properties (MY PC):*/
    g1->print_vertex_by_id(100449);
    g1->print_neighbors_of_vertex(100449);


    /*
    std::unique_ptr<morphstore::BFS> bfs = std::make_unique<morphstore::BFS>(g1);
    bfs->do_measurements();
    */

    return 0;
}
