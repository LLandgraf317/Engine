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
 * @file generate_ldbc_graph.cpp
 * @brief Test for generating social network graph from LDBC files
 * @todo
 */

#include <core/storage/graph/adj_list/graph.h>
#include <core/storage/graph/ldbc_import.h>
#include <core/storage/graph/graph_abstract.h>
#include <chrono>  // for high_resolution_clock
#include <memory>

int main( void ){

    // ------------------------------------ LDBC-IMPORT TEST ------------------------------------
    auto start = std::chrono::high_resolution_clock::now(); // For measuring the execution time

    morphstore::LDBC_Import ldbcImport("/opt/ldbc_snb_datagen-0.2.8/social_network/");
    morphstore::AdjacencyList socialGraph;
    // create abstract pointer to adjc_list (ldbc importer just has to handle with one input class and not adjcancyList, CSR, ....)
    morphstore::Graph *graph;
    graph = &socialGraph;

    // generate vertices & edges from LDBC files and insert into socialGraph
    ldbcImport.import(*graph);

    // measuring time...
    auto finish = std::chrono::high_resolution_clock::now(); // For measuring the execution time
    std::chrono::duration<double> elapsed = finish - start;

    // delete graph-pointer to adj_list socialGraph
    graph = NULL;
    delete graph;

    socialGraph.statistics();
    std::cout << "Import & Graph-Generation Time: " << elapsed.count() << " sec.\n";

    /*
    // test vertices:
    socialGraph.print_vertex_by_id(100454);
    socialGraph.print_vertex_by_id(100450);
    socialGraph.print_vertex_by_id(100168);
    */

    // calculate size of social graph
    std::cout << "Size of socialGraph: " << socialGraph.get_size_of_graph() << " Bytes\n";

    return 0;
}