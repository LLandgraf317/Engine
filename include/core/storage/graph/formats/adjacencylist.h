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
 * @file adjacencylist.h
 * @brief Derived adj. list storage format class. Base: graph.h
 * @todo Adjust get_size_of_graph(), ?replace unordered_map with a fixed sized array
*/

#ifndef MORPHSTORE_ADJACENCYLIST_H
#define MORPHSTORE_ADJACENCYLIST_H

#include <core/storage/graph/graph.h>
#include <core/storage/column_gen.h>
#include <core/storage/graph/graph_compr_format.h>

#include <iterator>
#include <assert.h>
#include <variant>
#include <type_traits>

namespace morphstore{

    class AdjacencyList: public Graph {

    private:
        using adjacency_column = column_base*; 
        using adjacency_vector = std::vector<uint64_t>*;
        using adjacency_list_variant = std::variant<adjacency_vector, adjacency_column>;

        struct Adjacency_List_Size_Visitor {
            size_t operator()(const adjacency_column c) const {
                return c->get_size_used_byte();
            }
            size_t operator()(const adjacency_vector v) const {
               return v->size();
            }
        };

        struct Adjacency_List_Finalizer {
            adjacency_column operator()(const adjacency_column c) const {
                return c;
            }
            adjacency_column operator()(const adjacency_vector v) const {
                // const_cast to be able to assign colum to entry value
               return const_cast<column_uncompr*>(make_column(v->data(), v->size(), true));
            }
        };

        // maps the outgoing edges (ids) per vertex
        std::unordered_map<uint64_t, adjacency_list_variant> adjacencylistPerVertex;
        
        // indicating whether we have columns or vectors (columns after first compress() call)
        // TODO: is this replace-able by just checking the type of the first element in the map? (via holds_alternative)
        bool finalized = false;

        // convert every adjVector to a adjColumn
        void finalize() {
            if (!finalized) {
                std::unordered_map<uint64_t, adjacency_list_variant> adjacencyColumnPerVertex;
                adjacencyColumnPerVertex.reserve(adjacencylistPerVertex.size());

                for(auto entry: adjacencylistPerVertex) {
                    adjacencyColumnPerVertex.insert({entry.first, std::visit(Adjacency_List_Finalizer{}, entry.second)});
                }

                this->adjacencylistPerVertex = adjacencyColumnPerVertex;
                this->finalized = true;
            }
        }
    public:
        ~AdjacencyList() {
                for(auto entry: this->adjacencylistPerVertex) {
                    if (finalized) {
                        free(std::get<adjacency_column>(entry.second));
                    }
                    else {
                        free(std::get<adjacency_vector>(entry.second));
                    }
            }
        }

        AdjacencyList(EdgesContainerType edges_container_type)
            : Graph(VerticesContainerType::VectorArrayContainer, edges_container_type) {}

        AdjacencyList(VerticesContainerType vertices_container_type = VerticesContainerType::VectorArrayContainer) : Graph(vertices_container_type) {}
        
        std::string get_storage_format() const override {
            return "Adjacency_List";
        }

        // function: to set graph allocations
        void allocate_graph_structure(uint64_t numberVertices, uint64_t numberEdges) override {
            Graph::allocate_graph_structure(numberVertices, numberEdges);
            adjacencylistPerVertex.reserve(numberVertices);
        }

        // adding a single edge to vertex:
        void add_edge(uint64_t sourceId, uint64_t targetId, unsigned short int type) override {
            Edge e = Edge(sourceId, targetId, type);
            add_edges(sourceId, {e});
        }

        // function that adds multiple edges (list of neighbors) at once to vertex
        void add_edges(uint64_t sourceId, const std::vector<morphstore::Edge> edgesToAdd) override {
            if (finalized) {
                throw std::runtime_error("Cannot add edges, if adj. lists are compressed");
            }

            if (!vertices->exists_vertex(sourceId)) {
                throw std::runtime_error("Source-id not found " + std::to_string(sourceId));
            }

            // avoid inserting an empty adjacencyList (waste of memory)
            if (edgesToAdd.size() == 0) {
                return ;
            }

            std::vector<uint64_t> *adjacencyList;
            if (adjacencylistPerVertex.find(sourceId) != adjacencylistPerVertex.end()) {
                adjacencyList = std::get<adjacency_vector>(adjacencylistPerVertex[sourceId]);
            } else {
                adjacencyList = new std::vector<uint64_t>();
                adjacencylistPerVertex.insert({sourceId, adjacencyList});
            }

            adjacencyList->reserve(edgesToAdd.size());

            for (const auto edge : edgesToAdd) {
                if (!vertices->exists_vertex(edge.getTargetId())) {
                    throw std::runtime_error("Target not found  :" + edge.to_string());
                }
                edges->add_edge(edge);
                adjacencyList->push_back(edge.getId());
            }
        }


        // get number of neighbors of vertex with id
        uint64_t get_out_degree(uint64_t id) override {
            auto entry = adjacencylistPerVertex.find(id);
            if (entry == adjacencylistPerVertex.end()) {
                return 0;
            }
            else { 
                uint64_t out_degree;
                if (finalized) {
                    out_degree = std::get<adjacency_column>(entry->second)->get_count_values();
                }
                else {
                    out_degree = std::get<adjacency_vector>(entry->second)->size();
                }
                return out_degree;
            }
        }

        std::vector<uint64_t> get_outgoing_edge_ids(uint64_t id) {
            std::vector<uint64_t> edge_ids;
            auto entry = adjacencylistPerVertex.find(id);
            if (entry != adjacencylistPerVertex.end()) {
                if (this->finalized) {
                    adjacency_column col = std::get<adjacency_column>(entry->second);
                    const size_t column_size = col->get_count_values();
                    // TODO: init vector via range-constructor / mem-cpy
                    //const uint8_t * end_addr = start_addr + sizeof(uint64_t) * out_degree;
                    const uint64_t * start_addr = col->get_data();
                    edge_ids.insert(edge_ids.end(), start_addr, start_addr+column_size);
                } else {
                    edge_ids = *std::get<adjacency_vector>(entry->second);
                }
            }
            return edge_ids;
        }

        // get the neighbors-ids into vector for BFS alg.
        // todo: this is actually format generic and can be pulled to graph.h
        std::vector<uint64_t> get_neighbors_ids(uint64_t id) override {
            std::vector<uint64_t> targetVertexIds;

            for (uint64_t const edgeId : get_outgoing_edge_ids(id)) {
                assert(edges->exists_edge(edgeId));
                targetVertexIds.push_back(edges->get_edge(edgeId).getTargetId());
            }

            return targetVertexIds;
        }

        void compress(GraphCompressionFormat target_format) override {
            std::cout << "Compressing graph format specific data structures using: " << to_string(target_format) << std::endl;
            
            if (!finalized) {
                std::cout << "Transforming vectors into columns" << std::endl;
                this->finalize();
            }
            
            // TODO: convert column to target_format

            //this->current_compression = target_format;
        }

        // for measuring the size in bytes:
        std::pair<size_t, size_t> get_size_of_graph() const override {
            auto [index_size, data_size] = Graph::get_size_of_graph();

            // adjacencyListPerVertex
            index_size += sizeof(std::unordered_map<uint64_t, adjacency_list_variant>);
            index_size += adjacencylistPerVertex.size() * (sizeof(uint64_t) + sizeof(adjacency_list_variant));

            for(const auto iterator : adjacencylistPerVertex){
                data_size += std::visit(Adjacency_List_Size_Visitor{}, iterator.second);
            }

            return {index_size, data_size};
        }

        // for debugging: print neighbors a vertex
        void print_neighbors_of_vertex(uint64_t id) override{
            std::cout << std::endl << "Neighbours for Vertex with id " << id << std::endl;
            auto edge_ids = get_outgoing_edge_ids(id);
            
            if(edge_ids.size() == 0) {
                std::cout << "  No outgoing edges for vertex with id: " << id << std::endl;
            }
            else {
                for (const auto edge_id : edge_ids) {
                    print_edge_by_id(edge_id);
                }
            }
        }

        void statistics() override {
            Graph::statistics();
            std::cout << "Number of adjacency lists:" << adjacencylistPerVertex.size() << std::endl;
            std::string isFinal = (finalized) ? "true" : "false";
            std::cout << "AdjacencyLists finalized:" << isFinal << std::endl;
            std::cout << std::endl << std::endl;
        }

    };
}

#endif //MORPHSTORE_ADJACENCYLIST_H
