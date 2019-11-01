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
 * @file graph.h
 * @brief base graph class for any storage format --> CSR,ADJ
 * @todo
*/

#ifndef MORPHSTORE_GRAPH_H
#define MORPHSTORE_GRAPH_H

#include "vertex/vertex.h"
#include "edge/edge.h"

#include <map>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <functional>
#include <memory>
#include <fstream>
#include <sstream>


namespace morphstore{

    class Graph{

    protected:
        uint64_t numberVertices;
        uint64_t numberEdges;

        // Data-structure for Vertex-Properties
        std::unordered_map<uint64_t , std::shared_ptr<morphstore::Vertex>> vertices;

        // Lookup for entities and relations: number to string
        std::map<unsigned short int, std::string> entityDictionary;
        std::map<unsigned short int, std::string> relationDictionary;

    public:

        enum storageFormat {csr, adjacencylist };

        // -------------------- Setters & Getters --------------------

        const std::map<unsigned short, std::string> &getEntityDictionary() const {
            return entityDictionary;
        }

        void setEntityDictionary(const std::map<unsigned short, std::string>& ent) {
            this->entityDictionary = ent;
        }

        const std::map<unsigned short, std::string> &getRelationDictionary() const {
            return relationDictionary;
        }

        void setRelationDictionary(const std::map<unsigned short, std::string>& rel) {
            this->relationDictionary = rel;
        }

        uint64_t getNumberVertices() const {
            return numberVertices;
        }

        void setNumberVertices(uint64_t numV) {
            Graph::numberVertices = numV;
        }

        uint64_t getNumberEdges() const {
            return numberEdges;
        }

        void setNumberEdges(uint64_t numE) {
            Graph::numberEdges = numE;
        }

        std::string get_entity_by_number(unsigned short int e){
            if(entityDictionary.find( e ) != entityDictionary.end()){
                return entityDictionary.at(e);
            }else{
                return "No Matching of entity-number in the database!";
            }
        }

        std::string get_relation_by_number(unsigned short int re){
            if(relationDictionary.find( re ) != relationDictionary.end()){
                return relationDictionary.at(re);
            }else{
                return "No Matching of relation-number in the database!";
            }
        }

        // function to check if the vertex-ID is present or not (exists)
        bool exist_id(const uint64_t id){
            if(vertices.find(id) == vertices.end()){
                return false;
            }
            return true;
        }

        // function which returns a pointer to vertex by id
        std::shared_ptr<Vertex> get_vertex_by_id(uint64_t id){
            return vertices[id];
        }

	    // function to return a list of pair < vertex id, degree > DESC:
        std::vector<std::pair<uint64_t, uint64_t>> get_list_of_degree_DESC(){
            std::vector<std::pair<uint64_t, uint64_t>> vertexDegreeList;
            vertexDegreeList.reserve(numberVertices);
            // fill the vector with every vertex key and his degree
            for(uint64_t i = 0; i < numberVertices; ++i){
                vertexDegreeList.push_back({i, this->get_degree(i)});
            }
            // sort the vector on degree DESC
            std::sort(vertexDegreeList.begin(), vertexDegreeList.end(), [](const std::pair<uint64_t, uint64_t> &left, const std::pair<uint64_t ,uint64_t> &right) {
                return left.second > right.second;
            });

            return vertexDegreeList;
        }

        // function to measure graph characteristics (degree and count):
        void measure_degree_count(std::string filePath){
            std::vector<std::pair<uint64_t, uint64_t>> verticesDegree = get_list_of_degree_DESC();
            // unordered map for mapping degree to count:
	        std::unordered_map<uint64_t, uint64_t> results;

	        for(uint64_t i = 0; i < verticesDegree.size(); ++i){
		        // increment count in results for a given degree:
	    	    results[verticesDegree[i].second]++;
	        }

	        // write to file:
	        std::ofstream fs;
            std::stringstream ss;
            // open file for writing and delete existing stuff:
            fs.open(filePath, std::fstream::out | std::ofstream::trunc);

            for(auto const& m : results){
                ss << m.first << "," << m.second << "\n";
            }
            fs << ss.str() ;
            fs.close();
        }

        // -------------------- pure virtual functions --------------------

        virtual storageFormat getStorageFormat() const = 0;
        virtual void allocate_graph_structure(uint64_t numberVertices, uint64_t numberEdges) = 0;
        virtual void add_vertex() = 0;
        virtual uint64_t add_vertex_with_properties(const std::unordered_map<std::string, std::string> props ) = 0;
        virtual void add_property_to_vertex(uint64_t id, const std::pair<std::string, std::string> property) = 0;
        virtual void add_entity_to_vertex(const uint64_t id, const unsigned short int entity) = 0;
        virtual void add_edge(uint64_t from, uint64_t to, unsigned short int rel) = 0;
        virtual void add_edges(uint64_t sourceID, const std::vector<morphstore::Edge> relations) = 0;
        virtual uint64_t get_degree(uint64_t id) = 0;
        virtual std::vector<uint64_t> get_neighbors_ids(uint64_t id) = 0;
	    virtual std::pair<size_t, size_t> get_size_of_graph() = 0;

        // -------------------- debugging functions --------------------

        // for debugging
        virtual void print_neighbors_of_vertex(uint64_t id) = 0;

        void statistics(){
            std::cout << "---------------- Statistics ----------------" << std::endl;
            std::cout << "Number of vertices: " << getNumberVertices() << std::endl;
            std::cout << "Number of relations/edges: " << getNumberEdges() << std::endl;
            std::cout << "--------------------------------------------" << std::endl;
        }

        void print_vertex_by_id(uint64_t id) {
            std::cout << "-------------- Vertex ID: " << id << " --------------" << std::endl;
            std::shared_ptr<Vertex> v = vertices[id];
            std::cout << "Vertex-ID: \t" << v->getID() << std::endl;
            std::cout << "Entity: \t" << get_entity_by_number(v->getEntity()) << std::endl;
            std::cout << "\n";
            std::cout << "Properties: ";
            v->print_properties();
            std::cout << "#Edges: " << this->get_degree(v->getID());
            std::cout << "\n";
            std::cout << "-----------------------------------------------" << std::endl;
        }

        void print_entity_relationship_dicts(){
            std::cout << "Entity-Dict: " << std::endl;
            for(auto const& entry : entityDictionary){
                std::cout << entry.first << " -> " << entry.second << std::endl;
            }
            std::cout << "\n";

            std::cout << "Relationship-Dict: " << std::endl;
            for(auto const& rel : relationDictionary){
                std::cout << rel.first << " -> " << rel.second << std::endl;
            }
        }

    };

}


#endif //MORPHSTORE_GRAPH_H