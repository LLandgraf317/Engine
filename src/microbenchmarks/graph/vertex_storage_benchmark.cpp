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
 * @file vertex_storage_benchmark.cpp
 * @brief A little mirco benchmark of the vertex storage (hashmap<id, vertex> vs vector<vector<vertex>>).
 */

#include <core/storage/graph/formats/csr.h>
#include <core/storage/graph/vertex/vertices_container.h>
#include <chrono>
#include <random>
#include <algorithm>


typedef std::chrono::high_resolution_clock highResClock;
using namespace morphstore;

int64_t get_duration(std::chrono::time_point<std::chrono::system_clock> start) {
    auto stop = highResClock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
}

int64_t get_median(std::vector<int64_t> values) {
    assert(values.size() > 0);
    std::nth_element(values.begin(), values.begin() + values.size()/2, values.end());
    return values[values.size()/2];
}

int main(void) {
    // TODO: use core/utils/monitoring.h ? or a "time_it" function to stop a given function

    int number_of_executions = 5;

    std::cout << "Test vertex storage structure (median of 5 for full_iterate and random access)" << std::endl;
    std::cout << "Container type | vertex_count | loading time in μs | full_iterate in μs | random access 1/10 of the vertex count in μs" << std::endl;

    std::vector<VerticesContainerType> storage_types = {
        VerticesContainerType::HashMapContainer,
        VerticesContainerType::VectorArrayContainer
        };

    std::vector<int> vertex_counts = {10000, 100000, 1000000, 2000000, 5000000, 10000000, 15000000};

    for (int vertex_count: vertex_counts) {
      std::random_device rd;
      std::uniform_int_distribution<uint64_t> dist(0, vertex_count - 1);
      std::vector<int> random_accesses;
      for (int i = 0; i < vertex_count; i++) {
        random_accesses.push_back(dist(rd));
      }

      for (auto storage_type : storage_types) {
        std::unique_ptr<CSR> graph = std::make_unique<CSR>(storage_type);
        graph->allocate_graph_structure(vertex_count, 0);

        std::string measurement_entry =
            graph->vertices_container_description() + " | ";
        measurement_entry += std::to_string(vertex_count) + " | ";

        auto start = highResClock::now();
        for (int i = 0; i < vertex_count; i++) {
          graph->add_vertex(i);
        }

        measurement_entry += std::to_string(get_duration(start)) + " | ";

        std::vector<int64_t> durations;

        for (int exec = 0; exec < number_of_executions; exec++) {
          auto start = highResClock::now();
          // iterate
          for (int i = 0; i < vertex_count; i++) {
            graph->get_vertex(i);
          }
          durations.push_back(get_duration(start));
        }

        measurement_entry += std::to_string(get_median(durations)) + " | ";

        // random access

        durations.clear();

        for (int exec = 0; exec < number_of_executions; exec++) {
          auto start = highResClock::now();

          for (int random_pos : random_accesses) {
            graph->get_vertex(random_pos);
          }

          durations.push_back(get_duration(start));
        }

        measurement_entry += std::to_string(get_median(durations));

        std::cout << measurement_entry << std::endl;
      }
    }

    return 0;
}