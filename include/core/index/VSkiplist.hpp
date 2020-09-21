/*
 * Copyright (C) 2017-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of our NVM-based Data Structures repository.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <core/memory/management/abstract_mm.h>
#include <core/memory/management/general_mm.h>

#include <cstdlib>
#include <iostream>

#include <core/tracing/trace.h>

#include "utils/Random.hpp"

namespace morphstore {


/* NOTE: Copyright for this is still GPL 3 as the code is mainly copied from TU Ilmenaus PMEM DS project */
/**
 * A volatile memory implementation of a skip list with a single record per node.
 *
 * @tparam KeyType the data type of the key
 * @tparam ValueType the data type of the values associated with the key
 * @tparam MAX_LEVEL the maximum number of levels
 */
template<typename KeyType, typename ValueType, int MAX_LEVEL>
class VSkiplist {

  static constexpr auto MAX_KEY = std::numeric_limits<KeyType>::max();
  static constexpr auto MIN_KEY = std::numeric_limits<KeyType>::min();

#ifndef UNIT_TESTS
    private:
#else
    public:
#endif

  /**
   * A structure for representing a node of a skip list.
   */
  struct alignas(64) SkipNode {
    KeyType key;
    ValueType value;
    size_t nodeLevel;
    std::array<SkipNode *, MAX_LEVEL> forward;

    /**
     * Constructor for creating a new empty node.
     */
    SkipNode() : nodeLevel(0) {}

    /**
     * Constructor for creating a new empty node on a given level.
     */
    SkipNode(size_t level) : nodeLevel(level) {}

    /**
     * Constructor for creating a new node with an initial key and value.
     */
    explicit SkipNode(const KeyType &key, const ValueType &value, size_t level) :
      key(key), value(value), nodeLevel(level) {}

    /**
     * Print details this node to standard output.
     */
    void printNode() {
      std::cout << "{" << key << "," << value << "} --> ";
    }

  }; /// end struct SkipNode

  /* -------------------------------------------------------------------------------------------- */

  size_t m_NumaNode;
  size_t level;     ///< the current number of levels (height)
  size_t nodeCount; ///< the current number of nodes
  dbis::Random* rnd;   ///< volatile pointer to a random number generator
  SkipNode * head; ///< pointer to the entry node of the skip list

  /**
   * Create a new node within the skip list.
   *
   * @param node[out] the pointer/reference to the newly allocated node
   */
  void newSkipNode(SkipNode * &node) {
      auto newSkipNode = reinterpret_cast<SkipNode*>( general_memory_manager::get_instance().allocateNuma(sizeof(SkipNode), m_NumaNode));
      node = new (newSkipNode) SkipNode(level);
  }

  /**
   * Create a new node within the skip list.
   *
   * @param level the level of the node within the skip list
   * @param key the initial key
   * @param value the initial value
   * @param node[out] the pointer/reference to the newly allocated node
   */
  void newSkipNode(const KeyType &key, const ValueType &value, size_t level, SkipNode * &node) {
      auto newSkipNode = reinterpret_cast<SkipNode*>( general_memory_manager::get_instance().allocateNuma(sizeof(SkipNode), m_NumaNode));
      node = new (newSkipNode) SkipNode(key, value, level);
  }

  /**
   * Initialize a skip list with a new head and initial empty node.
   */
  void initList() {
    newSkipNode(MIN_KEY, ValueType{}, MAX_LEVEL, head);
    auto &headForward = head->forward;
    for (auto i = 0u; i < MAX_LEVEL; ++i) {
      headForward[i] = nullptr;
    }
  }

  size_t getRandomLevel() const {
    const auto rlevel = rnd->Uniform(MAX_LEVEL);
    return (rlevel ? rlevel : 1);
  }

  /* -------------------------------------------------------------------------------------------- */

  public:

  /**
   * Constructor for creating a new skip list.
   */
  VSkiplist(size_t numaNode) : m_NumaNode(numaNode), level(0), nodeCount(0), rnd(new dbis::Random(std::time(nullptr))) {
    initList();
  }

  /**
   * Destructor for the skip list; should only free volatile parts.
   */
  ~VSkiplist() {
    delete rnd;
  }

  /**
   * Inserts a new key-value pair into the skip list.
   *
   * @param key the key to be inserted
   * @param value the value to be inserted
   * @return true if insert was successful, false if a value was updated
   */
  bool insert(const KeyType &key, const ValueType &value) {
    /// find target node @c node and prepare forward pointers for each level in DRAM
    std::array<SkipNode*, MAX_LEVEL> update{};
    auto node = head;
    for(auto i = level;; --i) {
      auto next = node->forward[i];
      while(next != nullptr && next->key < key) {
        node = next;
        next = node->forward[i];
      }
      update[i] = node;
      if (i == 0) break;
    }

    /// get sibling node of last level, if not tail
    const auto next = node->forward[0];
    if (next) node = next;

    /// handle duplicates
    if(node->key == key) {
      node->value = value;
      return false;
    }

    /// insert key and value into a new node with random level
    const auto newLevel = getRandomLevel();
    if(newLevel > level) {
      for (auto i = level + 1; i < newLevel; ++i)
        update[i] = head;
      level = newLevel;
    }
    SkipNode * newNode;
    newSkipNode(key, value, newLevel, newNode);
    auto newNodeV = newNode;
    for(auto i = 0u; i < newLevel; ++i) {
      newNodeV->forward[i] = update[i]->forward[i];
      update[i]->forward[i] = newNode;
    }
    ++nodeCount;

    return true;
  }

  /**
   * Search for the value for a given key.
   *
   * @param key the key to be searched for
   * @param[out] value a copy of the value
   * @return true if found, else otherwise
   */
  bool search(const KeyType &key, ValueType &val) const {
    auto node = head;

    for (auto i = level;; --i) {
      auto next = node->forward[i];
      while (next && key > next->key) {
        node = next;
        next = next->forward[i];
      }

      if (i == 0)
          break;

      if (key == 0 && node) {
          val = node->value;
          return true;
      }
    }
    node = node->forward[0];
    if(node && node->key == key) {
      val = node->value;
      return true;
    }

    return false;
  }

  using ScanFunc = std::function<void(const KeyType &key, const ValueType &val)>;
  void scan(ScanFunc func) {
      auto node = head;
      auto next = node->forward[0];
      //trace_l(T_DEBUG, "Scanning for ", node->key, " and ", node->value);
      func(node->key, node->value);

      while (next) {
          node = next;
          next = next->forward[0];

          //trace_l(T_DEBUG, "Scanning for ", node->key, " and ", node->value);
          func(node->key, node->value);
      }
  }

  inline void scanValue(const uint64_t &minKey, const uint64_t &maxKey, std::list<ValueType> &outList) const {
    auto node = head;
    for (auto i = level;; --i) {
      auto next = node->forward[i];
      while (next && minKey > next->key) {
        node = next;
        next = next->forward[i];
      }
      if (i == 0)
          break;
    }
    if (!node->forward[0])
        return;
    node = node->forward[0];
    auto next = node->forward[0];

    /*if(node && node->key >= minKey) {
      outList.push_back(node->value);
      node = next;
      next = next->forward[0];
    }*/
    while (next && maxKey+1 > node->key) {
      outList.push_back(node->value);
      node = next;
      next = next->forward[0];
    }
  }

  /**
   * Recover volatile parts of the skip list.
   */
  void recover() {
    rnd = new dbis::Random(std::time(nullptr));
  }

  /**
   * Print all nodes to standard output.
   */
  void printList() const {
    std::cout << "\nLevels: " << level + 1 << ", Nodes: " << nodeCount << "\n";
    std::cout << "________\n";
    for (int i = level; i >= 0; --i) {
      auto tmp = head->forward[i];
      std::cout << "|HEAD|" << i << "| --> ";
      while (tmp) {
        tmp->printNode();
        tmp = tmp->forward[i];
      }
      std::cout << "\u001b[31mTAIL\u001b[0m\n";
    }
    std::cout << "‾‾‾‾‾‾‾‾" << std::endl;
  }

}; /// end class simplePSkiplist

} /// namespace morphstore

