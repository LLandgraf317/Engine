#pragma once

#include <nvmdatastructures/src/pbptrees/PBPTree.hpp>

#include <core/index/NodeBucketList.h>

#include <list>

using namespace dbis::pbptrees;

//TODO: Figure out way to abstract this
// Parametrization of PBPTree
using CustomKey = uint64_t;
using CustomTuple = std::tuple<uint64_t>;

using MultiVal = std::list< uint64_t >;

constexpr auto TARGET_BRANCH_SIZE = 512;
constexpr auto TARGET_LEAF_SIZE = 512;

constexpr size_t getBranchKeysPBPTree() {
  return ((TARGET_BRANCH_SIZE - 28) / (sizeof(CustomKey) + 24));
}

constexpr size_t getLeafKeysPBPTree() {
  return ((TARGET_LEAF_SIZE - 64) / (sizeof(CustomKey) + sizeof(CustomTuple)));
}

constexpr uint64_t ipow(uint64_t base, int exp, uint64_t result = 1) {
  return exp < 1 ? result : ipow(base * base, exp / 2, (exp % 2) ? result * base : result);
}

constexpr auto TARGET_DEPTH = 10;
constexpr auto LEAFKEYS = getLeafKeysPBPTree();
constexpr auto BRANCHKEYS = getBranchKeysPBPTree() & ~1;  ///< make this one even
constexpr auto ELEMENTS = LEAFKEYS * ipow(BRANCHKEYS + 1, TARGET_DEPTH);

using TreeType = PBPTree<CustomKey, CustomTuple, BRANCHKEYS, LEAFKEYS>;

template<unsigned bucket_size>
using MultiValTree = PBPTree<CustomKey, pptr<NodeBucketList<uint64_t, bucket_size>>, BRANCHKEYS, LEAFKEYS>;
