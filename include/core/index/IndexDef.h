#pragma once

#include <core/index/MultiValTreeIndex.hpp>
#include <core/index/VolatileTreeIndex.hpp>
#include <core/index/SkipListIndex.hpp>
#include <core/index/VSkipListIndex.hpp>
#include <core/index/HashMapIndex.hpp>
#include <core/index/VHashMapIndex.hpp>

namespace morphstore {
    using MultiValTreeIndex = PTreeIndex<OSP_SIZE>;
    using HashMapIndex = PHashMapIndex<OSP_SIZE>;
    using SkipListIndex = PSkipListIndex<OSP_SIZE>;
}
