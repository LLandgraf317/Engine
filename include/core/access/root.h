#pragma once

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>


struct root {
    pmem::obj::persistent_ptr<uint64_t> col_count;
};
