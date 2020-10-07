#pragma once

#include <unistd.h>
#include <numa.h>
#include <sys/statvfs.h>

namespace morphstore {

class AccessPathManager {
public:
    static AccessPathManager & getInstance() {
        static AccessPathManager instance;
        return instance;
    }

    size_t getTotalSystemMemory()
    {
        long pages = sysconf(_SC_PHYS_PAGE);
        long page_size = sysconf(_SC_PAGE_SIZE);
        return pages * page_size;
    }

    size_t nvramRemaining(size_t nodeNumber)
    {

    }

    size_t dramRemaining(size_t nodeNumber)
    {

    }

    void pinExecutionToNode(size_t nodeNumber)
    {

    }

    bool checkNumaLocation(void* address, size_t nodeNumber)
    {

    }

    column<uncompr_f> * optimizeBetween(column<uncompr_f> * col, uint64_t lower, uint64_t upper)
    {

        return nullptr;
    }

    column<uncompr_f> * optimizeSelect(column<uncompr_f> * col, uint64_t pred)
    {

        return nullptr;
    }

    column<uncompr_f> * optimizeGroupAggr(column<uncompr_f> * grCol, column<uncompr_f> * dataCol)
    {

        return nullptr;
    }

    column<uncompr_f> * optimizeJoin(column<uncompr_f> * left, column<uncompr_f> * right)
    {

        return nullptr;
    }

}

}
