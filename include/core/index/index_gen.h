#pragma once

#include <libpmemobj++/persistent_ptr.hpp>
#include <core/storage/PersistentColumn.h>

namespace morphstore {

using pmem::obj::persistent_ptr;

template<class index_structure_ptr>
class IndexGen {
public:
    static void generateFromPersistentColumn(index_structure_ptr index, persistent_ptr<PersistentColumn> keyCol, persistent_ptr<PersistentColumn> valueCol)
    {
        if (index->isInit()) return; // Should throw exception instead

        auto count_values = keyCol->get_count_values();
        uint64_t* key_data = keyCol->get_data();
        uint64_t* value_data = valueCol->get_data();

        //TODO: slow, much optimization potential
        for (size_t i = 0; i < count_values; i++) {
            index->insert(key_data[i], value_data[i]);
        }

        index->setInit();
    }

    static void generateKeyToPos(index_structure_ptr index, persistent_ptr<PersistentColumn> keyCol)
    {
        RootManager& root_mgr = RootManager::getInstance();
        if (index->isInit()) return; // Should throw exception

        auto count_values = keyCol->get_count_values();
        uint64_t* key_data = keyCol->get_data();

        //TODO: slow, much optimization potential
        for (size_t i = 0; i < count_values; i++) {
            index->insert(key_data[i], i);
            if (i % 5000 == 0) {
                trace_l(T_INFO, "Inserted key ", i);
                root_mgr.drainAll();
            }
        }

        index->setInit();
    }

};

}
