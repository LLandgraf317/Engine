

namespace morphstore {

template<class index_structure>
class IndexGen {
public:
    static void generateFromPersistentColumn(pptr<index_structure> index, pptr<PersistentColumn> keyCol, pptr<PersistentColumn> valueCol)
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

    static void generateKeyToPos(pptr<index_structure> index, pptr<PersistentColumn> keyCol)
    {
        if (index->isInit()) return; // Should throw exception

        auto count_values = keyCol->get_count_values();
        uint64_t* key_data = keyCol->get_data();

        //TODO: slow, much optimization potential
        for (size_t i = 0; i < count_values; i++) {
            index->insert(key_data[i], i);
            if (i % 10000 == 0)
                trace_l(T_DEBUG, "Inserted key ", i);
        }

        index->setInit();
    }

};

}
