#include <core/index/PHashMap.hpp>


class HashMapIndex {

    pptr<PHashMap <ps, uint64_t, uint64_t>> m_HashMap;
    uint64_t m_PmemNode;

    pptr<char[]> m_Table;
    pptr<char[]> m_Relation;
    pptr<char[]> m_Attribute;

    HashMapIndex(uint64_t pMemNode, pobj_alloc_class_desc alloc_class, std::string table, std::string relation, std::string attribute)
    {

    }

    void generateFromPersistentColumn(pptr<PersistentColumn> keyCol, pptr<PersistentColumn> valueCol)
    {

    }

    void generateKeyToPos(pptr<PersistentColumn> keyCol)
    {

    }

    pptr<NodeBucketList<uint64_t>> find(uint64_t key)
    {
        return nullptr;
    }

    void insert(uint64_t key, uint64_t value)
    {

    }

    bool lookup(uint64_t key, uint64_t val)
    {
        return false;
    }

    using ScanFunc = std::function<void(const uint64_t &key, const pptr<NodeBucketList<uint64_t>> &val)>;
    void scan(const uint64_t &minKey, const uint64_t &maxKey, ScanFunc func) const
    {

    }

    void scan(ScanFunc func) const
    {

    }

    bool deleteEntry(uint64_t key, uint64_t value)
    {
        return false;
    }

};
