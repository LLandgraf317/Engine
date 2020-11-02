#pragma once

#include <core/access/RootManager.h>
#include <core/access/root.h>

#include <core/memory/constants.h>
#include <core/index/IndexDef.h>

#include <core/storage/PersistentColumn.h>

#define PBOILERPLATE(classname, popmember)                   \
    static persistent_ptr<classname> get##classname(std::string relation, std::string table, std::string attribute, size_t pmemNode) { \
        RootManager& root_mgr = RootManager::getInstance(); \
        auto pop = root_mgr.getPop(pmemNode);               \
        auto iter = pop.root()->popmember->begin();              \
        for (; iter != pop.root()->popmember->end(); iter++) {   \
            if (compare(*iter, relation, table, attribute)) { \
                trace_l(T_DEBUG, "Found column in persistent storage"); \
                return *iter;                               \
            }                                               \
        }                                                   \
                                                            \
        return nullptr;                                     \
    }                                                       \
                                                            \
    static void remove##classname( persistent_ptr<classname> index ) \
    {                                                       \
        RootManager& root_mgr = RootManager::getInstance(); \
        auto pop = root_mgr.getPop(index->getNumaNode());               \
        for (auto i = pop.root()->popmember->begin(); i != pop.root()->popmember->end(); i++) { \
            if (*i == index) { \
                pop.root()->popmember->erase(i);                \
                return;\
            } \
        } \
    }                                                       \
                                                            \
    static void push##classname( persistent_ptr<classname> index ) \
    {                                                       \
        RootManager& root_mgr = RootManager::getInstance(); \
        auto pop = root_mgr.getPop(index->getNumaNode());   \
                                                            \
        pop.root()->popmember->push_back(index);           \
    }                                                       \
                                                            \
    static pmem::obj::vector<persistent_ptr<classname>> & get##classname##s(size_t pmemNode) \
    {                                                       \
        RootManager& root_mgr = RootManager::getInstance(); \
        auto pop = root_mgr.getPop(pmemNode);   \
                                                    \
        return *pop.root()->popmember.get();               \
    }


namespace morphstore {

class NVMStorageManager {

private:
    template <class index_structure>
    static bool compare(pptr<index_structure> index, std::string relation, std::string table, std::string attribute)
    {
        trace_l(T_DEBUG, "Looking for ", relation, ", ", table, ", ", attribute);
        trace_l(T_DEBUG, "Got ", index->getRelation(), ", ", index->getTable(), ", ", index->getAttribute());
        return index->getRelation().compare(relation) == 0 && index->getTable().compare(table) == 0 && index->getAttribute().compare(attribute) == 0;
    }

public:

    PBOILERPLATE(MultiValTreeIndex, treeIndeces);
    PBOILERPLATE(SkipListIndex, skipListIndeces);
    PBOILERPLATE(HashMapIndex, hashMapIndeces);
    PBOILERPLATE(PersistentColumn, cols);
    PBOILERPLATE(CLTreeIndex, clTreeIndeces);
    PBOILERPLATE(CLHashMapIndex, clHashMapIndeces);
    PBOILERPLATE(CLSkipListIndex, clSkipListIndeces);

};

}
