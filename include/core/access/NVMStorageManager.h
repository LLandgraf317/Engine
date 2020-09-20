#pragma once

#include <core/access/RootManager.h>
#include <core/access/root.h>

#include <core/storage/PersistentColumn.h>
#include <core/index/MultiValTreeIndex.hpp>
#include <core/index/SkipListIndex.hpp>
#include <core/index/HashMapIndex.hpp>

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
    static void pushPersistentColumn(persistent_ptr<PersistentColumn> col)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(col->getPmemNode());

        pop.root()->cols->push_back(col);
    }

    static pptr<PersistentColumn> getColumn(std::string relation, std::string table, std::string attribute, size_t pmemNode)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(pmemNode);

        auto iter = pop.root()->cols->begin();
 
        for (; iter != pop.root()->cols->end(); iter++) {
            if (compare(*iter, relation, table, attribute))
                return *iter;
        }

        return nullptr;
    }

    static void pushTree(persistent_ptr<MultiValTreeIndex> tree)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(tree->getPmemNode());

        pop.root()->treeIndeces->push_back(tree);
    }

    static pptr<MultiValTreeIndex> getTree(std::string relation, std::string table, std::string attribute, size_t pmemNode)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(pmemNode);

        auto iter = pop.root()->treeIndeces->begin();
 
        for (; iter != pop.root()->treeIndeces->end(); iter++) {
            if (compare(*iter, relation, table, attribute))
                return *iter;
        }

        return nullptr;

    }

    static void pushSkiplist(persistent_ptr<SkipListIndex> skiplist)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(skiplist->getPmemNode());

        pop.root()->skipListIndeces->push_back(skiplist);
    }

    static pptr<SkipListIndex> getSkiplist(std::string relation, std::string table, std::string attribute, size_t pmemNode)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(pmemNode);

        auto iter = pop.root()->skipListIndeces->begin();
 
        for (; iter != pop.root()->skipListIndeces->end(); iter++) {
            if (compare(*iter, relation, table, attribute))
                return *iter;
        }

        return nullptr;

    }

    static void pushHashmap(persistent_ptr<HashMapIndex> hashmap)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(hashmap->getPmemNode());

        pop.root()->hashMapIndeces->push_back(hashmap);
    }

    static pptr<HashMapIndex> getHashmap(std::string relation, std::string table, std::string attribute, size_t pmemNode)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(pmemNode);

        auto iter = pop.root()->hashMapIndeces->begin();
 
        for (; iter != pop.root()->hashMapIndeces->end(); iter++) {
            if (compare(*iter, relation, table, attribute))
                return *iter;
        }

        return nullptr;

    }

};

}
