#pragma once

#include <core/access/RootManager.h>
#include <core/access/root.h>

#include <core/memory/constants.h>
#include <core/index/IndexDef.h>

#include <core/storage/PersistentColumn.h>

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
            if (compare(*iter, relation, table, attribute)) {
                trace_l(T_DEBUG, "Found column in persistent storage");
                return *iter;
            }
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
            if (compare(*iter, relation, table, attribute)) {
                trace_l(T_DEBUG, "Found tree in persistent storage");
                return *iter;
            }
        }
        return nullptr;
    }

    static void pushCLTree(persistent_ptr<CLTreeIndex> tree)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(tree->getPmemNode());

        pop.root()->clTreeIndeces->push_back(tree);
    }

    static pptr<CLTreeIndex> getCLTree(std::string relation, std::string table, std::string attribute, size_t pmemNode)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(pmemNode);

        auto iter = pop.root()->clTreeIndeces->begin();
 
        for (; iter != pop.root()->clTreeIndeces->end(); iter++) {
            if (compare(*iter, relation, table, attribute)) {
                trace_l(T_DEBUG, "Found tree in persistent storage");
                return *iter;
            }
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
            if (compare(*iter, relation, table, attribute)) {
                trace_l(T_DEBUG, "Found skiplist in persistent storage");
                return *iter;
            }
        }
        return nullptr;
    }

    static void pushCLSkiplist(persistent_ptr<CLSkipListIndex> skiplist)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(skiplist->getPmemNode());

        pop.root()->clSkipListIndeces->push_back(skiplist);
    }

    static pptr<CLSkipListIndex> getCLSkiplist(std::string relation, std::string table, std::string attribute, size_t pmemNode)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(pmemNode);

        auto iter = pop.root()->clSkipListIndeces->begin();
 
        for (; iter != pop.root()->clSkipListIndeces->end(); iter++) {
            if (compare(*iter, relation, table, attribute)) {
                trace_l(T_DEBUG, "Found skiplist in persistent storage");
                return *iter;
            }
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
            if (compare(*iter, relation, table, attribute)) {
                trace_l(T_DEBUG, "Found hashmap in persistent storage");
                return *iter;
            }
        }

        return nullptr;

    }

    static void pushCLHashmap(persistent_ptr<CLHashMapIndex> hashmap)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(hashmap->getPmemNode());

        pop.root()->clHashMapIndeces->push_back(hashmap);
    }

    static pptr<CLHashMapIndex> getCLHashmap(std::string relation, std::string table, std::string attribute, size_t pmemNode)
    {
        RootManager& root_mgr = RootManager::getInstance();
        auto pop = root_mgr.getPop(pmemNode);

        auto iter = pop.root()->clHashMapIndeces->begin();
 
        for (; iter != pop.root()->clHashMapIndeces->end(); iter++) {
            if (compare(*iter, relation, table, attribute)) {
                trace_l(T_DEBUG, "Found hashmap in persistent storage");
                return *iter;
            }
        }
        return nullptr;
    }

};

}
