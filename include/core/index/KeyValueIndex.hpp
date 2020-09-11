#include <iostream>
#include <cstdio>
#include <cassert>
#include <string>

#include <core/index/NodeBucketList.h>

#include <libpmemkv.hpp>

class KeyValueIndex {

private:
    db kv;

public:
    pmemkv_config* config_setup(const char* path, const uint64_t fcreate,
                 const uint64_t size) {

        pmemkv_config *cfg = pmemkv_config_new();
        assert(cfg != nullptr);

        if (pmemkv_config_put_string(cfg, "path", path) != PMEMKV_STATUS_OK) {
            fprintf(stderr, "%s", pmemkv_errormsg()); return NULL;
            return NULL;
        } 

        if (pmemkv_config_put_uint64(cfg, "force_create", fcreate) != PMEMKV_STATUS_OK) {
            fprintf(stderr, "%s", pmemkv_errormsg()); return NULL;
            return NULL;
        }

        if (pmemkv_config_put_uint64(cfg, "size", size) != PMEMKV_STATUS_OK) { fprintf(stderr, "%s", pmemkv_errormsg());
            fprintf(stderr, "%s", pmemkv_errormsg()); return NULL;
            return NULL;
        }

        return cfg;
    }

    void initialize()
    {
        if (kv.open("cmap", config(cfg)) != status::OK) {
                std::cerr << db::errormsg() << std::endl;
                return;
        }
    }

    void insert(uint64_t key, uint64_t value)
    {
        pptr<NodeBucketList<uint64_t>> list;
        if (kv.get(key, &list) != status::OK) {
            list = make_persistent<NodeBucketList<uint64_t>>(); 
            list.insertValue(value);
        }
        else {
            list.insertValue(value);
        }
    }

    pmem::obj::persistent_ptr<NodeBucketList<uint64_t>> find(uint64_t key)
    {
        pptr<NodeBucketList<uint64_t>> list;
        if (kv.get(key, &list) != status::OK) {
            return list;    
        }
        else {
            return nullptr;
        }
    }

    bool remove(uint64_t key, uint64_t value)
    {
        pptr<NodeBucketList<uint64_t>> list;
        if (kv.get(key, &list) != status::OK) {
            return false;
        }
        else {
            return list->remove(value);
        }
    }

};
