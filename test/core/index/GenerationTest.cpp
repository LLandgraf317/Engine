
#include <core/memory/global/mm_hooks.h>
#include <core/memory/management/allocators/global_scope_allocator.h>
#include <core/memory/management/utils/alignment_helper.h>

#include <core/access/RootManager.h>
#include <core/access/root.h>

#include <core/index/NodeBucketList.h>
#include <core/index/VNodeBucketList.h>

#include <core/storage/column_gen.h>
#include <core/index/index_gen.h>
#include <core/index/IndexDef.h>

#include <iostream>
#include <chrono>

#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>

using namespace morphstore;
using pmem::obj::delete_persistent;

    //using ScanFunc = std::function<void(const uint64_t &key, const pptr<NodeBucketList<uint64_t>> &val)>;
    /*index1->scan([] (const uint64_t &key, const pptr<NodeBucketList<uint64_t>> &val) {
        trace_l(T_INFO, "Key ", key, " with count values ", val->getCountValues());
    });*/
pobj_alloc_class_desc alloc_class;

template<class index_structure>
void check(pptr<PersistentColumn> keyColPers, pptr<index_structure> index1, pptr<index_structure> index2)
{
    RootManager& root_mgr = RootManager::getInstance();
    auto pop = root_mgr.getPop(0);

    auto start = std::chrono::system_clock::now();
    IndexGen::generateKeyToPos(index1, keyColPers);
    auto end = std::chrono::system_clock::now();

    std::chrono::duration<double> dur1 = end - start;

    start = std::chrono::system_clock::now();
    IndexGen::generateFast<pptr<index_structure>, OSP_SIZE>(index2, keyColPers);
    end = std::chrono::system_clock::now();

    std::chrono::duration<double> dur2 = end - start;

    trace_l(T_INFO, "Slow method: ", dur1.count());
    trace_l(T_INFO, "Fast method: ", dur2.count());

    assert(dur2 < dur1);

    for (uint64_t i = 0; i < 100; i++) {
        auto buck1 = index1->find(i);
        auto buck2 = index2->find(i);

        assert(buck1->getCountValues() == buck2->getCountValues());

        /*trace_l(T_INFO, "buck1 values:");
        buck1->printAll();
        trace_l(T_INFO, "buck2 values:");
        buck2->printAll();*/

        auto iter = buck1->begin();
        while (iter != buck1->end()) {
            assert(buck2->lookup(*iter));
            iter++;
        }
    }

    index1->prepareDest();
    index2->prepareDest();

    transaction::run(pop, [&] () { 
        delete_persistent<index_structure>(index1);
        delete_persistent<index_structure>(index2);
    });

}

int main( void ) {
    RootInitializer::getInstance().initPmemPool(std::string("generationtest"), std::string("NVMDS"), 3ul << 29);

    RootManager& root_mgr = RootManager::getInstance();
    auto pop = root_mgr.getPop(0);

    const size_t SEED = 66;
    const size_t pmemNode = 0;
    const size_t ARRAY_SIZE = 100000;

    auto keyColPers = generate_with_distr_pers(
        ARRAY_SIZE, std::uniform_int_distribution<uint64_t>(0, 99), false, SEED, pmemNode); 


    persistent_ptr<MultiValTreeIndex> tree1;
    persistent_ptr<MultiValTreeIndex> tree2;

    transaction::run(pop, [&] () { 
        tree1 = make_persistent<MultiValTreeIndex>(0, alloc_class, std::string(""), std::string(""), std::string(""));
    });
    transaction::run(pop, [&] () { 
        tree2 = make_persistent<MultiValTreeIndex>(0, alloc_class, std::string(""), std::string(""), std::string(""));
    });

    persistent_ptr<SkipListIndex> s1;
    persistent_ptr<SkipListIndex> s2;

    transaction::run(pop, [&] () { 
        s1 = make_persistent<SkipListIndex>(0, std::string(""), std::string(""), std::string(""));
    });
    transaction::run(pop, [&] () { 
        s2 = make_persistent<SkipListIndex>(0, std::string(""), std::string(""), std::string(""));
    });

    persistent_ptr<HashMapIndex> h1;
    persistent_ptr<HashMapIndex> h2;

    transaction::run(pop, [&] () { 
        h1 = make_persistent<HashMapIndex>(100, 0, std::string(""), std::string(""), std::string(""));
    });
    transaction::run(pop, [&] () { 
        h2 = make_persistent<HashMapIndex>(100, 0, std::string(""), std::string(""), std::string(""));
    });

    check<MultiValTreeIndex>(keyColPers, tree1, tree2);
    check<SkipListIndex>(keyColPers, s1, s2);
    check<HashMapIndex>(keyColPers, h1, h2);

    keyColPers->prepareDest();
    transaction::run(pop, [&] () { 
        delete_persistent<PersistentColumn>(keyColPers);
    });

    RootInitializer::getInstance().cleanUp();

    return 0;
}
