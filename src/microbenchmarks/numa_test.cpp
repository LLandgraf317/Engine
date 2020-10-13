
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <string>
#include <iostream>
#include <cassert>
#include <numa.h>
#include <numaif.h>
#include <unistd.h>
#include <sys/mman.h>

using namespace pmem::obj;

struct Foo {
    p<uint64_t> bar;
    persistent_ptr<uint64_t[]> foo;
};

struct root {
    persistent_ptr<Foo> foo;
};

bool isLocOnNode(void* loc, int numaNode)
{
    int ret_numa;
    void * page = reinterpret_cast<void*>(reinterpret_cast<uint64_t>(loc) & ~( (4096ul) - 1));

    auto ret = get_mempolicy(&ret_numa, NULL, 0, page, MPOL_F_NODE | MPOL_F_ADDR);
    //numa_move_pages( 0 /*calling process this*/, 1 /* we dont move pages */, reinterpret_cast<void**>(page), nullptr, &status, 0);
    std::cout << "Pointer location on " << page << " is located on node " << ret_numa << ", requested is " << numaNode << ", status returned is "<< ret << std::endl;

    return ret_numa == numaNode;
}

pmem::obj::pool<root> createPool(std::string dir)
{
    pmem::obj::pool<root> pop;
    std::string filename = "numatest";

    const std::string path = dir + filename;

    if (access(path.c_str(), F_OK) != 0) {
        std::cout << "Creating new file on " << path << std::endl;
        pop = pmem::obj::pool<root>::create(path, "numatest", PMEMOBJ_MIN_POOL, S_IRWXU);
        transaction::run(pop, [&]() {
            pop.root()->foo = make_persistent<Foo>();
            pop.root()->foo->foo = make_persistent<uint64_t[]>(4096);
        });
    }
    else {
        std::cout << "File " << path << " already existed, opening and returning." << std::endl;
        pop = pmem::obj::pool<root>::open(path, "numatest");
        std::cout << "Pops root is " << pop.root()->foo->bar << std::endl;
    }

    return pop;
}

int main( void ) {

    pmem::obj::pool<root> pop0;
    pmem::obj::pool<root> pop1;

    std::string path0 = "/mnt/pmem0/";
    std::string path1 = "/mnt/pmem1/";

    pop0 = createPool(path0);
    pop1 = createPool(path1);

    persistent_ptr<Foo> foo0;
    transaction::run(pop0, [&]() {
        pop0.root()->foo->bar.get_rw() = 0;

        foo0 = make_persistent<Foo>();
        foo0->bar.get_rw() = 0;
    });

    uint64_t * ptr = pop0.root()->foo->foo.get();
    for (size_t i = 0; i < 4096; i++)
        ptr[i] = i;
    assert(isLocOnNode(pop0.root()->foo->foo.get(), 0));

    persistent_ptr<Foo> foo1;
    transaction::run(pop1, [&]() {
        pop1.root()->foo->bar.get_rw() = 1;

        foo1 = make_persistent<Foo>();
        foo1->bar.get_rw() = 1;
    });

    uint64_t * ptr1 = pop1.root()->foo->foo.get();
    for (size_t i = 0; i < 4096; i++)
        ptr1[i] = i;
    assert(isLocOnNode(pop1.root()->foo->foo.get(), 1));


    uint64_t sum = foo1->bar + foo0->bar;

    pop0.persist(foo0);
    pop0.flush(foo0);
    pop0.drain();

    pop1.persist(foo1);
    pop1.flush(foo1);
    pop1.drain();

    transaction::run(pop0, [&]() {
        foo0->bar = sum;
        pop0.memcpy_persist(foo1.get(), foo0.get(), sizeof(Foo));
    });
    transaction::run(pop1, [&]() {
        foo1->bar = sum;
        pop1.memcpy_persist(foo0.get(), foo1.get(), sizeof(Foo));
    });

    msync(&pop0.root()->foo->bar, sizeof(p<uint64_t>), MS_SYNC);
    msync(&pop1.root()->foo->bar, sizeof(p<uint64_t>), MS_SYNC);
    msync(foo0.get(), sizeof(Foo), MS_SYNC);
    msync(foo1.get(), sizeof(Foo), MS_SYNC);

    assert(isLocOnNode(&pop0.root()->foo->bar.get_rw(), 0));
    assert(isLocOnNode(&pop1.root()->foo->bar.get_rw(), 1));
    assert(isLocOnNode(foo0.get(), 0));
    assert(isLocOnNode(foo1.get(), 1));

    return 0;
}
