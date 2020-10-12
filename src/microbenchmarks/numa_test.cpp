
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

using namespace pmem::obj;

struct root {};

struct Foo {
    p<uint64_t> bar;
};

bool isLocOnNode(void* loc, int numaNode)
{
    int status;
    numa_move_pages( 0 /*calling process this*/, 1 /* we dont move pages */, reinterpret_cast<void**>(loc), nullptr, &status, 0);
    std::cout << "Pointer location on " << loc << " is located on node " << status << ", requested is " << numaNode << std::endl;

    return status == numaNode;
}

pmem::obj::pool<root> createPool(std::string dir)
{
    pmem::obj::pool<root> pop;
    std::string filename = "numatest";

    const std::string path = dir + filename;

    if (access(path.c_str(), F_OK) != 0) {
        std::cout << "Creating new file on " << path << std::endl;
        pop = pmem::obj::pool<root>::create(path, "numatest");
    }
    else {
        std::cout << "File " << path << " already existed, opening and returning." << std::endl;
        pop = pmem::obj::pool<root>::open(path, "numatest");
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
        foo0 = make_persistent<Foo>();
        foo0->bar = 0;
    });


    persistent_ptr<Foo> foo1;
    transaction::run(pop1, [&]() {
        foo1 = make_persistent<Foo>();
        foo1->bar = 1;
    });


    assert(isLocOnNode(foo0.get(), 0));
    assert(isLocOnNode(foo1.get(), 1));

    return 0;
}
