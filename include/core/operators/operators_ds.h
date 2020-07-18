#include <nvmdatastructures/src/pbptrees/PBPTree.hpp>
#include <core/storage/column.h>
#include <core/storage/VolatileColumn.h>

#include <functional>

using storage::VolatileColumn;

/**** EXESSIVE TEMPLATE GAME ****/

template<template<typename, typename, int, int> class Tree, typename KeyType, typename ValueType, int N, int M, template<typename> class t_op>
struct select_t {
    static VolatileColumn * apply( Tree<KeyType, ValueType, N, M> * in, uint64_t val )
    {
        //PBPTrees do not allow duplicate values, how handle?
        uint64_t res = 0;
         VolatileColumn * col = new VolatileColumn(in->get_size(), 0);
        uint64_t* outpos = col->get_data();
         uint64_t* initOutPos = outpos;

        t_op<uint64_t> op;

        auto iter = in->begin();
        while (!op(iter->key, val) && iter != in->end())
            iter++;

        while (op(iter->key, val) && iter != in->end()) {
            //entry into resulting column of positionals
            *outpos = iter->key;            
            iter++;
        }
        //Tree is sorted, abort and return

        *outpos = res;

        return col;
    }
};

template<template<typename, typename, int, int> class Tree, typename KeyType, typename ValueType, int N, int M>
VolatileColumn * select_value(Tree<KeyType,ValueType, N,M>* tree, size_t max_elements, ValueType value)
{
    auto newCol = new VolatileColumn(max_elements * sizeof(uint64_t), 0);
    uint64_t* valData = newCol->get_data();
    
    auto valueComp = [&valData, &value, &newCol](KeyType key, ValueType node_val)
    {
        if (node_val == value)
            *valData = key;
        valData++;
    };
    tree->scan(valueComp);

    return newCol;
}

template<template<typename, typename, int, int> class Tree, typename KeyType, typename ValueType, int N, int M>
struct select_t<Tree, KeyType, ValueType, N, M, std::equal_to> {

    static VolatileColumn * apply( Tree<KeyType, ValueType, N, M> * in, uint64_t val )
    {
        //PBPTrees do not allow duplicate values, how handle?
        std::tuple<uint64_t> res = {0};
        bool success = in->lookup(val, &res);
        if (!success)
            return nullptr;

         VolatileColumn * col = new VolatileColumn(sizeof(uint64_t), 0);

        uint64_t* outpos = col->get_data();
        *outpos = std::get<0>(res);

        return col;
    }
};

/*template<template<typename, typename, int, int> class Tree, typename KeyType, typename ValueType, int N, int M>
struct select_t<Tree, KeyType, ValueType, N, M, std::less> {
     VolatileColumn * apply( Tree<KeyType, ValueType, N, M> * in, uint64_t val )
    {
        //PBPTrees do not allow duplicate values, how handle?
        uint64_t res = 0;
        bool success = in->lookup(val, &res);
        if (!success)
            return nullptr;

         VolatileColumn * col = new VolatileColumn(in->get_size());

        uint64_t* outpos = col->get_data();
         uint64_t* initOutPos = outpos;

        *outpos = res;

        return col;
    }//,  Column *  inDataCol,  uint64_t value)
};*/

template<typename KeyType, typename ValueType,
    template<typename, typename, int, int> class Tree,
    int N, int M>
struct project_t {
    VolatileColumn * apply( Tree<KeyType, ValueType, N, M> * /*in*/)
    {
        return nullptr;
    }
};

template<typename KeyType, typename ValueType,
    template<typename, typename, int, int> class Tree1,
    int N1, int M1,
    template<typename, typename, int, int> class Tree2,
    int N2, int M2>
struct join_t {
     std::tuple<
             VolatileColumn *,
             VolatileColumn *
    >
    apply( Tree1<KeyType, ValueType, N1, M1> * /*inDataLCol*/,  Tree2<KeyType, ValueType, N2, M2> * /*inDataRCol*/)
    {
        return std::make_tuple< VolatileColumn *,  VolatileColumn *>(nullptr, nullptr);
    }
};

template<typename KeyType, typename ValueType,
    template<typename, typename, int, int> class Tree1,
    int N1, int M1,
    template<typename, typename, int, int> class Tree2,
    int N2, int M2>
 std::tuple<
         VolatileColumn *,
         VolatileColumn *
>
group( Tree1<KeyType, ValueType, N1, M1> * /*inGrCol*/,  Tree2<KeyType, ValueType, N2, M2> * /*inDataCol*/)
{
    return std::make_tuple<VolatileColumn *, VolatileColumn *>(nullptr, nullptr);
}

template<
    typename KeyType,
    typename ValueType,
    template<typename, typename, int, int> class Tree1,
    int N1, int M1,
    template<typename, typename, int, int> class Tree2,
    int N2, int M2>
 VolatileColumn * merge( Tree1<KeyType, ValueType, N1, M1> * /*inPosLCol*/,  Tree2<KeyType, ValueType, N2, M2> * /*inPosRCol*/)
{
    return nullptr;
}

template<typename KeyType, typename ValueType,
    template<typename, typename, int, int> class Tree,
    int N, int M>
 VolatileColumn * agg_sum( Tree<KeyType, ValueType, N, M> * /*inDataCol*/)
{
    return nullptr;
}
