#pragma once

#include <core/operators/general_vectorized/project_compr.h>
#include <core/operators/general_vectorized/select_compr.h>
#include <core/operators/scalar/agg_sum_uncompr.h>
#include <core/operators/scalar/select_uncompr.h>
#include <core/operators/general_vectorized/intersect_uncompr.h>

#include <core/utils/measure.h>


namespace morphstore {

using namespace vectorlib;
using ps = scalar<v64<uint64_t>>;

class Query {

    using Dur = std::chrono::duration<double>;
private:
    std::chrono::duration<double> exec_time;
    std::chrono::time_point<std::chrono::system_clock> starttime;
    std::chrono::time_point<std::chrono::system_clock> endtime;
protected:
    pthread_t thread_id;
    std::atomic<uint64_t>& downVar;
    bool ready = false;

public:
    Query(std::atomic<uint64_t>& dv)
        : downVar(dv) 
    {
    }

    static void waitAllReady(std::atomic<uint64_t>& down) //std::vector<bool>& queue)
    {
        while (down != 0) {}
    }

    inline void start()
    {
        starttime = std::chrono::system_clock::now();
    }

    inline void end()
    {
        endtime = std::chrono::system_clock::now();
    }

    Dur getExecTime()
    {
        std::chrono::duration<double> dur = endtime - starttime;

        return dur;
    }

    /*void setWaitQueue(std::atomic<uint64_t>& downVar)
    {
        this->downVar = downVar;
    }*/

    void setReady()
    {
        ready = true;
    }

    pthread_t getThread()
    {
        return thread_id;
    }
};

class QueryCollection {
private:
    std::atomic<uint64_t> downVar = 0;
    std::vector<Query*> queries;

public:
    void pushQuery(Query * query)
    {
        //downVar++;
        //query->setWaitQueue(downVar);
        queries.push_back(query);
    }

    void waitAllReady() {
        while (downVar != 0) {}

        for (auto i : queries) 
            pthread_join(i->getThread(), nullptr);
    }

    template< typename query_type >
    query_type * create()
    {
        auto q = new query_type(downVar);
        pushQuery(q);

        return q;
    }
};

template< typename data_struct_ptr >
struct ArgList {
    using Dur = std::chrono::duration<double>;
private:
    std::chrono::time_point<std::chrono::system_clock> starttime;
    std::chrono::time_point<std::chrono::system_clock> endtime;

public:
    ArgList(uint64_t selection, const column<uncompr_f> * col, data_struct_ptr index, const uint64_t node) //std::vector<bool>& readyQueue)
        : sel(selection), xCol(col), datastruct(index), runNode(node) {
        }

    inline void start()
    {
        starttime = std::chrono::system_clock::now();
    }

    inline void end()
    {
        endtime = std::chrono::system_clock::now();
    }

    Dur getExecTime()
    {
        std::chrono::duration<double> dur = endtime - starttime;

        return dur;
    }

    const uint64_t sel;
    const column<uncompr_f> * xCol;
    data_struct_ptr datastruct;
    const uint64_t runNode;

    std::atomic<uint64_t> * downVar;
};

/*template< typename index_structure_ptr >
struct ArgIndexList
{
    ArgIndexList(uint64_t selection, const column<uncompr_f> * col, index_structure_ptr index, const uint64_t node) //std::vector<bool>& readyQueue)
        : sel(selection), xCol(col), yIndex(index), runNode(node) {
        }

    void print() {
        //trace_l(T_DEBUG, "Index arg list with selection ", sel, ", col ", xCol, ", index ", index, ", thread number ", threadNum , ", max threads ", maxThreads, ", queue ", &queue);
    }
             
    const uint64_t sel;
    const column<uncompr_f> * xCol;
    index_structure_ptr yIndex;
    const uint64_t runNode;

    std::atomic<uint64_t> * downVar;
};

template< typename col_ptr >
struct ArgColList
{
    ArgColList(uint64_t selection, const column<uncompr_f> * x, col_ptr i, const uint64_t node)//std::vector<bool>& readyQueue)
        : sel(selection), xCol(x), yCol(i), runNode(node) {//queue(readyQueue) {
        }

    void print() {
        //trace_l(T_DEBUG, "col arg list with selection ", sel, ", col ", xCol, ", index ", col, ", thread number ", threadNum , ", max threads ", maxThreads, ", queue ", &queue);
    }

    const uint64_t sel;
    const column<uncompr_f> * xCol;
    col_ptr yCol;
    const uint64_t runNode;

    std::atomic<uint64_t> * downVar;
};*/

class SingleSelectSumQuery : public Query {
    using Dur = std::chrono::duration<double>;
public:
    SingleSelectSumQuery(std::atomic<uint64_t>& down) : Query(down) {}

    template< typename index_structure_ptr >
    Dur runIndex(const column<uncompr_f> * xCol, index_structure_ptr index, const uint64_t selection)
    {
        //numa_run_on_node(runnode1);
        start();
        auto select = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( index, selection);

        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, select);

        auto res = agg_sum<ps, uncompr_f>(projection);
        end();

        delete select;
        delete projection;
        delete res;

        return getExecTime();
    }

    template< typename col_ptr >
    Dur runCol(const column<uncompr_f> * xCol, col_ptr col, const uint64_t selection)
    {
        start();
        auto select = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( col, selection);
        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, select);
        auto res = agg_sum<ps, uncompr_f>(projection);

        end();

        delete select;
        delete projection;
        delete res;

        return getExecTime();
    }

    template< typename index_structure_ptr >
    static void* runIndexPT(void * argPtr)
    {
        ArgList< index_structure_ptr > * args = reinterpret_cast<ArgList<index_structure_ptr>*>(argPtr);
        //args->print();

        numa_run_on_node(args->runNode);
        const uint64_t selection = args->sel;
        auto xCol = args->xCol;
        auto index = args->datastruct;

        (*args->downVar)--;
        waitAllReady(*args->downVar);

        args->start();
        auto select = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( index, selection);
        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, select);

        auto res = agg_sum<ps, uncompr_f>(projection);
        args->end();

        delete select;
        delete projection;
        delete res;

        return nullptr;
    }

    template< typename col_ptr >
    static void * runColPT(void * argPtr)
    {
        ArgList<col_ptr> * args = reinterpret_cast<ArgList<col_ptr>*>(argPtr);
        //args->print();

        numa_run_on_node(args->runNode);
        const uint64_t selection = args->sel;
        auto xCol = args->xCol;
        auto yCol = args->datastruct;

        (*args->downVar)--;
        waitAllReady(*args->downVar);

        args->start();
        auto select = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( yCol, selection);
        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, select);
        auto res = agg_sum<ps, uncompr_f>(projection);
        args->end();

        delete select;
        delete projection;
        delete res;

        return nullptr;
    }

    template< typename index_structure_ptr >
    void dispatchAsyncIndex(ArgList< index_structure_ptr > * argList )
    {
        argList->downVar = &downVar;
        pthread_create(&thread_id, nullptr, SingleSelectSumQuery::runIndexPT<index_structure_ptr>, reinterpret_cast<void*>(argList));
    }

    template< typename col_ptr >
    void dispatchAsyncColumn(ArgList< col_ptr > * argList )
    {
        argList->downVar = &downVar;
        pthread_create(&thread_id, nullptr, SingleSelectSumQuery::runColPT< col_ptr >, reinterpret_cast<void*>(argList));
    }

    /*template< typename col_ptr, typename index_structure_ptr >
    std::tuple<Dur, Dur> runColIndexThreads(const column<uncompr_f> * xCol, const uint64_t selection, 
            col_ptr yCol,               const uint64_t colThreadCount,   const uint64_t colRunNode,
            index_structure_ptr yIndex, const uint64_t indexThreadCount, const uint64_t indexRunNode)
    {
        uint64_t allThreadCount = colThreadCount + indexThreadCount;

        std::vector<pthread_t> thread_ids(allThreadCount);
        std::atomic<uint64_t> downVar = allThreadCount;

        for (uint64_t i = 0; i < indexThreadCount; i++) {
            ArgIndexList< index_structure_ptr > * args = new ArgIndexList< index_structure_ptr >( selection, xCol, yIndex, i, indexThreadCount, downVar );
            pthread_t temp;
            pthread_create(&temp, nullptr, SingleSelectSumQuery::runIndexPT<index_structure_ptr>, reinterpret_cast<void*>(args));
            thread_ids[i] = temp;
        }
        for (uint64_t i = 0; i < colThreadCount; i++) {
            ArgColList< col_ptr > * args = new ArgColList<col_ptr>( selection, xCol, yCol, i, colThreadCount, downVar );
            pthread_t temp;
            pthread_create(&temp, nullptr, SingleSelectSumQuery::runColPT<col_ptr>, reinterpret_cast<void*>(args));
            thread_ids[i] = temp;
        }
        waitAllReady(downVar);

        start();
        for (uint64_t i = 0; i < thread_count; i++) {
            pthread_join(thread_ids[i], nullptr);
        }
        end();

        return getExecTime();
    }

    // Select sum(x) from r where y = c
    template< typename index_structure_ptr >
    Dur runIndexThreads(const column<uncompr_f> * xCol, index_structure_ptr index, const uint64_t selection, uint64_t thread_count)
    {
        numa_run_on_node(0);

        std::vector<pthread_t> thread_ids(thread_count);
        std::atomic<uint64_t> downVar = thread_count;

        for (uint64_t i = 0; i < thread_count; i++) {
            ArgIndexList< index_structure_ptr > * args = new ArgIndexList< index_structure_ptr >( selection, xCol, index, i, thread_count, downVar );
            pthread_t temp;
            pthread_create(&temp, nullptr, SingleSelectSumQuery::runIndexPT<index_structure_ptr>, reinterpret_cast<void*>(args));
            thread_ids[i] = temp;
        }
        waitAllReady(downVar);
        start();
        for (uint64_t i = 0; i < thread_count; i++) {
            pthread_join(thread_ids[i], nullptr);
        }
        end();

        return getExecTime();
    }

    template< typename col_ptr >
    Dur runColThreads(const column<uncompr_f> * xCol, col_ptr col, const uint64_t selection, uint64_t thread_count)
    {
        numa_run_on_node(0);

        std::vector<pthread_t> thread_ids(thread_count);
        std::atomic<uint64_t> downVar = thread_count;

        for (uint64_t i = 0; i < thread_count; i++) {
            ArgColList< col_ptr > * args = new ArgColList<col_ptr>( selection, xCol, col, i, thread_count, downVar );
            pthread_t temp;
            pthread_create(&temp, nullptr, SingleSelectSumQuery::runColPT<col_ptr>, reinterpret_cast<void*>(args));
            thread_ids[i] = temp;
        }
        waitAllReady(downVar);
        start();
        for (uint64_t i = 0; i < thread_count; i++) {
            pthread_join(thread_ids[i], nullptr);
        }
        end();

        return getExecTime();
    }*/
};

class DoubleSelectSumQuery : public Query {
public:
    template< typename index_structure_ptr >
    Dur runIndex(const column<uncompr_f> * xCol, index_structure_ptr yIndex, persistent_ptr<MultiValTreeIndex> zTree, const uint64_t selection1, const uint64_t selection2)
    {
        start();
        auto select1 = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( yIndex, selection1);
        auto select2 = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            persistent_ptr<MultiValTreeIndex>, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( zTree, selection2);
        auto intersect = intersect_sorted<ps, uncompr_f, uncompr_f, uncompr_f >(select1, select2);

        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, intersect);
        auto res = agg_sum<ps, uncompr_f>(projection);
        end();

        delete select1;
        delete select2;
        delete intersect;
        delete projection;
        delete res;

        return getExecTime();
    }

    template< typename col_ptr >
    Dur runCol(const column<uncompr_f> * xCol, col_ptr yCol, persistent_ptr<MultiValTreeIndex> zTree, const uint64_t selection1, const uint64_t selection2)
    {
        start();
        auto select1 = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( yCol, selection1);
        /*auto select2 = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( col, selection2);*/
        auto select2 = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            persistent_ptr<MultiValTreeIndex>, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( zTree, selection2);
        auto intersect = intersect_sorted<ps, uncompr_f, uncompr_f, uncompr_f >(select1, select2);

        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, intersect);
        auto res = agg_sum<ps, uncompr_f>(projection);

        end();

        delete select1;
        delete select2;
        delete intersect;
        delete projection;
        delete res;

        return getExecTime();
    }

    template< typename col_ptr >
    Dur runColCol(const column<uncompr_f> * xCol, col_ptr yCol, col_ptr zCol, const uint64_t selection1, const uint64_t selection2)
    {
        start();
        auto select1 = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( yCol, selection1);
        /*auto select2 = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( col, selection2);*/
        auto select2 = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( zCol, selection2);
        auto intersect = intersect_sorted<ps, uncompr_f, uncompr_f, uncompr_f >(select1, select2);

        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, intersect);
        auto res = agg_sum<ps, uncompr_f>(projection);

        end();

        delete select1;
        delete select2;
        delete intersect;
        delete projection;
        delete res;

        return getExecTime();
    }

};

}
