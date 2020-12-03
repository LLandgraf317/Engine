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

public:
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

};

template< typename index_structure_ptr >
struct ArgIndexList
{
    ArgIndexList(uint64_t selection, const column<uncompr_f> * col, index_structure_ptr i, uint64_t t, const uint64_t m, std::atomic<uint64_t>& down) //std::vector<bool>& readyQueue)
        : sel(selection), xCol(col), index(i), threadNum(t), maxThreads(m), downVar(down) {//queue(readyQueue) {
        }

    void print() {
        trace_l(T_DEBUG, "Index arg list with selection ", sel, ", col ", xCol, ", index ", index, ", thread number ", threadNum , ", max threads ", maxThreads, ", queue ", &queue);
    }
             
    const uint64_t sel;
    const column<uncompr_f> * xCol;
    index_structure_ptr index;
    const uint64_t threadNum;
    const uint64_t maxThreads;
    std::atomic<uint64_t>& downVar;

};

template< typename col_ptr >
struct ArgColList
{
    ArgColList(uint64_t selection, const column<uncompr_f> * x, col_ptr i, uint64_t t, const uint64_t m, std::atomic<uint64_t>& down)//std::vector<bool>& readyQueue)
        : sel(selection), xCol(x), col(i), threadNum(t), maxThreads(m), downVar(down) {//queue(readyQueue) {
        }

    void print() {
        trace_l(T_DEBUG, "col arg list with selection ", sel, ", col ", xCol, ", index ", col, ", thread number ", threadNum , ", max threads ", maxThreads, ", queue ", &queue);
    }

    const uint64_t sel;
    const column<uncompr_f> * xCol;
    col_ptr col;
    const uint64_t threadNum;
    const uint64_t maxThreads;
    //std::vector<bool>& queue;
    std::atomic<uint64_t>& downVar;
};

class SingleSelectSumQuery : public Query {
    using Dur = std::chrono::duration<double>;
public:
    template< typename index_structure_ptr >
    Dur runIndex(const column<uncompr_f> * xCol, index_structure_ptr index, const uint64_t selection)
    {
        //numa_run_on_node(runnode1);
        start();
        auto select = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( index, selection);
        //numa_run_on_node(runnode2);
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
        //numa_run_on_node(runnode1);
        start();
        auto select = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( col, selection);
        //numa_run_on_node(runnode2);
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
        ArgIndexList< index_structure_ptr > * args = reinterpret_cast<ArgIndexList<index_structure_ptr>*>(argPtr);
        args->print();

        const uint64_t selection = args->sel;
        auto xCol = args->xCol;
        auto index = args->index;

        args->downVar--;
        //args->queue[args->threadNum] = true;
        waitAllReady(args->downVar);

        auto select = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( index, selection);
        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, select);

        auto res = agg_sum<ps, uncompr_f>(projection);

        delete select;
        delete projection;
        delete res;

        return nullptr;
    }

    template< typename col_ptr >
    static void * runColPT(void * argPtr)
    {
        ArgColList<col_ptr> * args = reinterpret_cast<ArgColList<col_ptr>*>(argPtr);
        args->print();

        const uint64_t selection = args->sel;
        auto xCol = args->xCol;
        auto col = args->col;

        args->downVar--;
        //args->queue[args->threadNum] = true;
        waitAllReady(args->downVar);
        /*args->queue[args->threadNum] = true;
        trace_l(T_INFO, "Set thread number ", args->threadNum, " to true");
        waitAllReady(args->queue);*/

        auto select = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( col, selection);
        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, select);
        auto res = agg_sum<ps, uncompr_f>(projection);

        delete select;
        delete projection;
        delete res;

        return nullptr;
    }

    // Select sum(x) from r where y = c
    template< typename index_structure_ptr >
    Dur runIndexThreads(const column<uncompr_f> * xCol, index_structure_ptr index, const uint64_t selection, uint64_t thread_count)
    {
        numa_run_on_node(0);

        std::vector<pthread_t> thread_ids(thread_count);
        std::atomic<uint64_t> downVar = thread_count;

        /*for (uint64_t i = 0; i < thread_count; i++) {
            readyQueue[i] = false;
        }*/

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
        /*for (uint64_t i = 0; i < thread_count; i++) {
            readyQueue[i] = false;
        }*/

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
    }
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
