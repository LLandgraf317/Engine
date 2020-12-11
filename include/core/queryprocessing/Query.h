#pragma once

#include <core/operators/general_vectorized/project_compr.h>
#include <core/operators/general_vectorized/select_compr.h>
#include <core/operators/scalar/agg_sum_uncompr.h>
#include <core/operators/scalar/select_uncompr.h>
#include <core/operators/general_vectorized/intersect_uncompr.h>

#include <core/utils/measure.h>

#include <atomic>


namespace morphstore {

using namespace vectorlib;
using ps = scalar<v64<uint64_t>>;

class ExecutionTracker {
    using Dur = std::chrono::duration<double>;
private:
    std::chrono::duration<double> exec_time;
    std::chrono::time_point<std::chrono::system_clock> starttime;
    std::chrono::time_point<std::chrono::system_clock> endtime;
public:
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

class Query {

    using Dur = std::chrono::duration<double>;
private:
    ExecutionTracker exTr;

protected:
    pthread_t thread_id;
    std::atomic<uint64_t>& downVar;
    bool ready = false;
    void * argListPtr;

public:
    Query(std::atomic<uint64_t>& dv)
        : downVar(dv) 
    {
    }

    static void waitAllReady(std::atomic<uint64_t>& down) //std::vector<bool>& queue)
    {
        while (down > 0ul) {
        }
    }

    inline void start()
    {
        exTr.start();
    }

    inline void end()
    {
        exTr.end();
    }

    Dur getExecTime()
    {
        return exTr.getExecTime();
    }

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
    using Dur = std::chrono::duration<double>;
private:
    std::atomic<uint64_t> downVar = 0;
    std::vector<Query*> queries;

public:
    ~QueryCollection() {
        /*for (auto i : queries)
            delete i;*/
    }

    void reset()
    {
        downVar = queries.size();
    }

    void pushQuery(Query * query)
    {
        downVar++;
        queries.push_back(query);
    }

    void waitAllReady() {
        while (downVar != 0) {}

        for (auto i : queries) {
            //trace_l(T_INFO, "Joining thread ", i->getThread());
            pthread_join(i->getThread(), nullptr);
        }
    }

    template< typename query_type >
    query_type * create()
    {
        auto q = new query_type(downVar);
        pushQuery(q);

        return q;
    }

    std::vector<Dur> getAllDurations()
    {
        std::vector<Dur> durs;

        for (auto i : queries)
            durs.push_back(i->getExecTime());
        
        return durs;
    }
};

template< typename data_struct_ptr >
struct ArgList {
    using Dur = std::chrono::duration<double>;
private:
    std::chrono::time_point<std::chrono::system_clock> starttime;
    std::chrono::time_point<std::chrono::system_clock> endtime;

public:
    ArgList(uint64_t selection, const column<uncompr_f> * col, data_struct_ptr index, const uint64_t node, Query * q) //std::vector<bool>& readyQueue)
        : sel(selection), xCol(col), datastruct(index), runNode(node), query(q) {
        }

    const uint64_t sel;
    const column<uncompr_f> * xCol;
    data_struct_ptr datastruct;
    const uint64_t runNode;
    Query * query;

    std::atomic<uint64_t> * downVar;
};

class SingleSelectSumQuery : public Query {
    using Dur = std::chrono::duration<double>;
public:
    SingleSelectSumQuery(std::atomic<uint64_t>& down) : Query(down) {}

    template< typename index_structure_ptr >
    Dur runIndex(const column<uncompr_f> * xCol, index_structure_ptr index, const uint64_t selection)
    {
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

        numa_run_on_node(args->runNode);

        const uint64_t selection = args->sel;
        auto xCol = args->xCol;
        auto index = args->datastruct;

        (*args->downVar)--;
        //uint64_t a = (*args->downVar);
        //trace_l(T_DEBUG, "Spin var is now ", a);
        waitAllReady(*args->downVar);

        args->query->start();
        auto select = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( index, selection);
        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, select);

        auto res = agg_sum<ps, uncompr_f>(projection);
        args->query->end();

        delete select;
        delete projection;
        delete res;

        //trace_l(T_DEBUG, "Done.");

        return nullptr;
    }

    template< typename col_ptr >
    static void * runColPT(void * argPtr)
    {
        ArgList<col_ptr> * args = reinterpret_cast<ArgList<col_ptr>*>(argPtr);

        numa_run_on_node(args->runNode);
        const uint64_t selection = args->sel;
        auto xCol = args->xCol;
        auto yCol = args->datastruct;

        (*args->downVar)--;
        waitAllReady(*args->downVar);

        args->query->start();
        auto select = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( yCol, selection);
        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, select);
        auto res = agg_sum<ps, uncompr_f>(projection);
        args->query->end();

        delete select;
        delete projection;
        delete res;


        return nullptr;
    }

    template< typename index_structure_ptr >
    void dispatchAsyncIndex(ArgList< index_structure_ptr > * argList )
    {
        argList->downVar = &downVar;
        argList->query = this;

        pthread_create(&thread_id, nullptr, SingleSelectSumQuery::runIndexPT<index_structure_ptr>, reinterpret_cast<void*>(argList));
    }

    template< typename col_ptr >
    void dispatchAsyncColumn(ArgList< col_ptr > * argList )
    {
        argList->downVar = &downVar;
        argList->query = this;

        pthread_create(&thread_id, nullptr, SingleSelectSumQuery::runColPT< col_ptr >, reinterpret_cast<void*>(argList));
    }

};

template< typename data_structure_ptr0, typename data_structure_ptr1 >
struct DoubleArgList {
    using Dur = std::chrono::duration<double>;
private:
    std::chrono::time_point<std::chrono::system_clock> starttime;
    std::chrono::time_point<std::chrono::system_clock> endtime;

public:
    DoubleArgList(const column<uncompr_f> * col, uint64_t selection0, data_structure_ptr0 index0, uint64_t selection1, data_structure_ptr1 index1, const uint64_t node, Query * q) //std::vector<bool>& readyQueue)
        : xCol(col), sel0(selection0), datastruct0(index0), sel1(selection1), datastruct1(index1), runNode(node), query(q) {
        }

    const column<uncompr_f> * xCol;

    const uint64_t sel0;
    data_structure_ptr0 datastruct0;
    const uint64_t sel1;
    data_structure_ptr1 datastruct1;

    const uint64_t runNode;
    Query * query;

    std::atomic<uint64_t> * downVar;
};

class DoubleSelectSumQuery : public Query {
public:
    DoubleSelectSumQuery(std::atomic<uint64_t>& down) : Query(down) {}

    template< typename index_structure_ptr0, typename index_structure_ptr1 >
    static void * runIndInd(void * argPtr) //const column<uncompr_f> * xCol, index_structure_ptr0 index0, index_structure_ptr1 index1, const uint64_t selection1, const uint64_t selection2)
    {
        DoubleArgList< index_structure_ptr0, index_structure_ptr1> * args = reinterpret_cast<DoubleArgList<index_structure_ptr0, index_structure_ptr1>*>(argPtr);

        numa_run_on_node(args->runNode);
        const uint64_t selection0 = args->sel0;
        const uint64_t selection1 = args->sel1;
        auto xCol = args->xCol;
        auto index0 = args->datastruct0;
        auto index1 = args->datastruct1;

        (*args->downVar)--;
        //waitAllReady(*args->downVar);

        args->query->start();
        auto select1 = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr0, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( index0, selection0);
        auto select2 = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr1, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( index1, selection1);
        auto intersect = intersect_sorted<ps, uncompr_f, uncompr_f, uncompr_f >(select1, select2);

        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, intersect);
        auto res = agg_sum<ps, uncompr_f>(projection);
        args->query->end();

        delete select1;
        delete select2;
        delete intersect;
        delete projection;
        delete res;

        return nullptr;
    }

    template< typename col_ptr, typename index_structure_ptr >
    static void * runColInd(void * argPtr)
            //const column<uncompr_f> * xCol, col_ptr col0, index_structure_ptr index1, const uint64_t selection1, const uint64_t selection2)
    {
        DoubleArgList< col_ptr, index_structure_ptr> * args = reinterpret_cast<DoubleArgList<col_ptr, index_structure_ptr>*>(argPtr);

        numa_run_on_node(args->runNode);
        const uint64_t selection0 = args->sel0;
        const uint64_t selection1 = args->sel1;
        auto xCol = args->xCol;
        auto col0 = args->datastruct0;
        auto index1 = args->datastruct1;

        (*args->downVar)--;
        //waitAllReady(*args->downVar);

        args->query->start();
        auto select1 = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( col0, selection0);
        auto select2 = index_select_wit_t<std::equal_to, uncompr_f, uncompr_f,
            index_structure_ptr, persistent_ptr<NodeBucketList<uint64_t, OSP_SIZE>>>
                ::apply( index1, selection1);
        auto intersect = intersect_sorted<ps, uncompr_f, uncompr_f, uncompr_f >(select1, select2);

        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, intersect);
        auto res = agg_sum<ps, uncompr_f>(projection);
        args->query->end();

        delete select1;
        delete select2;
        delete intersect;
        delete projection;
        delete res;

        return nullptr;
    }

    template< typename col_ptr >
    static void * runColCol(void * argPtr)
            //const column<uncompr_f> * xCol, col_ptr yCol, col_ptr zCol, const uint64_t selection1, const uint64_t selection2)
    {
        DoubleArgList< col_ptr, col_ptr> * args = reinterpret_cast<DoubleArgList<col_ptr, col_ptr>*>(argPtr);

        numa_run_on_node(args->runNode);
        const uint64_t selection0 = args->sel0;
        const uint64_t selection1 = args->sel1;
        auto xCol = args->xCol;
        auto col0 = args->datastruct0;
        auto col1 = args->datastruct1;

        (*args->downVar)--;
        //waitAllReady(*args->downVar);

        args->query->start();
        auto select1 = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( col0, selection0);
        auto select2 = my_select_wit_t<equal, ps, uncompr_f, uncompr_f>
                ::apply( col1, selection1);
        auto intersect = intersect_sorted<ps, uncompr_f, uncompr_f, uncompr_f >(select1, select2);

        auto projection = my_project_wit_t<ps, uncompr_f, uncompr_f, uncompr_f >::apply(xCol, intersect);
        auto res = agg_sum<ps, uncompr_f>(projection);
        args->query->end();

        delete select1;
        delete select2;
        delete intersect;
        delete projection;
        delete res;

        return nullptr;
    }

    template< typename data_structure_ptr0, typename data_structure_ptr1 >
    void dispatchAsyncIndInd(DoubleArgList< data_structure_ptr0, data_structure_ptr1 > * argList )
    {
        argList->downVar = &downVar;
        argList->query = this;

        pthread_create(&thread_id, nullptr, DoubleSelectSumQuery::runIndInd<data_structure_ptr0, data_structure_ptr1>, reinterpret_cast<void*>(argList));
    }

    template< typename col_ptr >
    void dispatchAsyncColCol(DoubleArgList< col_ptr, col_ptr > * argList )
    {
        argList->downVar = &downVar;
        argList->query = this;

        pthread_create(&thread_id, nullptr, DoubleSelectSumQuery::runColCol< col_ptr>, reinterpret_cast<void*>(argList));
    }

    template< typename col_ptr, typename index_structure_ptr >
    void dispatchAsyncColInd(DoubleArgList< col_ptr, index_structure_ptr > * argList )
    {
        argList->downVar = &downVar;
        argList->query = this;

        pthread_create(&thread_id, nullptr, DoubleSelectSumQuery::runColInd< col_ptr,index_structure_ptr >, reinterpret_cast<void*>(argList));
    }


};

}
