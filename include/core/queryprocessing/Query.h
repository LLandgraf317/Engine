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
