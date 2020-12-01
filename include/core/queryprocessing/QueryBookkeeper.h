#pragma once

#include <core/utils/measure.h>
#include <core/replication/ReplicationManager.h>
#include <core/queryprocessing/Query.h>

#include <core/operators/general_vectorized/project_compr.h>
#include <core/operators/general_vectorized/select_compr.h>
#include <core/operators/scalar/agg_sum_uncompr.h>
#include <core/operators/scalar/select_uncompr.h>

#include <numa.h>
#include <vector>
#include <list>
#include <map>

namespace morphstore {

enum Remoteness {
    LOCAL,
    REMOTE
};

enum QueryType {
    SSELECTSUM,
    DSELECTSUM
};

struct DataPoint {
    using Dur = std::chrono::duration<double>;

    DataPoint(QueryType querytype, DataStructure datastruct, Remoteness rem, Dur dur, float sel, uint64_t col_size)
     : qt(querytype), ds(datastruct), r(rem), exec_time(dur), selectivity(sel), column_size(col_size) {}

    QueryType qt;
    DataStructure ds;
    Remoteness r;
    std::chrono::duration<double> exec_time;
    float selectivity;
    uint64_t column_size;
};

class Statistic {
    using Dur = std::chrono::duration<double>;
private:
    std::vector<DataPoint> data;
    Statistic() {}

    double compAvgDuration(const std::list<Dur> & list)
    {
        uint64_t count = 0;
        double sum = 0.0;
        for (auto i : list) {
            count++;
            sum += i.count();
        }

        return sum/count;
    }

public:
    Statistic(Statistic const&)               = delete;
    void operator=(Statistic const&)  = delete;

    static Statistic& getInstance()
    {
        static Statistic instance;

        return instance;
    }

    void log(QueryType querytype, DataStructure datastruct, Remoteness rem, Dur dur, float selectivity, uint64_t column_size)
    {
        data.emplace_back(querytype, datastruct, rem, dur, selectivity, column_size);
    }

    using SelToDurations = std::map<float, std::list<Dur>>;
    std::tuple<double, double> getSelSumInterpolation(DataStructure ds, uint64_t column_size)
    {
        std::map<uint64_t, SelToDurations> colSizeMap;

        // Buildup of data map
        for (auto & i : data) {
            if (i.ds != ds || i.r != Remoteness::LOCAL)
                continue;

            if (colSizeMap.end() == colSizeMap.find(i.column_size)) {
                colSizeMap.emplace(i.column_size, SelToDurations());
            }

            auto & selToDur = colSizeMap[i.column_size];
            if (selToDur.end() == selToDur.find(i.selectivity)) {
                selToDur.emplace(i.selectivity, std::list<Dur>());
            }

            auto & durList = selToDur[i.selectivity];
            durList.push_back(i.exec_time);
        }

        // Interpolation phase
        if (colSizeMap.end() == colSizeMap.find(column_size)) {
            // find largest column size and scale down
            uint64_t maxColSize = 0;
            for (auto const& [colSize, selToDur] : colSizeMap) {
                if (colSize > maxColSize)
                    maxColSize = colSize;
            }

            SelToDurations & selToDur = colSizeMap[maxColSize];
            auto tup = calculateLinearInter( selToDur );

            return tup;
        }
        else {
            // we found a corresponding column size for interpolation
            auto & selToDur = colSizeMap[column_size];

            return calculateLinearInter( selToDur );
        }
    }

    std::tuple<double, double> calculateLinearInter( SelToDurations & selToDur )
    {
        float minSel = 1.0f; // max
        double minDurAvg = 0.0;

        float maxSel = 0.0f; // min
        double maxDurAvg = 0.0;

        for (auto const& [sel, durList] : selToDur) {
            if (sel > maxSel) {
                maxDurAvg = compAvgDuration(durList);
            }
            if (sel < minSel) {
                minDurAvg = compAvgDuration(durList);
            }

        } 

        double m = (maxDurAvg - minDurAvg) / (maxSel - minSel);
        double c = minDurAvg - m * minSel;

        return std::make_tuple(c, m);
    }

};

class Optimizer {
private:
    Optimizer() {}

public:
    Optimizer(Optimizer const&)               = delete;
    void operator=(Optimizer const&)  = delete;

    void optimizeSelectSum(/*uint64_t sel, std::string relation, std::string table, std::string attribute*/) {

    }

    void executeAllSelectSum(uint64_t sel, std::string relation, std::string table, std::string attribute)
    {
        SingleSelectSumQuery query;
        uint64_t node = 0;

        auto & stat = Statistic::getInstance();

        auto & repl_mgr = ReplicationManager::getInstance();
        auto xStatus = repl_mgr.getStatus(relation, table, "X");
        auto yStatus = repl_mgr.getStatus(relation, table, attribute);

        auto xCol = xStatus->getPersistentColumn(node)->convert();
        auto yPCol = yStatus->getPersistentColumn(node)->convert();
        auto yTree = yStatus->getMultiValTreeIndex(node);
        auto yHash = yStatus->getHashMapIndex(node);
        auto ySkip = yStatus->getSkipListIndex(node);

        uint64_t column_size = yPCol->get_count_values() * sizeof(uint64_t);

        auto coldur = query.runCol  (xCol, yPCol, sel);
        auto treedur = query.runIndex(xCol, yTree, sel); 
        auto hashdur = query.runIndex(xCol, yHash, sel);
        auto skipdur = query.runIndex(xCol, ySkip, sel);

        stat.log(SSELECTSUM, DataStructure::PCOLUMN, Remoteness::LOCAL, coldur, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PTREE, Remoteness::LOCAL, treedur, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PHASHMAP, Remoteness::LOCAL, hashdur, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PSKIPLIST, Remoteness::LOCAL, skipdur, sel, column_size);
    }

};

class PlacementAdvisor {
private:
    PlacementAdvisor() {}

public:
    PlacementAdvisor(PlacementAdvisor const&)               = delete;
    void operator=(PlacementAdvisor const&)  = delete;



};

} // namespace morphstore
