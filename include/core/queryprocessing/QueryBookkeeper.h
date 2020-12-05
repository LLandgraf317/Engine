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
#include <limits>

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
        trace_l(T_DEBUG, "querytype: ", querytype, ", datastruct: ", datastruct, ", remoteness: ", rem, ", duration: ", dur.count(), "s, selectivity: ", selectivity, ", column size: ", column_size, "b");
        data.emplace_back(querytype, datastruct, rem, dur, selectivity, column_size);
    }

    using SelToDurations = std::map<float, std::list<Dur>>;
    std::tuple<double, double> getSelSumInterpolation(DataStructure ds, Remoteness r, uint64_t column_size)
    {
        std::map<uint64_t, SelToDurations> colSizeMap;

        // Buildup of data map
        for (auto & i : data) {
            if (i.ds != ds || i.r != r)
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
    using Dur = std::chrono::duration<double>;
private:
    Optimizer() {}

public:
    Optimizer(Optimizer const&)               = delete;
    void operator=(Optimizer const&)  = delete;

    static Optimizer & getInstance()
    {
        static Optimizer opt;
        return opt;
    }

    void optimizeSelectSum(uint64_t sel, std::string relation, std::string table, std::string attribute) {

        QueryCollection qc0;
        SingleSelectSumQuery * query = qc0.create<SingleSelectSumQuery>();

        uint64_t node = 0;

        auto & repl_mgr = ReplicationManager::getInstance();
        auto xStatus = repl_mgr.getStatus(relation, table, "X");
        auto yStatus = repl_mgr.getStatus(relation, table, attribute);

        auto xCol = xStatus->getPersistentColumn(node)->convert();
        auto yPCol = yStatus->getPersistentColumn(node);
        auto yPColConv = yStatus->getPersistentColumn(node)->convert();
        auto yTree = yStatus->getMultiValTreeIndex(node);
        //auto yHash = yStatus->getHashMapIndex(node);
        //auto ySkip = yStatus->getSkipListIndex(node);

        trace_l(T_INFO, "Interpolating parameters");
        uint64_t columnSize = yPCol->get_count_values() * sizeof(uint64_t);
        auto & s = Statistic::getInstance();
        std::tuple<double, double> colParamsLocal = s.getSelSumInterpolation(DataStructure::PCOLUMN, Remoteness::LOCAL, columnSize);
        std::tuple<double, double> treeParamsLocal = s.getSelSumInterpolation(DataStructure::PTREE, Remoteness::LOCAL, columnSize);

        std::tuple<double, double> colParamsRemote = s.getSelSumInterpolation(DataStructure::PCOLUMN, Remoteness::REMOTE, columnSize);
        std::tuple<double, double> treeParamsRemote = s.getSelSumInterpolation(DataStructure::PTREE, Remoteness::REMOTE, columnSize);

        (void) colParamsRemote;
        (void) treeParamsRemote; 

        auto coldur = query->runCol  (xCol, yPColConv, sel);
        auto treedur = query->runIndex(xCol, yTree, sel); 
        //auto hashdur = query.runIndex(xCol, yHash, sel);
        //auto skipdur = query.runIndex(xCol, ySkip, sel);
        //
        // interpolate query execution times using params from previous experiments, hardcoded
        auto interMultiColumn = [](double t) {
            return 0.963127 * exp(t * 0.03757);
        };

        auto interMultiTree = [](double t) {
            return 0.96275 * exp(t * 0.03796);
        };

        auto execTree = [&](double selectivity) {
            return std::get<0>(treeParamsLocal) * selectivity + std::get<1>(treeParamsLocal);
        };

        auto execCol = [&](double selectivity) {
            return std::get<0>(colParamsLocal) * selectivity + std::get<1>(colParamsLocal);
        };

        const uint64_t numThreads = 50;

        double prevAbs = std::numeric_limits<double>::max();
        uint64_t colThreads = 0;
        uint64_t treeThreads = 0;
        double selectivity = 0.3;

        trace_l(T_INFO, "Iterating break even points");

        // Find out break-even point by iteration
        for (uint64_t i = 0; i <= numThreads; i++) {
            double numTD = (double) i;
            double res = interMultiColumn(numTD) * execCol(selectivity) - interMultiTree(numTD) * execTree(selectivity); 
            if (res < prevAbs) {
                prevAbs = res;
                colThreads = i;
                treeThreads = numThreads - i;
            }
        }

        QueryCollection qc;

        // Execute dispatch
        for (uint64_t i = 0; i < colThreads; i++) {

            SingleSelectSumQuery * q = qc.create<SingleSelectSumQuery>();
            auto * colArgs
                = new ArgList<const column<uncompr_f>*>(sel, xCol, yPColConv, 0, q);

            q->dispatchAsyncColumn(colArgs);
        }
        for (uint64_t i = 0; i < treeThreads; i++) {

            SingleSelectSumQuery * q = qc.create<SingleSelectSumQuery>();
            ArgList<pptr<MultiValTreeIndex>> * treeArgs
                = new ArgList<pptr<MultiValTreeIndex>>(sel, xCol, yTree, 1, q);

            q->dispatchAsyncIndex(treeArgs);
        }

        qc.waitAllReady();

        std::vector<Dur> durations = qc.getAllDurations();
        for (auto i : durations)
            std::cout << "Duration: " << i.count() << std::endl;
    }

    void executeAllSelectSum(uint64_t sel, std::string relation, std::string table, std::string attribute)
    {
        trace_l(T_INFO, "Warm up iteration");
        QueryCollection qc;
        SingleSelectSumQuery * query = qc.create<SingleSelectSumQuery>();

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

        auto coldur  = query->runCol  (xCol, yPCol, sel);
        auto treedur = query->runIndex(xCol, yTree, sel); 
        auto hashdur = query->runIndex(xCol, yHash, sel);
        auto skipdur = query->runIndex(xCol, ySkip, sel);

        stat.log(SSELECTSUM, DataStructure::PCOLUMN, Remoteness::LOCAL, coldur, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PTREE, Remoteness::LOCAL, treedur, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PHASHMAP, Remoteness::LOCAL, hashdur, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PSKIPLIST, Remoteness::LOCAL, skipdur, sel, column_size);

        delete xCol;
        delete yPCol;
        delete query;
    }

};

enum Placement {
    COLCOL,
    DATASCOL,
    COLDATAS,
    DATASDATAS
};

class PlacementAdvisor {
private:
    PlacementAdvisor() {}

public:
    PlacementAdvisor(PlacementAdvisor const&)               = delete;
    void operator=(PlacementAdvisor const&)  = delete;

    static PlacementAdvisor& getInstance()
    {
        static PlacementAdvisor ad;
        return ad;
    }

    using SizeShareSelectivityQoSQoS = std::tuple<uint64_t, double, double, double, double>;
    using Workload = std::vector<SizeShareSelectivityQoSQoS>;

    void calculatePlacementForWorkload(uint64_t columnSize, Workload & wl)
    {
        Statistic & s = Statistic::getInstance();

        std::tuple<double, double> colParamsLocal = s.getSelSumInterpolation(DataStructure::PCOLUMN, Remoteness::LOCAL, columnSize);
        std::tuple<double, double> treeParamsLocal = s.getSelSumInterpolation(DataStructure::PTREE, Remoteness::LOCAL, columnSize);

        std::tuple<double, double> colParamsRemote = s.getSelSumInterpolation(DataStructure::PCOLUMN, Remoteness::REMOTE, columnSize);
        std::tuple<double, double> treeParamsRemote = s.getSelSumInterpolation(DataStructure::PTREE, Remoteness::REMOTE, columnSize);
        //std::tuple<double, double> hashParams;
        //std::tuple<double, double> skipParams;

        double sumSecondsCC = 0.0;
        double sumSecondsDC = 0.0;
        double sumSecondsCD = 0.0;
        double sumSecondsDD = 0.0;

        for (auto i : wl) {
            //uint64_t colSize = std::get<0>(i);
            double share = std::get<1>(i);
            double sel = std::get<2>(i);
            double qos0 = std::get<3>(i);
            double qos1 = std::get<4>(i);
            
            double secondsCLocal = sel * std::get<1>(colParamsLocal) + std::get<0>(colParamsLocal);
            double secondsDLocal = sel * std::get<1>(treeParamsLocal) + std::get<0>(treeParamsLocal);

            double secondsCRemote = sel * std::get<1>(colParamsRemote) + std::get<0>(colParamsRemote);
            double secondsDRemote = sel * std::get<1>(treeParamsRemote) + std::get<0>(treeParamsRemote);

            //sumSecondsCC += share * ( prob0 * secondsCLocal + (1-prob) * secondsCRemote );
            sumSecondsCC += share * secondsCLocal;

            if ( secondsCLocal > secondsDLocal ) {
                double partialDC0 = qos0 * secondsDLocal;
                double partialDC1 = ( secondsDRemote > secondsCLocal ? ( 1 - qos0 ) * secondsCLocal : ( 1 - qos0 ) * secondsDRemote );

                double partialCD0 = qos1 * secondsDLocal;
                double partialCD1 = ( secondsDRemote > secondsCLocal ? ( 1 - qos1 ) * secondsCLocal : ( 1 - qos1 ) * secondsDRemote );

                sumSecondsDC += share * ( partialDC0 + partialDC1 );
                sumSecondsCD += share * ( partialCD0 + partialCD1 );
            }
            else {
                double partialDC0 = qos1 * secondsCLocal;
                double partialDC1 = ( secondsCRemote > secondsDLocal ? ( 1 - qos1 ) * secondsDLocal : ( 1 - qos1 ) * secondsCRemote );

                double partialCD0 = qos0 * secondsCLocal;
                double partialCD1 = ( secondsDLocal > secondsCRemote ? ( 1 - qos0 ) * secondsCRemote : ( 1 - qos0 ) * secondsDLocal );

                sumSecondsDC += share * ( partialDC0 + partialDC1 );
                sumSecondsCD += share * ( partialCD0 + partialCD1 );
            }

            sumSecondsDD += share * secondsDLocal;
        }

        std::cout << "Got placement: " << std::endl; 
        if (sumSecondsCC < sumSecondsDD && sumSecondsCC < sumSecondsDC)
            std::cout << "Column Column" << std::endl;

        if (sumSecondsDC < sumSecondsDD && sumSecondsDC < sumSecondsCC)
            std::cout << "Tree Column" << std::endl;

        if (sumSecondsDD < sumSecondsDC && sumSecondsDD < sumSecondsCC)
            std::cout << "Tree Tree" << std::endl;
    }



};

} // namespace morphstore
