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
                (void) selToDur;
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

    void runNaiveSelectSumTree(uint64_t sel, std::string relation, std::string table, std::string attribute) {
        QueryCollection qc0;

        auto & repl_mgr = ReplicationManager::getInstance();
        auto xStatus = repl_mgr.getStatus(relation, table, "x");
        auto yStatus = repl_mgr.getStatus(relation, table, attribute);

        auto yPCol = yStatus->getPersistentColumn(1);
        auto yPColConv = yPCol->convert();

        auto xCol = xStatus->getPersistentColumn(0)->convert();
        auto yTree0 = yStatus->getMultiValTreeIndex(0);
        auto buck = yTree0->find(sel);
        size_t countBuck = buck->getCountValues();
        double selectivity = (double) countBuck / yPColConv->get_count_values();

        const uint64_t numThreads = 30;

        QueryCollection qc;

        // Execute dispatch
        std::list<ArgList<pptr<MultiValTreeIndex>> *> args;
        for (uint64_t i = 0; i < numThreads; i++) {

            SingleSelectSumQuery * q = qc.create<SingleSelectSumQuery>();
            ArgList<pptr<MultiValTreeIndex>> * treeArgs
                = new ArgList<pptr<MultiValTreeIndex>>(sel, xCol, yTree0, 0, q);

            q->dispatchAsyncIndex(treeArgs);
            args.push_back(treeArgs);
        }

        qc.waitAllReady();

        std::vector<Dur> durations = qc.getAllDurations();
        for (auto i : durations) {
            std::cout << selectivity ;
            std::cout << ",30,NAIVETREE,TREE,";
            std::cout << i.count() << std::endl;
        }

        for (auto i : args)
            delete i;
    }

    void optimizeSelectSum(uint64_t sel, std::string relation, std::string table, std::string attribute) {

        uint64_t node = 0;

        auto & repl_mgr = ReplicationManager::getInstance();
        auto xStatus = repl_mgr.getStatus(relation, table, "x");
        auto yStatus = repl_mgr.getStatus(relation, table, attribute);

        auto xCol = xStatus->getPersistentColumn(node)->convert();
        auto yPCol = yStatus->getPersistentColumn(1);
        auto yPColConv = yPCol->convert();
        auto yTree0 = yStatus->getMultiValTreeIndex(node);
        //auto yHash = yStatus->getHashMapIndex(node);
        //auto ySkip = yStatus->getSkipListIndex(node);

        trace_l(T_INFO, "Interpolating parameters");
        uint64_t columnSize = yPCol->get_count_values() * sizeof(uint64_t);
        auto & s = Statistic::getInstance();
        std::tuple<double, double> colParamsLocal = s.getSelSumInterpolation(DataStructure::PCOLUMN, Remoteness::LOCAL, columnSize);
        std::tuple<double, double> treeParamsLocal = s.getSelSumInterpolation(DataStructure::PTREE, Remoteness::LOCAL, columnSize);

        std::tuple<double, double> colParamsRemote = s.getSelSumInterpolation(DataStructure::PCOLUMN, Remoteness::REMOTE, columnSize);
        std::tuple<double, double> treeParamsRemote = s.getSelSumInterpolation(DataStructure::PTREE, Remoteness::REMOTE, columnSize);

        trace_l(T_INFO, "Interpolation parameters for Select-Sum: ");
        trace_l(T_INFO, "Local column: y = ", std::to_string(std::get<0>(colParamsLocal)), " + sel * ", std::to_string(std::get<1>(colParamsLocal)));
        trace_l(T_INFO, "Local tree: y = ", std::to_string(std::get<0>(treeParamsLocal)), " + sel * ", std::to_string(std::get<1>(treeParamsLocal)));
        trace_l(T_INFO, "Parallelity scale equation local column: sf(t) = exp( 0.03757 * t ) * 0.963127");
        trace_l(T_INFO, "Parallelity scale equation local tree: sf(t) = exp( 0.03796 * t ) * 0.96275");

        (void) colParamsRemote;
        (void) treeParamsRemote; 

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
            return std::get<1>(treeParamsLocal) * selectivity + std::get<0>(treeParamsLocal);
        };

        auto execCol = [&](double selectivity) {
            return std::get<1>(colParamsLocal) * selectivity + std::get<0>(colParamsLocal);
        };

        trace_l(T_INFO, "Interpolation parameters for Select-Sum: ");

        const uint64_t numThreads = 30;

        double prevAbs = std::numeric_limits<double>::max();
        uint64_t colThreads = 0;
        uint64_t treeThreads = 0;

        auto buck = yTree0->find(sel);
        size_t countBuck = buck->getCountValues();

        double selectivity = (double) countBuck / yPColConv->get_count_values();

        trace_l(T_INFO, "Iterating break even points");

        // Find out break-even point by iteration
        for (uint64_t i = 0; i <= numThreads; i++) {
            double numTD = (double) i;
            double res = abs(interMultiColumn(numTD) * execCol(selectivity) - interMultiTree(numTD) * execTree(selectivity));
            if (res < prevAbs) {
                prevAbs = res;
                colThreads = i;
                treeThreads = numThreads - i;
            }
        }

        trace_l(T_INFO, "Iteration yielded following results: selectivity: ", selectivity, ", colThreads: ", colThreads, ", treeThreads: ", treeThreads, ", absPenaltyMin: ", prevAbs);

        QueryCollection qc;

        // Execute dispatch
        std::list<ArgList<const column<uncompr_f> *> *> argsC;
        for (uint64_t i = 0; i < colThreads; i++) {

            SingleSelectSumQuery * q = qc.create<SingleSelectSumQuery>();
            auto * colArgs
                = new ArgList<const column<uncompr_f>*>(sel, xCol, yPColConv, 1, q);

            q->dispatchAsyncColumn(colArgs);
            argsC.push_back(colArgs);
        }

        std::list<ArgList<pptr<MultiValTreeIndex>> *> argsT;
        for (uint64_t i = 0; i < treeThreads; i++) {

            SingleSelectSumQuery * q = qc.create<SingleSelectSumQuery>();
            ArgList<pptr<MultiValTreeIndex>> * treeArgs
                = new ArgList<pptr<MultiValTreeIndex>>(sel, xCol, yTree0, 0, q);

            q->dispatchAsyncIndex(treeArgs);
            argsT.push_back(treeArgs);
        }

        qc.waitAllReady();

        std::vector<Dur> durations = qc.getAllDurations();
        uint64_t colCount = 0;
        for (auto i : durations) {
            std::cout << selectivity << ",30,OPT,";
            if (colCount < colThreads)
                std::cout << "COL,";
            else
                std::cout << "TREE,";
            std::cout << i.count() << std::endl;
            colCount++;
        }

        for (auto i : argsT)
            delete i;
        for (auto i : argsC)
            delete i;
    }

    void executeAllSelectSum(uint64_t sel, std::string relation, std::string table, std::string attribute)
    {
        trace_l(T_INFO, "Warm up iteration");
        QueryCollection qc;
        SingleSelectSumQuery * query = qc.create<SingleSelectSumQuery>();

        uint64_t node = 0;
        uint64_t node1 = 1;

        auto & stat = Statistic::getInstance();

        auto & repl_mgr = ReplicationManager::getInstance();
        auto xStatus = repl_mgr.getStatus(relation, table, "x");
        auto yStatus = repl_mgr.getStatus(relation, table, attribute);

        auto xCol = xStatus->getPersistentColumn(node)->convert();
        auto yPCol0 = yStatus->getPersistentColumn(node)->convert();
        auto yTree0 = yStatus->getMultiValTreeIndex(node);
        /*auto yHash0 = yStatus->getHashMapIndex(node);
        auto ySkip0 = yStatus->getSkipListIndex(node);*/

        auto yPCol1 = yStatus->getPersistentColumn(node1)->convert();
        auto yTree1 = yStatus->getMultiValTreeIndex(node1);
        /*auto yHash1 = yStatus->getHashMapIndex(node1);
        auto ySkip1 = yStatus->getSkipListIndex(node1);*/

        uint64_t column_size = yPCol0->get_count_values() * sizeof(uint64_t);

        auto coldur  = query->runCol  (xCol, yPCol0, sel);
        auto treedur = query->runIndex(xCol, yTree0, sel); 
        /*auto hashdur = query->runIndex(xCol, yHash0, sel);
        auto skipdur = query->runIndex(xCol, ySkip0, sel);*/

        auto coldur1  = query->runCol  (xCol, yPCol1, sel);
        auto treedur1 = query->runIndex(xCol, yTree1, sel); 
        /*auto hashdur1 = query->runIndex(xCol, yHash1, sel);
        auto skipdur1 = query->runIndex(xCol, ySkip1, sel);*/

        stat.log(SSELECTSUM, DataStructure::PCOLUMN, Remoteness::LOCAL, coldur, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PTREE, Remoteness::LOCAL, treedur, sel, column_size);
        /*stat.log(SSELECTSUM, DataStructure::PHASHMAP, Remoteness::LOCAL, hashdur, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PSKIPLIST, Remoteness::LOCAL, skipdur, sel, column_size);*/

        stat.log(SSELECTSUM, DataStructure::PCOLUMN, Remoteness::REMOTE, coldur1, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PTREE, Remoteness::REMOTE, treedur1, sel, column_size);
        /*stat.log(SSELECTSUM, DataStructure::PHASHMAP, Remoteness::REMOTE, hashdur1, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PSKIPLIST, Remoteness::REMOTE, skipdur1, sel, column_size);*/

        delete xCol;
        delete yPCol0;
        delete yPCol1;
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
