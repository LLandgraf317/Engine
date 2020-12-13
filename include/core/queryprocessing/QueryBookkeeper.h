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

class QueryDesc {
public:
    std::vector<std::tuple<ReplicationStatus*,double>> operatorDescs;
    void add(ReplicationStatus * attr, double selectivity)
    {
        operatorDescs.emplace_back(attr, selectivity);
    }
};

class QueryWorkload {
public:
    std::vector<QueryDesc> queries;

    void add(QueryDesc desc)
    {
        queries.push_back(desc);
    }
};

class ReplicationDecision {
public:
    std::vector<
        std::vector< DataStructure >
        > replication;
    uint64_t node_count;
    uint64_t attribute_count;

    ReplicationDecision(uint64_t nodeCount)
    {
        // TODO: For all of your code: use common naming scheme
        node_count = nodeCount;
        replication.resize(nodeCount);
        /*for (uint64_t i = 0; i < nodeCount; i++)
            replication[i](0);*/
    }

    void print()
    {
        for (uint64_t i = 0; i < node_count; i++) {
            std::cout << "Node: " << i << ", attr ds: ";
            uint64_t attr_nr = 0;
            for (auto j : replication[i]) {
                std::cout << "nr: " << attr_nr << " and ds ";
                std::cout << (j == DataStructure::PTREE ? "TREE" : "PCOL") << ",";
            }
            std::cout << std::endl;
        }
    }

    void pushDecision(std::vector< DataStructure > dss)
    {
        assert(dss.size() == node_count);
        attribute_count++;
        replication.emplace_back(dss);
    }

};

class AttributeReplDecision {
public:
    ReplicationStatus* attribute;
    std::vector<DataStructure> dsPerNode;

    void print()
    {
        trace_l(T_INFO, "AttributeReplDecision for attribute ", attribute->getRelation(), ":",
                attribute->getTable(), ",", attribute->getAttribute());
        uint64_t node = 0;
        for (auto i : dsPerNode) {

            trace_l(T_INFO, " uses DS ", i, " for node ", node);
            node++;
        }
    }
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

    std::tuple<double, double> getSelSumInterPreComp( DataStructure ds, Remoteness r, uint64_t columnSize)
    {
        // Data was obtained for 800 MB Cols
        double sizeScale = 1.0 * columnSize / (800 * pow(10,6));

        // Returns c (0) +  m (1) * x
        if (ds == DataStructure::PTREE) {
            if (r == Remoteness::LOCAL) {
                return std::make_tuple(0.00066, sizeScale * 1.92837);
            }
            else { // r == REMOTE
                return std::make_tuple(0.00024, sizeScale * 2.0005);
            }
        }
        else { // ds == DataStructure::PCOLUMN
            if (r == Remoteness::LOCAL) {
                return std::make_tuple(sizeScale * 0.24953, sizeScale * 0.95802);
            }
            else { // r == REMOTE
                return std::make_tuple(sizeScale * 0.2871, sizeScale * 0.973039);
            }
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

    double interMultiColumn(double threadCount)
    {
        return 0.963127 * exp(threadCount * 0.03757);
    }

    double interMultiTree(double threadCount) {
        return 0.96275 * exp(threadCount * 0.03796);
    };

    double execTreeLocal(uint64_t columnSize, double selectivity)
    {
        auto & s = Statistic::getInstance();
        std::tuple<double, double> treeParamsLocal = s.getSelSumInterPreComp(DataStructure::PTREE, Remoteness::LOCAL, columnSize);
        return std::get<1>(treeParamsLocal) * selectivity + std::get<0>(treeParamsLocal);
    };

    double execColLocal(uint64_t columnSize, double selectivity)
    {
        auto & s = Statistic::getInstance();
        std::tuple<double, double> colParamsLocal = s.getSelSumInterPreComp(DataStructure::PCOLUMN, Remoteness::LOCAL, columnSize);
        return std::get<1>(colParamsLocal) * selectivity + std::get<0>(colParamsLocal);
    };

    double execTreeRemote(uint64_t columnSize, double selectivity)
    {
        auto & s = Statistic::getInstance();
        std::tuple<double, double> treeParamsRemote = s.getSelSumInterPreComp(DataStructure::PTREE, Remoteness::REMOTE, columnSize);
        return std::get<1>(treeParamsRemote) * selectivity + std::get<0>(treeParamsRemote);
    };

    double execColRemote(uint64_t columnSize, double selectivity)
    {
        auto & s = Statistic::getInstance();
        std::tuple<double, double> colParamsRemote = s.getSelSumInterPreComp(DataStructure::PCOLUMN, Remoteness::REMOTE, columnSize);
        return std::get<1>(colParamsRemote) * selectivity + std::get<0>(colParamsRemote);
    };

    void printParams(const column<uncompr_f>* col)
    {
        trace_l(T_INFO, "Interpolating parameters");
        uint64_t columnSize = col->get_count_values() * sizeof(uint64_t);
        auto & s = Statistic::getInstance();

        std::tuple<double, double> colParamsLocal = s.getSelSumInterPreComp(DataStructure::PCOLUMN, Remoteness::LOCAL, columnSize);
        std::tuple<double, double> treeParamsLocal = s.getSelSumInterPreComp(DataStructure::PTREE, Remoteness::LOCAL, columnSize);

        std::tuple<double, double> colParamsRemote = s.getSelSumInterPreComp(DataStructure::PCOLUMN, Remoteness::REMOTE, columnSize);
        std::tuple<double, double> treeParamsRemote = s.getSelSumInterPreComp(DataStructure::PTREE, Remoteness::REMOTE, columnSize);

        trace_l(T_INFO, "Interpolation parameters for Select-Sum: ");
        trace_l(T_INFO, "Local column: y = ", std::to_string(std::get<0>(colParamsLocal)), " + sel * ", std::to_string(std::get<1>(colParamsLocal)));
        trace_l(T_INFO, "Local tree: y = ", std::to_string(std::get<0>(treeParamsLocal)), " + sel * ", std::to_string(std::get<1>(treeParamsLocal)));
        trace_l(T_INFO, "Remote column: y = ", std::to_string(std::get<0>(colParamsRemote)), " + sel * ", std::to_string(std::get<1>(colParamsRemote)));
        trace_l(T_INFO, "Remote tree: y = ", std::to_string(std::get<0>(treeParamsRemote)), " + sel * ", std::to_string(std::get<1>(treeParamsRemote)));
        trace_l(T_INFO, "Parallelity scale equation local column: sf(t) = exp( 0.03757 * t ) * 0.963127");
        trace_l(T_INFO, "Parallelity scale equation local tree: sf(t) = exp( 0.03796 * t ) * 0.96275");
    }

    void optimizeDoubleSelectSum(uint64_t numThreads,
                        ReplicationDecision* repl,
                        uint64_t sel0, std::string relation0, std::string table0, std::string attribute0,
                        uint64_t sel1, std::string relation1, std::string table1, std::string attribute1) {

        auto & repl_mgr = ReplicationManager::getInstance();
        const uint64_t countnode = 2;

        auto xStatus = repl_mgr.getStatus(relation0, table0, "x");
        auto yStatus = repl_mgr.getStatus(relation0, table0, attribute0);
        auto zStatus = repl_mgr.getStatus(relation1, table1, attribute1);

        std::vector<const column<uncompr_f>*> xCols;

        std::vector<const column<uncompr_f>*> yCols;
        std::vector<const column<uncompr_f>*> zCols;

        std::vector<pptr<MultiValTreeIndex>> yTrees;
        std::vector<pptr<MultiValTreeIndex>> zTrees;

        for (uint64_t n = 0; n < countnode; n++) {
            xCols.push_back( xStatus->getPersistentColumn(n)->convert() );

            auto yPCol = yStatus->getPersistentColumn(n);
            yCols.push_back(yPCol->convert());

            auto zPCol = zStatus->getPersistentColumn(n);
            zCols.push_back(zPCol->convert());

            yTrees.push_back(yStatus->getMultiValTreeIndex(n));
            zTrees.push_back(zStatus->getMultiValTreeIndex(n));
        }

        trace_l(T_INFO, "Interpolating parameters");
        printParams(yCols[0]);

        trace_l(T_INFO, "Interpolation parameters for Select-Sum: ");
        double prevAbs = std::numeric_limits<double>::max();

        auto buckY = yTrees[0]->find(sel0);
        auto buckZ = zTrees[0]->find(sel1);

        size_t countBuckY = buckY->getCountValues();
        size_t countBuckZ = buckZ->getCountValues();

        uint64_t columnSize = yCols[0]->get_count_values() * sizeof(uint64_t);

        double ySel = (double) countBuckY / yCols[0]->get_count_values();
        double zSel = (double) countBuckZ / zCols[0]->get_count_values();

        trace_l(T_INFO, "Iterating break even points");

        double node0EstFin = std::numeric_limits<double>::max();
        double node1EstFin = std::numeric_limits<double>::max();

        uint64_t threadsNode0;
        uint64_t threadsNode1;

        // Find out break-even point by iteration
        for (uint64_t i = 0; i <= numThreads; i++) {
            double numT0 = (double) i;
            double numT1 = (double) numThreads - i;

            auto yReplDec = repl->replication[0];
            auto zReplDec = repl->replication[1];

            double node0Est = (yReplDec[0] == DataStructure::PTREE ? execTreeLocal(columnSize, ySel) * interMultiTree(numT0) : execColLocal(columnSize, ySel) * interMultiColumn(numT0));
            node0Est += (zReplDec[0] == DataStructure::PTREE ? execTreeLocal(columnSize, zSel) * interMultiTree(numT0) : execColLocal(columnSize, zSel) * interMultiColumn(numT0));
            
            double node1Est = (yReplDec[1] == DataStructure::PTREE ? execTreeLocal(columnSize, ySel) * interMultiTree(numT1) : execColLocal(columnSize, ySel) * interMultiColumn(numT1));
            node1Est += (zReplDec[1] == DataStructure::PTREE ? execTreeLocal(columnSize, zSel) * interMultiTree(numT1) : execColLocal(columnSize, zSel) * interMultiColumn(numT1));

            double res = node1Est + node0Est;
            if (res < prevAbs) {
                prevAbs = res;

                node0EstFin = node0Est;
                node1EstFin = node1Est;

                threadsNode0 = numT0;
                threadsNode1 = numT1;
            }
        }

        trace_l(T_INFO, "Iteration yielded following results: ySel: ", ySel, ", zSel: ", zSel, ", node0Threads: ", threadsNode0, ", node1Threads: ", threadsNode1, ", absPenaltyMin: ", prevAbs);
        trace_l(T_INFO, "Estimated execution time node 0: ", node0EstFin, " seconds");
        trace_l(T_INFO, "Estimated execution time node 1: ", node1EstFin, " seconds");

        std::vector<uint64_t> threadsPerNode;

        threadsPerNode.push_back(threadsNode0);
        threadsPerNode.push_back(threadsNode1);

        QueryCollection qc;

        auto yReplDec = repl->replication[0];
        auto zReplDec = repl->replication[1];

        std::vector<std::tuple<DataStructure, DataStructure>> execUtil;

        // Execute dispatch
        // TODO: memory leaks of args and Query objects
        for (uint64_t n = 0; n < countnode; n++) {
            uint64_t nodeThreads = threadsPerNode[n];

            for (uint64_t i = 0; i < nodeThreads; i++) {
                DoubleSelectSumQuery * q = qc.create<DoubleSelectSumQuery>();

                if (yReplDec[n] == DataStructure::PTREE) {
                    if (zReplDec[n] == DataStructure::PTREE) {

                        DoubleArgList<pptr<MultiValTreeIndex>, pptr<MultiValTreeIndex>> * args
                            = new DoubleArgList<pptr<MultiValTreeIndex>, pptr<MultiValTreeIndex>>(xCols[n], sel0, yTrees[n], sel1, zTrees[n], n, q);
                        execUtil.push_back(std::make_tuple(DataStructure::PTREE, DataStructure::PTREE));

                        q->dispatchAsyncIndInd(args);
                    }
                    else {
                        //(zReplDec[n] == DataStructure::PCOLUMN)
                        DoubleArgList<const column<uncompr_f>*, pptr<MultiValTreeIndex>> * args
                            = new DoubleArgList<const column<uncompr_f>*, pptr<MultiValTreeIndex>>(xCols[n], sel1, zCols[n], sel0, yTrees[n], n, q);
                        execUtil.push_back(std::make_tuple(DataStructure::PTREE, DataStructure::PCOLUMN));

                        q->dispatchAsyncColInd(args);

                    }
                }
                else {
                    //(yReplDec[n] == DataStructure::PCOLUMN) {
                    if (zReplDec[n] == DataStructure::PTREE) {

                        DoubleArgList<const column<uncompr_f>*, pptr<MultiValTreeIndex>> * args
                            = new DoubleArgList<const column<uncompr_f>*, pptr<MultiValTreeIndex>>(xCols[n], sel0, yCols[n], sel1, zTrees[n], n, q);
                        execUtil.push_back(std::make_tuple(DataStructure::PCOLUMN, DataStructure::PTREE));

                        q->dispatchAsyncColInd(args);
                    }
                    else {
                        //(zReplDec[n] == DataStructure::PCOLUMN)
                       
                        DoubleArgList<const column<uncompr_f>*, const column<uncompr_f>* > * args
                            = new DoubleArgList<const column<uncompr_f>*, const column<uncompr_f>*>(xCols[n], sel0, yCols[n], sel1, zCols[n], n, q);
                        execUtil.push_back(std::make_tuple(DataStructure::PCOLUMN, DataStructure::PCOLUMN));

                        q->dispatchAsyncColCol(args);
                    }
                }
            }
        }

        qc.waitAllReady();

        std::vector<Dur> durations = qc.getAllDurations();
        uint64_t threadCount = 0;
        uint64_t node0Threads = threadsPerNode[0];

        for (auto i : durations) {
            std::cout << "EVAL,";
           
            if (threadCount < node0Threads)
                std::cout << "0,";
            else
                std::cout << "1,";
            
            std::cout << ySel << ",";
            std::cout << zSel << ",";

            if (std::get<0>(execUtil[threadCount]) == DataStructure::PTREE)
                std::cout << "TREE,";
            else
                std::cout << "COLUMN,";

            if (std::get<1>(execUtil[threadCount]) == DataStructure::PTREE)
                std::cout << "TREE,";
            else
                std::cout << "COLUMN,";


            std::cout << i.count() << std::endl;
                threadCount++;
        }
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
        printParams(yPColConv);

        // interpolate query execution times using params from previous experiments, hardcoded
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
            double numT0 = (double) i;
            double numT1 = (double) numThreads - i;

            double res = abs(interMultiColumn(numT1) * execColLocal(columnSize, selectivity) + interMultiTree(numT0) * execTreeLocal(columnSize, selectivity));
            if (res < prevAbs) {
                prevAbs = res;
                colThreads = numT1;
                treeThreads = numT0;
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

    void coutDs(DataStructure ds1, DataStructure ds2)
    {
        std::cout << (ds1 == DataStructure::PTREE ? "TRE" : "COL");
        std::cout << (ds2 == DataStructure::PTREE ? "TRE" : "COL");
        std::cout << ",";
    }

    double estimateRuntime(double selectivityY, DataStructure dsY, double selectivityZ, DataStructure dsZ, uint64_t columnSize)
    {
        double node0Est = (dsY == DataStructure::PTREE ? execTreeLocal(columnSize, selectivityY) : execColLocal(columnSize, selectivityY) );
        node0Est += (dsZ == DataStructure::PTREE ? execTreeLocal(columnSize, selectivityZ) : execColLocal(columnSize, selectivityZ) );

        return node0Est;
    }

    void outLoc(uint64_t yLoc, uint64_t zLoc)
    {
        std::cout << (yLoc == 0 ? "LOCAL" : "REMOTE");
        std::cout << ",";
        std::cout << (zLoc == 0 ? "LOCAL" : "REMOTE");
        std::cout << ",";
    }

    void executeAllDoubleSelectSum(
            std::list<AttributeReplDecision*> dec,
            uint64_t ySel, ReplicationStatus* yStatus,
            uint64_t zSel, ReplicationStatus* zStatus)
    {

        auto & repl_mgr = ReplicationManager::getInstance();
        auto xStatus = repl_mgr.getStatus(yStatus->getRelation(), yStatus->getTable(), "x");
        auto xCol = xStatus->getPersistentColumn(0)->convert();

        auto yPCol0 = yStatus->getPersistentColumn(0)->convert();
        auto yTree0 = yStatus->getMultiValTreeIndex(0);

        auto zPCol0 = zStatus->getPersistentColumn(0)->convert();
        auto zTree0 = zStatus->getMultiValTreeIndex(0);

        auto yPCol1 = yStatus->getPersistentColumn(1)->convert();
        auto yTree1 = yStatus->getMultiValTreeIndex(1);

        auto zPCol1 = zStatus->getPersistentColumn(1)->convert();
        auto zTree1 = zStatus->getMultiValTreeIndex(1);

        DataStructure yReplDec;
        DataStructure zReplDec;

        for (auto i : dec) {
            if (i->attribute == yStatus) {
                yReplDec = i->dsPerNode[0];
            }
            if (i->attribute == zStatus)
                zReplDec = i->dsPerNode[0]; 
        }

        auto buckY = yTree0->find(ySel);
        auto buckZ = zTree0->find(zSel);

        size_t countBuckY = buckY->getCountValues();
        size_t countBuckZ = buckZ->getCountValues();

        double selectivityY = (double) countBuckY / yPCol0->get_count_values();
        double selectivityZ = (double) countBuckZ / zPCol0->get_count_values();

        uint64_t columnSize = yPCol0->get_count_values() * sizeof(uint64_t);

        trace_l(T_INFO, "sel y: attr ", ySel, " has ", selectivityY, ", sel z: attr ", zSel, " has ", selectivityZ);
        trace_l(T_INFO, "columnSize: ", columnSize);
        printParams(yPCol0);

        // Print estimate and chosen data structure for replication
        std::cout << "CHOSEN,";
        coutDs(yReplDec, zReplDec);
        outLoc(0,0);
        std::cout << estimateRuntime(selectivityY, yReplDec, selectivityZ, zReplDec, columnSize) << std::endl;

        DataStructure otherY = (yReplDec == DataStructure::PTREE ? DataStructure::PCOLUMN : DataStructure::PTREE);
        DataStructure otherZ = (zReplDec == DataStructure::PTREE ? DataStructure::PCOLUMN : DataStructure::PTREE);
        std::cout << "ELSE,";
        outLoc(0,0);
        coutDs(otherY, zReplDec);
        std::cout << estimateRuntime(selectivityY, otherY, selectivityZ, zReplDec, columnSize) << std::endl;

        std::cout << "ELSE,";
        outLoc(0,0);
        coutDs(yReplDec, otherZ);
        std::cout << estimateRuntime(selectivityY, yReplDec, selectivityZ, otherZ, columnSize) << std::endl;

        std::cout << "ELSE,";
        outLoc(0,0);
        coutDs(otherY, otherZ);
        std::cout << estimateRuntime(selectivityY, otherY, selectivityZ, otherZ, columnSize) << std::endl;

        QueryCollection qc;
        DoubleSelectSumQuery * query = qc.create<DoubleSelectSumQuery>();


        using ColPtr = const column<uncompr_f>*;
        using TreePtr = pptr<MultiValTreeIndex>;

        for (uint64_t yRem = 0; yRem < 2; yRem++) {
            for (uint64_t zRem = 0; zRem < 2; zRem++) {
                ColPtr& yPCol = (yRem == 0 ? yPCol0 : yPCol1);
                TreePtr& yTree = (yRem == 0 ? yTree0 : yTree1);

                ColPtr& zPCol = (zRem == 0 ? zPCol0 : zPCol1);
                TreePtr& zTree = (zRem == 0 ? zTree0 : zTree1);

                DoubleArgList<ColPtr, ColPtr> colcolArgs(xCol, ySel, yPCol, zSel, zPCol, 0, query);
                for (uint64_t i = 0; i < 10; i++) {
                    DoubleSelectSumQuery::runColCol<ColPtr>(&colcolArgs);
                    std::cout << "ITERAT,";
                    coutDs(DataStructure::PCOLUMN, DataStructure::PCOLUMN);
                    outLoc(yRem,zRem);
                    std::cout << query->getExecTime().count() << std::endl;
                    qc.reset();
                }

                DoubleArgList<ColPtr, TreePtr> colTreeArgs(xCol, ySel, yPCol, zSel, zTree, 0, query);
                for (uint64_t i = 0; i < 10; i++) {
                    DoubleSelectSumQuery::runColInd<ColPtr, TreePtr>(&colTreeArgs);
                    std::cout << "ITERAT,";
                    coutDs(DataStructure::PCOLUMN, DataStructure::PTREE);
                    outLoc(yRem,zRem);
                    std::cout << query->getExecTime().count() << std::endl;
                    qc.reset();
                }

                DoubleArgList<ColPtr, TreePtr> treeColArgs(xCol, zSel, zPCol, ySel, yTree, 0, query);
                for (uint64_t i = 0; i < 10; i++) {
                    DoubleSelectSumQuery::runColInd<ColPtr, TreePtr>(&treeColArgs);
                    std::cout << "ITERAT,";
                    coutDs(DataStructure::PTREE, DataStructure::PCOLUMN);
                    outLoc(yRem,zRem);
                    std::cout << query->getExecTime().count() << std::endl;
                    qc.reset();
                }

                DoubleArgList<TreePtr, TreePtr> treeTreeArgs(xCol, ySel, yTree, zSel, zTree, 0, query);
                for (uint64_t i = 0; i < 10; i++) {
                    DoubleSelectSumQuery::runIndInd<TreePtr, TreePtr>(&treeTreeArgs);
                    std::cout << "ITERAT,";
                    coutDs(DataStructure::PTREE, DataStructure::PTREE);
                    outLoc(yRem,zRem);
                    std::cout << query->getExecTime().count() << std::endl;
                    qc.reset();
                }
            }
        }
    }

    void executeAllSelectSum(uint64_t sel, std::string relation, std::string table, std::string attribute)
    {
        //trace_l(T_INFO, "Warm up iteration");
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

        auto yPCol1 = yStatus->getPersistentColumn(node1)->convert();
        auto yTree1 = yStatus->getMultiValTreeIndex(node1);

        uint64_t column_size = yPCol0->get_count_values() * sizeof(uint64_t);
        numa_run_on_node(0);

        auto coldur  = query->runCol  (xCol, yPCol0, sel);
        auto treedur = query->runIndex(xCol, yTree0, sel); 

        auto coldur1  = query->runCol  (xCol, yPCol1, sel);
        auto treedur1 = query->runIndex(xCol, yTree1, sel); 

        stat.log(SSELECTSUM, DataStructure::PCOLUMN, Remoteness::LOCAL, coldur, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PTREE, Remoteness::LOCAL, treedur, sel, column_size);

        stat.log(SSELECTSUM, DataStructure::PCOLUMN, Remoteness::REMOTE, coldur1, sel, column_size);
        stat.log(SSELECTSUM, DataStructure::PTREE, Remoteness::REMOTE, treedur1, sel, column_size);

        delete xCol;
        delete yPCol0;
        delete yPCol1;
    }

};

class Preference {
public:
    ReplicationStatus* m_attr1;
    DataStructure m_ds1;
    ReplicationStatus* m_attr2;
    DataStructure m_ds2;
    
    uint64_t m_count = 0;

    Preference(ReplicationStatus* first, DataStructure ds1, ReplicationStatus* second, DataStructure ds2)
        : m_attr1(first), m_ds1(ds1), m_attr2(second), m_ds2(ds2)
    {
        m_count = 1;       
    }

    void print()
    {
        trace_l(T_INFO, "Preference for attribute1 ", m_attr1->getRelation(), ":", m_attr1->getTable(), ":", m_attr1->getAttribute(), " with Data Structure ", m_ds1);
        trace_l(T_INFO, "Preference for attribute2 ", m_attr2->getRelation(), ":", m_attr2->getTable(), ":", m_attr2->getAttribute(), " with Data Structure ", m_ds2);
        trace_l(T_INFO, "has weight ", m_count);
    }

    bool match(ReplicationStatus* first, DataStructure ds1, ReplicationStatus* second, DataStructure ds2)
    {
        return ((m_attr1 == first && m_attr2 == second) && (m_ds1 == ds1 && m_ds2 == ds2) )
            || ( (m_attr1 == second && m_attr2 == first && m_ds1 == ds2 && m_ds2 == ds1) );
    }

    bool sameAttr(Preference& pref)
    {
        ReplicationStatus* first = pref.m_attr1;
        ReplicationStatus* second = pref.m_attr2;

        return ( first == m_attr1 && second == m_attr2 ) || ( first == m_attr2 && second == m_attr1 );
    }

    void increaseCount()
    {
        m_count++;
    }

    uint64_t getCount()
    {
        return m_count;
    }
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
    using SelectivityVector = std::vector<double>;
    using Workload = std::vector<SelectivityVector>;
    using ShareWorkload = std::vector<SizeShareSelectivityQoSQoS>;


    bool addToPreferences(std::list<Preference>& preferences, ReplicationStatus* attribute1, DataStructure ds1, ReplicationStatus* attribute2, DataStructure ds2)
    {
        for (auto & preference : preferences) {
            if (preference.match(attribute1, ds1, attribute2, ds2)) {
                preference.increaseCount();
                return false;
            }
        }

        return true;
    }

    void constructPreferencesFromQueries(std::list<ReplicationStatus*> attributes, std::list<Preference>& preferences, QueryWorkload* wl, double percBoth)
    {
        for (auto query : wl->queries) {

            std::list<std::tuple<ReplicationStatus*, DataStructure>> correlation;

            for (auto op : query.operatorDescs) {
                ReplicationStatus* attribute = std::get<0>(op);
                double sel = std::get<1>(op);
                DataStructure ds = (sel < percBoth ? DataStructure::PTREE : DataStructure::PCOLUMN);

                correlation.push_back(std::make_tuple(attribute, ds));
            }

            uint64_t it1Count = 0;
            for (auto it1 : correlation) {

                uint64_t it2Count = 0;
                for (auto it2 : correlation) {
                    if (it1Count == it2Count)
                        continue;

                    ReplicationStatus* attribute1 = std::get<0>(it1);
                    ReplicationStatus* attribute2 = std::get<0>(it2);

                    bool found1 = (std::find(attributes.begin(), attributes.end(), attribute1) != attributes.end());
                    if (!found1)
                        attributes.push_back(attribute1);

                    bool found2 = (std::find(attributes.begin(), attributes.end(), attribute2) != attributes.end());
                    if (!found2)
                        attributes.push_back(attribute2);

                    DataStructure ds1 = std::get<1>(it1);
                    DataStructure ds2 = std::get<1>(it2);

                    if (addToPreferences(preferences, attribute1, ds1, attribute2, ds2)) {
                        preferences.emplace_back(attribute1, ds1, attribute2, ds2);
                    }
                    it2Count++;
                }
                it1Count++;
            }
        }
    }

    std::list<AttributeReplDecision*> calculatePlacementForCorrelatedWorkload(uint64_t columnSize, QueryWorkload* wl)
    {
        Statistic & s = Statistic::getInstance();

        std::tuple<double, double> colParamsLocal = s.getSelSumInterPreComp(DataStructure::PCOLUMN, Remoteness::LOCAL, columnSize);
        std::tuple<double, double> treeParamsLocal = s.getSelSumInterPreComp(DataStructure::PTREE, Remoteness::LOCAL, columnSize);

        //std::tuple<double, double> colParamsRemote = s.getSelSumInterPreComp(DataStructure::PCOLUMN, Remoteness::REMOTE, columnSize);
        //std::tuple<double, double> treeParamsRemote = s.getSelSumInterPreComp(DataStructure::PTREE, Remoteness::REMOTE, columnSize);

        double percBoth = (std::get<0>(colParamsLocal) - std::get<0>(treeParamsLocal)) / (std::get<1>(treeParamsLocal) - std::get<1>(colParamsLocal));

        std::list<Preference> preferences;
        std::list<ReplicationStatus*> attributes;

        constructPreferencesFromQueries(attributes, preferences, wl, percBoth);

        // For now, we will not consider continous path because searching for the best would explode, two attributes must suffice for now
        // I did not expect this problem to be this complex, I should have done a math major
        // or at least took graph threory
        uint64_t heaviest = 0;
        uint64_t secondHeaviest = 0;

        ReplicationStatus* attr1 = nullptr;
        ReplicationStatus* attr2 = nullptr;
        ReplicationStatus* secAttr1 = nullptr;
        ReplicationStatus* secAttr2 = nullptr;

        (void) secAttr2;

        DataStructure ds1, ds2;
        DataStructure secDs1, secDs2;

        for (auto& preference0 : preferences) {
            preference0.print();

            if (preference0.getCount() > heaviest) {
                secAttr1 = attr1;
                secAttr2 = attr2;

                attr1 = preference0.m_attr1;
                attr2 = preference0.m_attr2;  

                secDs1 = ds1;
                secDs2 = ds2;

                ds1 = preference0.m_ds1;
                ds2 = preference0.m_ds2;

                secondHeaviest = heaviest;
                heaviest = preference0.getCount();
            }
            else if (preference0.getCount() > secondHeaviest) {
                secAttr1 = preference0.m_attr1;
                secAttr2 = preference0.m_attr2;

                secDs1 = preference0.m_ds1;
                secDs2 = preference0.m_ds2;

                secondHeaviest = preference0.getCount();
            }
        }

        if (preferences.size() == 1) {
            secDs1 = ds1;
            secDs2 = ds2;
        }
        /*class AttributeReplDecision {
        public:
            ReplicationStatus* attribute;
            std::vector<DataStructure> dsPerNode;
        };*/

        std::list<AttributeReplDecision*> ret;

        AttributeReplDecision* dec1 = new AttributeReplDecision();
        AttributeReplDecision* dec2 = new AttributeReplDecision();

        dec1->attribute = attr1;
        dec1->dsPerNode.push_back(ds1);

        dec2->attribute = attr2;
        dec2->dsPerNode.push_back(ds2);

        if (secAttr1 == attr1) {
            dec1->dsPerNode.push_back(secDs1);
            dec2->dsPerNode.push_back(secDs2);
        }
        else {
            dec2->dsPerNode.push_back(secDs1);
            dec1->dsPerNode.push_back(secDs2);
        }
        
        ret.push_back(dec1); 
        ret.push_back(dec2); 
   
        return ret;
    }

    ReplicationDecision* calculatePlacementForWorkload(uint64_t columnSize, Workload& wl)
    {
        Statistic & s = Statistic::getInstance();

        std::tuple<double, double> colParamsLocal = s.getSelSumInterPreComp(DataStructure::PCOLUMN, Remoteness::LOCAL, columnSize);
        std::tuple<double, double> treeParamsLocal = s.getSelSumInterPreComp(DataStructure::PTREE, Remoteness::LOCAL, columnSize);

        std::tuple<double, double> colParamsRemote = s.getSelSumInterPreComp(DataStructure::PCOLUMN, Remoteness::REMOTE, columnSize);
        std::tuple<double, double> treeParamsRemote = s.getSelSumInterPreComp(DataStructure::PTREE, Remoteness::REMOTE, columnSize);

        using TCountBCountDCount = std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>;
        std::vector<TCountBCountDCount> countVector;

        for (auto i : wl) {
            // i is vector of selectivities, implicit number of queries utilizing one attribute

            double percTC = (std::get<0>(colParamsRemote) - std::get<0>(treeParamsLocal)) / (std::get<1>(treeParamsLocal) - std::get<1>(colParamsRemote));
            double percCT = (std::get<0>(colParamsLocal) - std::get<0>(treeParamsRemote)) / (std::get<1>(treeParamsRemote) - std::get<1>(colParamsLocal));
            double percBoth = (std::get<0>(colParamsLocal) - std::get<0>(treeParamsLocal)) / (std::get<1>(treeParamsLocal) - std::get<1>(colParamsLocal));

            trace_l(T_INFO, "Decisionpoint for tree local column remote: ", percTC);
            trace_l(T_INFO, "Decisionpoint for tree remote column local: ", percCT);
            trace_l(T_INFO, "Tendency towards either on: ", percBoth);

            assert(percCT < percTC);

            uint64_t countOnlyTree = 0;
            uint64_t countBothTTend = 0;
            uint64_t countBothCTend = 0;
            uint64_t countOnlyColumn = 0;

            for (auto sel : i) {

                if (sel < percCT) {
                    countOnlyTree++;
                }

                if (sel >= percCT && sel <= percTC) {
                    if (sel < percBoth)
                        countBothTTend++;
                    else 
                        countBothCTend++;
                }

                if (sel > percTC) {
                    countOnlyColumn++;
                }
            }
 
            countVector.emplace_back(countOnlyTree, countBothTTend, countBothCTend, countOnlyColumn);
        }

        uint64_t nodeCount = 2;
        ReplicationDecision * repl = new ReplicationDecision(nodeCount);
        uint64_t node0Acum = 0;
        uint64_t node1Acum = 0;

        for (auto i : countVector) {
            auto countOnlyTree = std::get<0>(i);
            auto countBothTTend = std::get<1>(i);
            auto countBothCTend = std::get<2>(i);
            auto countOnlyColumn = std::get<3>(i);

            if (countOnlyTree > 0 && countOnlyColumn > 0) {
                if (countOnlyTree > countOnlyColumn) {
                    if (node0Acum <= node1Acum) {
                        repl->replication[0].push_back(DataStructure::PTREE);
                        repl->replication[1].push_back(DataStructure::PCOLUMN);

                        node0Acum += countOnlyTree;
                        node1Acum += countOnlyColumn;
                    }
                    else {
                        repl->replication[0].push_back(DataStructure::PCOLUMN);
                        repl->replication[1].push_back(DataStructure::PTREE);

                        node0Acum += countOnlyColumn;
                        node1Acum += countOnlyTree;
                    }
                }
                else {
                    // Column > Tree
                    if (node0Acum <= node1Acum) {
                        repl->replication[0].push_back(DataStructure::PCOLUMN);
                        repl->replication[1].push_back(DataStructure::PTREE);

                        node0Acum += countOnlyColumn;
                        node1Acum += countOnlyTree;
                    }
                    else {
                        repl->replication[0].push_back(DataStructure::PTREE);
                        repl->replication[1].push_back(DataStructure::PCOLUMN);

                        node0Acum += countOnlyTree;
                        node1Acum += countOnlyColumn;
                    }
                }
            }
            else if (countOnlyTree > 0) {
                repl->replication[0].push_back(DataStructure::PTREE);
                repl->replication[1].push_back(DataStructure::PTREE);
            }
            else if (countOnlyColumn > 0) {
                repl->replication[0].push_back(DataStructure::PCOLUMN);
                repl->replication[1].push_back(DataStructure::PCOLUMN);
            }
            else if (countBothTTend > countBothCTend) {
                repl->replication[0].push_back(DataStructure::PTREE);
                repl->replication[1].push_back(DataStructure::PTREE);
            }
            else {
                repl->replication[0].push_back(DataStructure::PCOLUMN);
                repl->replication[1].push_back(DataStructure::PCOLUMN);
            }

        }

        return repl;
    }

    void calculateSharePlacementForWorkload(uint64_t columnSize, ShareWorkload & wl)
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
