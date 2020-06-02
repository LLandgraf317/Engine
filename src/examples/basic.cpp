/**********************************************************************************************
 * Copyright (C) 2019 by MorphStore-Team                                                      *
 *                                                                                            *
 * This file is part of MorphStore - a compression aware vectorized column store.             *
 *                                                                                            *
 * This program is free software: you can redistribute it and/or modify it under the          *
 * terms of the GNU General Public License as published by the Free Software Foundation,      *
 * either version 3 of the License, or (at your option) any later version.                    *
 *                                                                                            *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;  *
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  *
 * See the GNU General Public License for more details.                                       *
 *                                                                                            *
 * You should have received a copy of the GNU General Public License along with this program. *
 * If not, see <http://www.gnu.org/licenses/>.                                                *
 **********************************************************************************************/

/**
 * @brief This file was not automatically generated by mal2morphstore.py.
 */

#include <core/memory/mm_glob.h>
#include <core/morphing/format.h>
#include <core/persistence/binary_io.h>
#include <core/storage/column.h>
#include <core/utils/basic_types.h>
#include <core/utils/printing.h>

#include <vector/vector_extension_structs.h>
#include <vector/vector_primitives.h>

//multiple threads
#include <vector>
#include <numa.h>
#include <pthread.h>
#include <thread>
#include <atomic>

#include <core/morphing/format.h>
#include <core/morphing/uncompr.h>
#include <core/operators/scalar/basic_operators.h>
#include <core/operators/general_vectorized/agg_sum_compr.h>
#include <core/operators/general_vectorized/calc_uncompr.h>
#include <core/operators/general_vectorized/intersect_uncompr.h>
#include <core/operators/general_vectorized/join_compr.h>
#include <core/operators/general_vectorized/project_compr.h>
#include <core/operators/general_vectorized/select_compr.h>
#include <core/utils/column_info.h>
#include <core/storage/column_gen.h>
#include <core/utils/histogram.h>
#include <core/utils/monitoring.h>
#include <functional>
using namespace vectorlib;

#include <iostream>

#include <core/operators/scalar/append.h>

#include <core/storage/replicated_column.h>
#include <abstract/abstract_layer.h>
#include <abstract/stopwatch.h>
#include <abstract/memory_count.h>

using namespace morphstore;

// ****************************************************************************
// Multi-threading global variables
// ****************************************************************************

// Number of concurrent query workers
#ifdef SSB_COUNT_THREADS
size_t countThreads = SSB_COUNT_THREADS;
#else
size_t countThreads = 1;
#endif

// Test runtime
#ifdef SSB_RUNTIME
size_t measuringTimeMillis = SSB_RUNTIME;
#else
size_t measuringTimeMillis = 10000;
#endif

// PCM counters
PCM* m;

std::atomic<size_t> waitingThreads; // = {countThreads};

struct ThreadData {
    pthread_t thread;
    size_t node = 0;
    size_t core = 0;
    size_t nodeSize = 0;
    bool halt = false;

    size_t runsExecuted = 0;
    WallClockStopWatch sw;
};

// Base data declaration
column<uncompr_f> * data;
column<uncompr_f> * indeces;
size_t dataCount;

void* void_updater(void* parameters)
{
    ThreadData* td = (ThreadData*) parameters;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    if (td->core < td->nodeSize)
        CPU_SET(td->core + td->node * td->nodeSize, &cpuset);
    else
        CPU_SET(td->core + td->node * td->nodeSize + td->nodeSize, &cpuset);

    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    numa_set_preferred(td->node);

    waitingThreads--;

    while (waitingThreads > 0)
    {
       //wait for synchronized start
    }

    // do nothing

    return nullptr;
}

void* updater(void* parameters)
{
    ThreadData* td = (ThreadData*) parameters;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    if (td->core < td->nodeSize)
        CPU_SET(td->core + td->node * td->nodeSize, &cpuset);
    else
        CPU_SET(td->core + td->node * td->nodeSize + td->nodeSize, &cpuset);

    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    numa_set_preferred(td->node);

    waitingThreads--;

    while (waitingThreads > 0)
    {
       //wait for synchronized start
    }


    set_signal_handlers();

/// NUMA

    bool show_core_output = true;
    bool show_partial_core_output = false;
    bool show_socket_output = true;
    bool show_system_output = true;
    bool csv_output = false;
    bool reset_pmu = false;
    bool allow_multiple_instances = false;
    bool disable_JKT_workaround = false; // as per http://software.intel.com/en-us/articles/performance-impact-when-sampling-certain-llc-events-on-snb-ep-with-vtune

    std::bitset<MAX_CORES> ycores;


///
    double delay = -1.0;
    bool csv = false, csvheader=false, show_channel_output=true;
    uint32 no_columns = DEFAULT_DISPLAY_COLUMNS; // Default number of columns is 2

    char * sysCmd = NULL;
    char ** sysArgv = NULL;

#ifndef _MSC_VER
    long diff_usec = 0; // deviation of clock is useconds between measurements
    int calibrated = PCM_CALIBRATION_INTERVAL - 2; // keeps track is the clock calibration needed
#endif
    int rankA = -1, rankB = -1;
    bool PMM = true;
    unsigned int numberOfIterations = 0; // number of iterations ?

    

/// 
    std::vector<CoreCounterState> cstates1, cstates2;
    std::vector<SocketCounterState> sktstate1, sktstate2;
    SystemCounterState sstate1, sstate2;
    const auto cpu_model = m->getCPUModel();
    uint64 TimeAfterSleep = 0;
    PCM_UNUSED(TimeAfterSleep);

    if (delay <= 0.0) delay = PCM_DELAY_DEFAULT;

///

    m->disableJKTWorkaround();
    print_cpu_details();
    if (!m->hasPCICFGUncore())
    {
        std::cerr << "Unsupported processor model (" << m->getCPUModel() << ")." << std::endl;
        if (m->memoryTrafficMetricsAvailable())
            cerr << "For processor-level memory bandwidth statistics please use pcm.x" << endl;
        exit(EXIT_FAILURE);
    }

    if(PMM && (m->PMMTrafficMetricsAvailable() == false))
    {
        cerr << "PMM traffic metrics are not available on your processor." << endl;
        exit(EXIT_FAILURE);
    }

    if((rankA >= 0 || rankB >= 0) && PMM)
    {
        cerr << "PMM traffic metrics are not available on rank level" << endl;
        exit(EXIT_FAILURE);
    }

    if((rankA >= 0 || rankB >= 0) && !show_channel_output)
    {
        cerr << "Rank level output requires channel output" << endl;
        exit(EXIT_FAILURE);
    }

    //PCM::ErrorCode status = m->program();
    //PCM::ErrorCode status2 = m->programServerUncoreMemoryMetrics(rankA, rankB, PMM);
/*
    switch (status)
    {
        case PCM::Success:
            break;
        case PCM::MSRAccessDenied:
            cerr << "Access to Processor Counter Monitor has denied (no MSR or PCI CFG space access)." << endl;
            exit(EXIT_FAILURE);
        case PCM::PMUBusy:
            cerr << "Access to Processor Counter Monitor has denied (Performance Monitoring Unit is occupied by other application). Try to stop the application that uses PMU." << endl;
            cerr << "Alternatively you can try to reset PMU configuration at your own risk. Try to reset? (y/n)" << endl;
            char yn;
            std::cin >> yn;
            if ('y' == yn)
            {
                m->resetPMU();
                cerr << "PMU configuration has been reset. Try to rerun the program again." << endl;
            }
            exit(EXIT_FAILURE);
        default:
            cerr << "Access to Processor Counter Monitor has denied (Unknown error)." << endl;
            exit(EXIT_FAILURE);
    }

    switch (status2)
    {
        case PCM::Success:
            break;
        case PCM::MSRAccessDenied:
            cerr << "Access to Processor Counter Monitor has denied (no MSR or PCI CFG space access)." << endl;
            exit(EXIT_FAILURE);
        case PCM::PMUBusy:
            cerr << "Access to Processor Counter Monitor has denied (Performance Monitoring Unit is occupied by other application). Try to stop the application that uses PMU." << endl;
            cerr << "Alternatively you can try to reset PMU configuration at your own risk. Try to reset? (y/n)" << endl;
            char yn;
            std::cin >> yn;
            if ('y' == yn)
            {
                m->resetPMU();
                cerr << "PMU configuration has been reset. Try to rerun the program again." << endl;
            }
            exit(EXIT_FAILURE);
        default:
            cerr << "Access to Processor Counter Monitor has denied (Unknown error)." << endl;
            exit(EXIT_FAILURE);
    }
*/

    if(m->getNumSockets() > max_sockets)
    {
        cerr << "Only systems with up to "<<max_sockets<<" sockets are supported! Program aborted" << endl;
        exit(EXIT_FAILURE);
    }

    ServerUncorePowerState * BeforeState = new ServerUncorePowerState[m->getNumSockets()];
    ServerUncorePowerState * AfterState = new ServerUncorePowerState[m->getNumSockets()];
    uint64 BeforeTime = 0, AfterTime = 0;

    //if ( (sysCmd != NULL) && (delay<=0.0) ) {
        // in case external command is provided in command line, and
        // delay either not provided (-1) or is zero
    //m->setBlocked(true);
    //} else {
        m->setBlocked(false);
    //}

    if (csv) {
        if( delay<=0.0 ) delay = PCM_DELAY_DEFAULT;
    } else {
        // for non-CSV mode delay < 1.0 does not make a lot of practical sense: 
        // hard to read from the screen, or
        // in case delay is not provided in command line => set default
        if( ((delay<1.0) && (delay>0.0)) || (delay<=0.0) ) delay = PCM_DELAY_DEFAULT;
    }

    cerr << "Update every "<<delay<<" seconds"<< endl;

    while (!td->halt)
    {
    for(uint32 i=0; i<m->getNumSockets(); ++i)
        BeforeState[i] = m->getServerUncorePowerState(i); 

    BeforeTime = m->getTickCount();

    if( sysCmd != NULL ) {
        MySystem(sysCmd, sysArgv);
    }
///
    m->getAllCounterStates(sstate1, sktstate1, cstates1);
///
    unsigned int i = 1;

    //while ((i <= numberOfIterations) || (numberOfIterations == 0))
    {
        if(!csv) cout << std::flush;
        int delay_ms = int(delay * 1000);
        int calibrated_delay_ms = delay_ms;

        // compensation of delay on Linux/UNIX
        // to make the samling interval as monotone as possible
        struct timeval start_ts, end_ts;
        if(calibrated == 0) {
            gettimeofday(&end_ts, NULL);
            diff_usec = (end_ts.tv_sec-start_ts.tv_sec)*1000000.0+(end_ts.tv_usec-start_ts.tv_usec);
            calibrated_delay_ms = delay_ms - diff_usec/1000.0;
        }

        MySleepMs(calibrated_delay_ms);

#ifndef _MSC_VER
        calibrated = (calibrated + 1) % PCM_CALIBRATION_INTERVAL;
        if(calibrated == 0) {
            gettimeofday(&start_ts, NULL);
        }
#endif

        AfterTime = m->getTickCount();
        for(uint32 i=0; i<m->getNumSockets(); ++i)
            AfterState[i] = m->getServerUncorePowerState(i);

    if (!csv) {
      //cout << "Time elapsed: "<<dec<<fixed<<AfterTime-BeforeTime<<" ms\n";
      //cout << "Called sleep function for "<<dec<<fixed<<delay_ms<<" ms\n";
    }

        if(rankA >= 0 || rankB >= 0)
          calculate_bandwidth(m,BeforeState,AfterState,AfterTime-BeforeTime,csv,csvheader, no_columns, rankA, rankB);
        else
          calculate_bandwidth(m,BeforeState,AfterState,AfterTime-BeforeTime,csv,csvheader, no_columns, PMM, show_channel_output);

///
        //cerr << "debug: " << m->getNumSockets() << "  " << m->incomingQPITrafficMetricsAvailable() << endl;
        TimeAfterSleep = m->getTickCount();
        m->getAllCounterStates(sstate2, sktstate2, cstates2);

        //if (csv_output)
        //    print_csv(m, cstates1, cstates2, sktstate1, sktstate2, ycores, sstate1, sstate2,
        //    cpu_model, show_core_output, show_partial_core_output, show_socket_output, show_system_output);
        //else
        //{
            print_output(m, cstates1, cstates2, sktstate1, sktstate2, ycores, sstate1, sstate2,
            cpu_model, show_core_output, show_partial_core_output, show_socket_output, show_system_output);
        //}
        swap(sstate1, sstate2);
        swap(sktstate1, sktstate2);
        swap(cstates1, cstates2);
///

        swap(BeforeTime, AfterTime);
        swap(BeforeState, AfterState);

        if ( m->isBlocked() ) {
        // in case PCM was blocked after spawning child application: break monitoring loop here
            break;
        }
    ++i;
    }
    }

    delete[] BeforeState;
    delete[] AfterState;

    return nullptr;
}

void* query(void* parameters)
{
    // ************************************************************************
    // * Preparation of the affiliation setup
    // ************************************************************************

    ThreadData* td = (ThreadData*) parameters;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    if (td->core < td->nodeSize)
        CPU_SET(td->core + td->node * td->nodeSize, &cpuset);
    else
        CPU_SET(td->core + td->node * td->nodeSize + td->nodeSize, &cpuset);

    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    //numa_set_preferred(td->node);
    numa_set_preferred(1);

    const column<uncompr_f> * datat = generate_with_distr(dataCount, std::uniform_int_distribution<uint64_t>(0, dataCount - 1), false);

    waitingThreads--;

    while (waitingThreads > 0)
    {
       //wait for synchronized start
    }

    const column<uncompr_f> * res = nullptr;

    td->sw.start();

    while (!td->halt)
    {
        if (res != nullptr)
        {
            delete res;
        }

        // ************************************************************************
        // * Cyclic query execution
        // ************************************************************************

        // Query program.
        using ps = scalar<v64<uint64_t>>;

        res = sequential_read<ps, uncompr_f>(datat);
        //auto val = sequential_read<ps, uncompr_f>(data);
        //auto val = random_read<ps, uncompr_f>(data, indeces);
        //auto val = sequential_write<ps, uncompr_f>(data);
        //auto val = random_write<ps, uncompr_f>(data, indeces);

        td->runsExecuted++;
        //delete res;
    }

    td->sw.stop();


    //print_columns(print_buffer_base::decimal, res, "SUM(baseCol2)");
    delete res;
    delete datat;

    return nullptr;
}


// ****************************************************************************
// Main program
// ****************************************************************************

int main() {
    // Activate PCM
    m = PCM::getInstance();
    PCM::ErrorCode status = m->program();
//    PCM::ErrorCode status2 = m->programServerUncoreMemoryMetrics(-1, -1, true);

    // Ensure base data local allocation
    numa_available();
    //numa_set_localalloc();
    numa_set_preferred(1);

    // ------------------------------------------------------------------------
    // Loading the base data
    // ------------------------------------------------------------------------

    std::cerr << "Loading the base data started... ";
    std::cerr.flush();

    dataCount = 1000 * 1000 * 100;
    data = generate_with_distr(dataCount, std::uniform_int_distribution<uint64_t>(0, dataCount - 1), false);
    indeces = generate_with_distr(dataCount, std::uniform_int_distribution<uint64_t>(0, dataCount - 1), false);

    std::cerr << "done." << std::endl;

    std::cerr << "Cyclic query execution on the same base data," << std::endl << "started, from 1 to " << countThreads << " thread(s), planned runtime: " << measuringTimeMillis << "ms per experiment... " << std::endl;
    std::cerr << "Threads;RunsPerSecond;CumulativeRuntime;RelativeSpeedup;RelativeSlowdown;TotalRunsDone" << std::endl;
    double singleThreadRuntime = 0;
    double singleThreadRunsPerSecond = 0;

    //for (size_t j = 1; j < countThreads+1; j+=23)
    for (size_t j = 1; j < countThreads+1; j++)
    {
    // ------------------------------------------------------------------------
    // Parallel query execution
    // ------------------------------------------------------------------------
    //waitingThreads = j;
    waitingThreads = j+1;
    std::vector<ThreadData> threadParameters(j);
    // Memory bandwidth snooping thread
    ThreadData memSnoopThread;
    memSnoopThread.core = 47;
    memSnoopThread.nodeSize = numa_num_configured_cpus() / (numa_max_node() + 1) / 2; //if HT is disabled do not divide by 2

    // Specify theads affiliation
    for (size_t i = 0; i < j; i++)
    {
        threadParameters[i].core = i;
        threadParameters[i].nodeSize = numa_num_configured_cpus() / (numa_max_node() + 1) / 2; //if HT is disabled do not divide by 2
    }

    // Create all threads
    for (size_t i = 0; i < j; i++)
    {
        pthread_create(&threadParameters[i].thread, nullptr, query, &threadParameters[i]);
    }
    // Memory bandwidth snooping thread
    pthread_create(&memSnoopThread.thread, nullptr, updater, &memSnoopThread);

    std::this_thread::sleep_for(std::chrono::milliseconds(measuringTimeMillis));

    // Synchronization to stop at a same time
    for (size_t i = 0; i < j; i++)
    {
        threadParameters[i].halt = true;
    }
    memSnoopThread.halt = true;

    for (size_t i = 0; i < j; i++)
    {
        pthread_join(threadParameters[i].thread, nullptr);
    }
    pthread_join(memSnoopThread.thread, nullptr);

    // Statistical calculation
    double cumulativeRuntime = 0;
    double totalRunsExecuted = 0;
    double totalRunsPerSecond = 0;
    for (size_t i = 0; i < j; i++)
    {
        cumulativeRuntime += threadParameters[i].sw.duration();
        totalRunsExecuted += threadParameters[i].runsExecuted;
        totalRunsPerSecond += threadParameters[i].runsExecuted / threadParameters[i].sw.duration();
    }

    if (j == 1)
    {
        singleThreadRuntime = cumulativeRuntime / totalRunsExecuted;
        singleThreadRunsPerSecond = totalRunsPerSecond;
    }
    std::cerr << j << ";" << totalRunsPerSecond << ";" << cumulativeRuntime / totalRunsExecuted << ";" << totalRunsPerSecond / singleThreadRunsPerSecond << ";" << singleThreadRuntime / (cumulativeRuntime / totalRunsExecuted) << ";" << totalRunsExecuted << std::endl;

    }

    delete data;
    delete indeces;

    return 0;
}