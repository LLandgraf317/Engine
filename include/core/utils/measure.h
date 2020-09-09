#pragma once

#include <core/storage/column.h>

#include <iostream>
#include <chrono>

using namespace morphstore;

template<typename ...Ts, typename U>
void measure(const char* /*message*/, U (*function), Ts... args)
{
    auto start = std::chrono::system_clock::now();
    //TODO: delete objects on heap returned by function, might need smart pointers to solve this once and for all
    const column<uncompr_f> * ret = function(args...);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> dur = end - start;

    std::cout << 1.0f/dur.count() << ",";
    delete ret;
}

template<typename ...Ts, typename U>
void measureEnd(const char* /*message*/, U (*function), Ts... args)
{
    auto start = std::chrono::system_clock::now();
    //TODO: delete objects on heap returned by function, might need smart pointers to solve this once and for all
    const column<uncompr_f> * ret = function(args...);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> dur = end - start;

    std::cout << 1.0f/dur.count();

    delete ret;
}

template<typename ...Ts, typename U>
void measureTuple(const char* /*message*/, U (*function), Ts... args)
{
    auto start = std::chrono::system_clock::now();
    //TODO: delete objects on heap returned by function, might need smart pointers to solve this once and for all
    std::tuple<const column<uncompr_f> *, const column<uncompr_f> *> ret = function(args...);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> dur = end - start;

    std::cout << 1.0f/dur.count() << ",";

    delete std::get<0>(ret);
    delete std::get<1>(ret);
}

template<typename ...Ts, typename U>
void measureTupleEnd(const char* /*message*/, U (*function), Ts... args)
{
    auto start = std::chrono::system_clock::now();
    //TODO: delete objects on heap returned by function, might need smart pointers to solve this once and for all
    std::tuple<const column<uncompr_f> *, const column<uncompr_f> *> ret = function(args...);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> dur = end - start;

    std::cout << 1.0f/dur.count() << ",";

    delete std::get<0>(ret);
    delete std::get<1>(ret);
}
