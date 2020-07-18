#pragma once

#include <iostream>
//#define TRACE_LEVEL @TRACE_LEVEL@

const int T_DEBUG = 0;
const int T_WARN = 1;
const int T_INFO = 2;
const int T_EXIT = 3;
const int T_CRASH = 4;

template<class T, class A>
inline void t1(T& stream, A arg)
{
    stream << arg;
}

template<class T, class A, class ... Types>
inline void t1(T& stream, A arg, Types ... args)
{
    stream << arg;
    t1(stream, args...);
}

template<class ... Types>
inline void trace_l(int level, Types ... args)
{
    if (TRACE_LEVEL < level) {
        std::string prefix = "[Component] ";

        std::cerr << prefix;
        t1(std::cerr, args...);

        std::cerr << std::endl;
    }
}
