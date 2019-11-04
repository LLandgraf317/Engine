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
 * @file select_benchmark_2.cpp
 * @brief Another micro benchmark of the select operator.
 */

#include <core/memory/mm_glob.h>
#include <core/memory/noselfmanaging_helper.h>
#include <core/morphing/delta.h>
#include <core/morphing/dynamic_vbp.h>
#include <core/morphing/for.h>
#include <core/morphing/format.h>
#include <core/morphing/static_vbp.h>
#include <core/morphing/uncompr.h>
#include <core/morphing/vbp.h>
#include <core/operators/general_vectorized/select_compr.h>
//#include <core/operators/scalar/select_uncompr.h>
#include <core/storage/column.h>
#include <core/storage/column_gen.h>
#include <core/utils/basic_types.h>
#include <core/utils/math.h>
#include <core/utils/variant_executor.h>
#include <vector/vector_extension_structs.h>
#include <vector/vector_primitives.h>

#include <functional>
#include <iostream>
#include <limits>
#include <random>
#include <tuple>
#include <vector>

using namespace morphstore;
using namespace vectorlib;


// ----------------------------------------------------------------------------
// Formats
// ----------------------------------------------------------------------------
// All template-specializations of a format are mapped to a name, which may or
// may not contain the values of the template parameters.
        
template<class t_format>
std::string formatName = "(unknown format)";

template<size_t t_BlockSizeLog, size_t t_PageSizeBlocks, unsigned t_Step>
std::string formatName<
        dynamic_vbp_f<t_BlockSizeLog, t_PageSizeBlocks, t_Step>
> = "dynamic_vbp_f<" + std::to_string(t_BlockSizeLog) + ", " + std::to_string(t_PageSizeBlocks) + ", " + std::to_string(t_Step) + ">";

//template<size_t t_BlockSizeLog>
//std::string formatName<k_wise_ns_f<t_BlockSizeLog>> = "k_wise_ns_f<" + std::to_string(t_BlockSizeLog) + ">";

template<unsigned t_Bw, unsigned t_Step>
std::string formatName<
        static_vbp_f<vbp_l<t_Bw, t_Step> >
> = "static_vbp_f<vbp_l<bw, " + std::to_string(t_Step) + "> >";

template<size_t t_BlockSizeLog, unsigned t_Step, class t_inner_f>
std::string formatName<
        delta_f<t_BlockSizeLog, t_Step, t_inner_f>
> = "delta_f<" + std::to_string(t_BlockSizeLog) + ", " + std::to_string(t_Step) + ", " + formatName<t_inner_f> + ">";

template<size_t t_BlockSizeLog, size_t t_PageSizeBlocks, class t_inner_f>
std::string formatName<
        for_f<t_BlockSizeLog, t_PageSizeBlocks, t_inner_f>
> = "for_f<" + std::to_string(t_BlockSizeLog) + ", " + std::to_string(t_PageSizeBlocks) + ", " + formatName<t_inner_f> + ">";

template<>
std::string formatName<uncompr_f> = "uncompr_f";

// ****************************************************************************
// Macros for the variants for variant_executor.
// ****************************************************************************

#define STATIC_VBP_FORMAT(ve, bw) \
    SINGLE_ARG(static_vbp_f<vbp_l<bw, ve::vector_helper_t::element_count::value>>)

#define DYNAMIC_VBP_FORMAT(ve) \
    SINGLE_ARG(dynamic_vbp_f< \
            ve::vector_helper_t::size_bit::value, \
            ve::vector_helper_t::size_byte::value, \
            ve::vector_helper_t::element_count::value \
    >)

#define DELTA_DYNAMIC_VBP_FORMAT(ve) \
    SINGLE_ARG(delta_f< \
            1024, \
            ve::vector_helper_t::element_count::value, \
            dynamic_vbp_f< \
                    ve::vector_helper_t::size_bit::value, \
                    ve::vector_helper_t::size_byte::value, \
                    ve::vector_helper_t::element_count::value \
            > \
    >)

#define FOR_DYNAMIC_VBP_FORMAT(ve) \
    SINGLE_ARG(for_f< \
            1024, \
            ve::vector_helper_t::element_count::value, \
            dynamic_vbp_f< \
                    ve::vector_helper_t::size_bit::value, \
                    ve::vector_helper_t::size_byte::value, \
                    ve::vector_helper_t::element_count::value \
            > \
    >)

#define MAKE_VARIANT(ve, out_pos_f, in_data_f) { \
    new typename t_varex_t::operator_wrapper::template for_output_formats<out_pos_f>::template for_input_formats<in_data_f>( \
        &my_select_wit_t<equal, ve, out_pos_f, in_data_f>::apply \
    ), \
    STR_EVAL_MACROS(ve), \
    formatName<out_pos_f>, \
    formatName<in_data_f> \
}

#if 1
#define MAKE_VARIANTS_VE_OUT(ve, out_pos_f, inBw) \
    MAKE_VARIANT( \
            ve, \
            SINGLE_ARG(out_pos_f), \
            uncompr_f \
    ), \
    MAKE_VARIANT( \
            ve, \
            SINGLE_ARG(out_pos_f), \
            STATIC_VBP_FORMAT(ve, inBw) \
    ), \
    MAKE_VARIANT( \
            ve, \
            SINGLE_ARG(out_pos_f), \
            DYNAMIC_VBP_FORMAT(ve) \
    ), \
    MAKE_VARIANT( \
            ve, \
            SINGLE_ARG(out_pos_f), \
            DELTA_DYNAMIC_VBP_FORMAT(ve) \
    ), \
    MAKE_VARIANT( \
            ve, \
            SINGLE_ARG(out_pos_f), \
            FOR_DYNAMIC_VBP_FORMAT(ve) \
    )

#define MAKE_VARIANTS_VE(ve, outBw, inBw) \
    MAKE_VARIANTS_VE_OUT( \
            ve, \
            uncompr_f, \
            inBw \
    ), \
    MAKE_VARIANTS_VE_OUT( \
            ve, \
            STATIC_VBP_FORMAT(ve, outBw), \
            inBw \
    ), \
    MAKE_VARIANTS_VE_OUT( \
            ve, \
            DYNAMIC_VBP_FORMAT(ve), \
            inBw \
    ), \
    MAKE_VARIANTS_VE_OUT( \
            ve, \
            DELTA_DYNAMIC_VBP_FORMAT(ve), \
            inBw \
    ), \
    MAKE_VARIANTS_VE_OUT( \
            ve, \
            FOR_DYNAMIC_VBP_FORMAT(ve), \
            inBw \
    )
#else
#define MAKE_VARIANTS_VE_OUT(ve, out_pos_f, inBw) \
    MAKE_VARIANT( \
            ve, \
            SINGLE_ARG(out_pos_f), \
            uncompr_f \
    )

#define MAKE_VARIANTS_VE(ve, outBw, inBw) \
    MAKE_VARIANTS_VE_OUT( \
            ve, \
            uncompr_f, \
            inBw \
    )
#endif

template<class t_varex_t, unsigned t_OutBw, unsigned t_InBw>
std::vector<typename t_varex_t::variant_t> make_variants() {
    return {
        // Compressed variants.
        MAKE_VARIANTS_VE(scalar<v64<uint64_t>>, t_OutBw, t_InBw),
#ifdef SSE
        MAKE_VARIANTS_VE(sse<v128<uint64_t>>, t_OutBw, t_InBw),
#endif
#ifdef AVXTWO
        MAKE_VARIANTS_VE(avx2<v256<uint64_t>>, t_OutBw, t_InBw),
#endif
#ifdef AVX512
        MAKE_VARIANTS_VE(avx512<v512<uint64_t>>, t_OutBw, t_InBw),
#endif
    };
}

// ****************************************************************************
// Main program.
// ****************************************************************************

int main(void) {
    // @todo This should not be necessary.
    fail_if_self_managed_memory();
    
    using varex_t = variant_executor_helper<1, 1, uint64_t, size_t>::type
        ::for_variant_params<std::string, std::string, std::string>
        ::for_setting_params<size_t, unsigned, double, uint64_t, uint64_t, uint64_t, uint64_t, double, bool, unsigned>;
    varex_t varex(
            {"pred", "est"},
            {"vector_extension", "out_pos_f", "in_data_f"},
            {"countValues", "outMaxBw", "selectedShare", "mainMin", "mainMax", "outlierMin", "outlierMax", "outlierShare", "isSorted", "inMaxBw"}
    );
    
    const size_t countValues = 128 * 1024 * 1024;
    const unsigned outMaxBw = effective_bitwidth(countValues - 1);
    
    const uint64_t largeVal = bitwidth_max<uint64_t>(63);
    // Looks strange, but saves us from casting in the initializer list.
    const uint64_t _0 = 0;
    const uint64_t _63 = 63;
    
    for(float selectedShare : {
        0.00001,
        0.0001,
        0.001,
        0.01,
        0.1,
        0.25,
        0.5,
        0.75,
        0.9
    }) {
        bool isSorted;
        uint64_t mainMin;
        uint64_t mainMax;
        uint64_t outlierMin;
        uint64_t outlierMax;
        double outlierShare;
        
        for(auto params : {
            // Unsorted, small numbers, no outliers -> good for static_vbp.
            std::make_tuple(false, _0, _63, _0, _0, 0.0),
            // Unsorted, small numbers, many huge outliers -> good for k_wise_ns.
            std::make_tuple(false, _0, _63, largeVal, largeVal, 0.1),
            // Unsorted, small numbers, very rare outliers -> good for dynamic_vbp.
            std::make_tuple(false, _0, _63, largeVal, largeVal, 0.0001),
            // Unsorted, huge numbers in narrow range, no outliers -> good for for+dynamic_vbp.
            std::make_tuple(false, bitwidth_min<uint64_t>(64), bitwidth_min<uint64_t>(64) + 63, _0, _0, 0.0),
            // Sorted, large numbers -> good for delta+dynamic_vbp.
            std::make_tuple(true, _0, bitwidth_max<uint64_t>(24), _0, _0, 0.0),
            // Unsorted, random numbers -> good for nothing/uncompr.
            std::make_tuple(false, _0, std::numeric_limits<uint64_t>::max(), _0, _0, 0.0),
        }) {
            std::tie(isSorted, mainMin, mainMax, outlierMin, outlierMax, outlierShare) = params;
            const unsigned inMaxBw = effective_bitwidth((outlierShare > 0) ? outlierMax : mainMax);

            varex.print_datagen_started();
            auto origCol = generate_with_outliers_and_selectivity(
                    countValues,
                    mainMin, mainMax,
                    selectedShare,
                    outlierMin, outlierMax, outlierShare,
                    isSorted
            );
            varex.print_datagen_done();
            
            std::vector<varex_t::variant_t> variants;
            switch(inMaxBw) {
                // Generated with python:
                // for bw in range(1, 64+1):
                //   print("case {: >2}: variants = make_variants<varex_t, outMaxBw, {: >2}>(); break;".format(bw, bw))
                case  1: variants = make_variants<varex_t, outMaxBw,  1>(); break;
                case  2: variants = make_variants<varex_t, outMaxBw,  2>(); break;
                case  3: variants = make_variants<varex_t, outMaxBw,  3>(); break;
                case  4: variants = make_variants<varex_t, outMaxBw,  4>(); break;
                case  5: variants = make_variants<varex_t, outMaxBw,  5>(); break;
                case  6: variants = make_variants<varex_t, outMaxBw,  6>(); break;
                case  7: variants = make_variants<varex_t, outMaxBw,  7>(); break;
                case  8: variants = make_variants<varex_t, outMaxBw,  8>(); break;
                case  9: variants = make_variants<varex_t, outMaxBw,  9>(); break;
                case 10: variants = make_variants<varex_t, outMaxBw, 10>(); break;
                case 11: variants = make_variants<varex_t, outMaxBw, 11>(); break;
                case 12: variants = make_variants<varex_t, outMaxBw, 12>(); break;
                case 13: variants = make_variants<varex_t, outMaxBw, 13>(); break;
                case 14: variants = make_variants<varex_t, outMaxBw, 14>(); break;
                case 15: variants = make_variants<varex_t, outMaxBw, 15>(); break;
                case 16: variants = make_variants<varex_t, outMaxBw, 16>(); break;
                case 17: variants = make_variants<varex_t, outMaxBw, 17>(); break;
                case 18: variants = make_variants<varex_t, outMaxBw, 18>(); break;
                case 19: variants = make_variants<varex_t, outMaxBw, 19>(); break;
                case 20: variants = make_variants<varex_t, outMaxBw, 20>(); break;
                case 21: variants = make_variants<varex_t, outMaxBw, 21>(); break;
                case 22: variants = make_variants<varex_t, outMaxBw, 22>(); break;
                case 23: variants = make_variants<varex_t, outMaxBw, 23>(); break;
                case 24: variants = make_variants<varex_t, outMaxBw, 24>(); break;
                case 25: variants = make_variants<varex_t, outMaxBw, 25>(); break;
                case 26: variants = make_variants<varex_t, outMaxBw, 26>(); break;
                case 27: variants = make_variants<varex_t, outMaxBw, 27>(); break;
                case 28: variants = make_variants<varex_t, outMaxBw, 28>(); break;
                case 29: variants = make_variants<varex_t, outMaxBw, 29>(); break;
                case 30: variants = make_variants<varex_t, outMaxBw, 30>(); break;
                case 31: variants = make_variants<varex_t, outMaxBw, 31>(); break;
                case 32: variants = make_variants<varex_t, outMaxBw, 32>(); break;
                case 33: variants = make_variants<varex_t, outMaxBw, 33>(); break;
                case 34: variants = make_variants<varex_t, outMaxBw, 34>(); break;
                case 35: variants = make_variants<varex_t, outMaxBw, 35>(); break;
                case 36: variants = make_variants<varex_t, outMaxBw, 36>(); break;
                case 37: variants = make_variants<varex_t, outMaxBw, 37>(); break;
                case 38: variants = make_variants<varex_t, outMaxBw, 38>(); break;
                case 39: variants = make_variants<varex_t, outMaxBw, 39>(); break;
                case 40: variants = make_variants<varex_t, outMaxBw, 40>(); break;
                case 41: variants = make_variants<varex_t, outMaxBw, 41>(); break;
                case 42: variants = make_variants<varex_t, outMaxBw, 42>(); break;
                case 43: variants = make_variants<varex_t, outMaxBw, 43>(); break;
                case 44: variants = make_variants<varex_t, outMaxBw, 44>(); break;
                case 45: variants = make_variants<varex_t, outMaxBw, 45>(); break;
                case 46: variants = make_variants<varex_t, outMaxBw, 46>(); break;
                case 47: variants = make_variants<varex_t, outMaxBw, 47>(); break;
                case 48: variants = make_variants<varex_t, outMaxBw, 48>(); break;
                case 49: variants = make_variants<varex_t, outMaxBw, 49>(); break;
                case 50: variants = make_variants<varex_t, outMaxBw, 50>(); break;
                case 51: variants = make_variants<varex_t, outMaxBw, 51>(); break;
                case 52: variants = make_variants<varex_t, outMaxBw, 52>(); break;
                case 53: variants = make_variants<varex_t, outMaxBw, 53>(); break;
                case 54: variants = make_variants<varex_t, outMaxBw, 54>(); break;
                case 55: variants = make_variants<varex_t, outMaxBw, 55>(); break;
                case 56: variants = make_variants<varex_t, outMaxBw, 56>(); break;
                case 57: variants = make_variants<varex_t, outMaxBw, 57>(); break;
                case 58: variants = make_variants<varex_t, outMaxBw, 58>(); break;
                case 59: variants = make_variants<varex_t, outMaxBw, 59>(); break;
                case 60: variants = make_variants<varex_t, outMaxBw, 60>(); break;
                case 61: variants = make_variants<varex_t, outMaxBw, 61>(); break;
                case 62: variants = make_variants<varex_t, outMaxBw, 62>(); break;
                case 63: variants = make_variants<varex_t, outMaxBw, 63>(); break;
                case 64: variants = make_variants<varex_t, outMaxBw, 64>(); break;
            }

            varex.execute_variants(
                    variants,
                    countValues, outMaxBw, selectedShare, mainMin, mainMax, outlierMin, outlierMax, outlierShare, isSorted, inMaxBw,
                    origCol, mainMin, 0
            );

            delete origCol;
        }
    }
    
    varex.done();
    
    return 0;
}