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
 * @file variant_executor.h
 * @brief 
 * @todo Documentation.
 * @todo Support for repetitions.
 * @todo Rethink where the cached columns should be freed.
 */

#ifndef MORPHSTORE_CORE_UTILS_VARIANT_EXECUTOR_H
#define MORPHSTORE_CORE_UTILS_VARIANT_EXECUTOR_H

#include <core/morphing/format.h>
#include <core/morphing/morph.h>
#include <core/storage/column.h>
#include <core/utils/basic_types.h>
#include <core/utils/column_cache.h>
#include <core/utils/equality_check.h>
#include <core/utils/monitoring.h>
#include <core/utils/preprocessor.h>
#include <core/utils/processing_style.h>
#include <core/utils/variadic.h>

#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace morphstore {
    
    class variant_executor {
        static const std::string m_CsvColNameRuntime;
        static const std::string m_CsvColNameCheck;
        
    public:
        // These must all be uncompr_f, but their number matters.
        template<class ... t_uncompr_out_fs>
        class for_uncompr_output_formats {
    
            template<size_t ... t_Idxs>
            static bool check_uncompr_column_tuples(
                    std::tuple<const column<t_uncompr_out_fs> * ...> p_ExpCols,
                    std::tuple<const column<t_uncompr_out_fs> * ...> p_FndCols,
                    MSV_CXX_ATTRIBUTE_PPUNUSED std::index_sequence<t_Idxs ...>
            ) {
                STATIC_ASSERT_PARAMPACK_SAMESIZE(t_Idxs, t_uncompr_out_fs)
                std::vector<bool> vec = {
                    equality_check(
                            std::get<t_Idxs>(p_ExpCols),
                            std::get<t_Idxs>(p_FndCols)
                    ).good() ...
                };
                return std::count(vec.begin(), vec.end(), false) == 0;
            }
            
            template<size_t ... t_Idxs>
            static void delete_uncompr_column_tuple(
                    std::tuple<const column<t_uncompr_out_fs> * ...> p_Cols,
                    MSV_CXX_ATTRIBUTE_PPUNUSED std::index_sequence<t_Idxs ...>
            ) {
                STATIC_ASSERT_PARAMPACK_SAMESIZE(t_Idxs, t_uncompr_out_fs)
                std::vector<const column<uncompr_f> *> vec = {
                    std::get<t_Idxs>(p_Cols) ...
                };
                for(auto col : vec)
                    delete col;
            }
            
        public:
            // These must all be uncompr_f, but their number matters.
            template<class ... t_uncompr_in_fs>
            struct for_uncompr_input_formats {
                
                template<typename ... t_additional_param_ts>
                struct for_additional_params {
                    
                    template<typename ... t_variant_key_ts>
                    struct for_variant_keys {

                        template<size_t ... t_VariantKeyIdxs>
                        struct internal {
                            
                            STATIC_ASSERT_PARAMPACK_SAMESIZE(t_VariantKeyIdxs, t_variant_key_ts)
                        
                            template<typename ... t_setting_key_ts>
                            class for_setting_keys {
                                
                                const std::vector<std::string> m_CsvVariantKeyColNames;
                                std::vector<std::string> m_CsvAllKeyColNames;
                                
                                void check_csv_column_names(
                                        const std::vector<std::string> & p_CsvColNames,
                                        const std::string & p_Word,
                                        size_t p_ExpectedCount
                                ) {
                                    const size_t count = p_CsvColNames.size();
                                    if(count != p_ExpectedCount) {
                                        std::stringstream s;
                                        s
                                                << "you must provide as many " << p_Word
                                                << " column names as there are " << p_Word
                                                << "s (found " << count << ", expected "
                                                << p_ExpectedCount << ')';
                                        throw std::runtime_error(s.str());
                                    }
                                }

                            public:
                                struct abstract_operator_wrapper {
                                    virtual
                                    std::tuple<const column<t_uncompr_out_fs> * ...>
                                    call_operator(
                                            column_cache & cache,
                                            const column<t_uncompr_in_fs> * ... p_InCols,
                                            t_additional_param_ts ... p_AdditionalParams,
                                            t_variant_key_ts ... p_VariantKeys,
                                            t_setting_key_ts ... p_SettingKeys
                                    ) = 0;
                                };
                                
                                struct operator_wrapper {

                                    // return_type_helper is required, because those
                                    // operators that return a single column return it as
                                    // a mere pointer, not as a tuple containing a pointer.

                                    template<class ... t_out_fs>
                                    struct return_type_helper {
                                        STATIC_ASSERT_PARAMPACK_SAMESIZE(t_out_fs, t_uncompr_out_fs)
                                        using return_t = std::tuple<const column<t_out_fs> * ...>;
                                    };

                                    template<class t_single_out_f>
                                    struct return_type_helper<t_single_out_f> {
                                        using return_t = const column<t_single_out_f> *;
                                    };

                                    template<class ... t_out_fs>
                                    struct for_output_formats {

                                        STATIC_ASSERT_PARAMPACK_SAMESIZE(t_out_fs, t_uncompr_out_fs)

                                        template<size_t ... t_Idxs>
                                        static
                                        std::tuple<const column<t_uncompr_out_fs> * ...>
                                        decompress_column_tuple(
                                                std::tuple<const column<t_out_fs> * ...> p_Cols,
                                                MSV_CXX_ATTRIBUTE_PPUNUSED std::index_sequence<t_Idxs ...>
                                        ) {
                                            STATIC_ASSERT_PARAMPACK_SAMESIZE(t_Idxs, t_uncompr_out_fs)
                                            return {
                                                // @todo Do not hardcode the processing style.
                                                morph<processing_style_t::scalar, uncompr_f>(
                                                        std::get<t_Idxs>(p_Cols) ...
                                                )
                                            };
                                        }

                                        template<size_t t_Count, class t_head_f, class ... t_tail_fs>
                                        struct column_tuple_deleter {
                                            static void apply(
                                                    const column<t_head_f> * p_HeadCol,
                                                    const column<t_tail_fs> * ... p_TailCols
                                            ) {
                                                if(typeid(t_head_f) != typeid(uncompr_f))
                                                    delete p_HeadCol;
                                                column_tuple_deleter<t_Count - 1, t_tail_fs ...>::apply(
                                                        p_TailCols ...
                                                );
                                            }
                                        };

                                        template<class t_head_f, class ... t_tail_fs>
                                        struct column_tuple_deleter<1, t_head_f, t_tail_fs ...> {
                                            static void apply(
                                                    const column<t_head_f> * p_HeadCol,
                                                    const column<t_tail_fs> * ... p_TailCols
                                            ) {
                                                if(typeid(t_head_f) != typeid(uncompr_f))
                                                    delete p_HeadCol;
                                            }
                                        };

                                        template<size_t ... t_Idxs>
                                        static void delete_column_tuple(
                                                std::tuple<const column<t_out_fs> * ...> p_Cols,
                                                MSV_CXX_ATTRIBUTE_PPUNUSED std::index_sequence<t_Idxs ...>
                                        ) {
                                            STATIC_ASSERT_PARAMPACK_SAMESIZE(t_Idxs, t_out_fs)
                                            column_tuple_deleter<sizeof...(t_out_fs), t_out_fs ...>::apply(
                                                    std::get<t_Idxs>(p_Cols) ...
                                            );
                                        }

                                        template<class ... t_in_fs>
                                        struct for_input_formats : public abstract_operator_wrapper {

                                            STATIC_ASSERT_PARAMPACK_SAMESIZE(t_in_fs, t_uncompr_in_fs)

                                            using op_func_ptr_t = typename return_type_helper<t_out_fs ...>::return_t (*)(
                                                    const column<t_in_fs> * ...,
                                                    t_additional_param_ts ...
                                            );

                                            op_func_ptr_t m_OpFuncPtr;

                                            for_input_formats(op_func_ptr_t p_OpFuncPtr)
                                            : m_OpFuncPtr(p_OpFuncPtr) {
                                                //
                                            }

                                            std::tuple<const column<t_uncompr_out_fs> * ...>
                                            call_operator(
                                                    column_cache & cache,
                                                    const column<t_uncompr_in_fs> * ... p_InCols,
                                                    t_additional_param_ts ... p_AdditionalParams,
                                                    t_variant_key_ts ... p_VariantKeys,
                                                    t_setting_key_ts ... p_SettingKeys
                                            ) {
                                                std::make_tuple(
                                                        cache.ensure_presence<t_in_fs>(p_InCols) ...
                                                );
                                                MONITORING_START_INTERVAL_FOR(
                                                        m_CsvColNameRuntime,
                                                        p_VariantKeys ...,
                                                        p_SettingKeys ...,
                                                        p_AdditionalParams ...
                                                );
                                                auto resInternal = (*m_OpFuncPtr)(
                                                        cache.get<t_in_fs>(p_InCols) ...,
                                                        p_AdditionalParams ...
                                                );
                                                MONITORING_END_INTERVAL_FOR(
                                                        m_CsvColNameRuntime,
                                                        p_VariantKeys ...,
                                                        p_SettingKeys ...,
                                                        p_AdditionalParams ...
                                                );
                                                // Note that, when `m_OpFuncPtr` returns a
                                                // single pointer (not wrapped in a tuple),
                                                // the following lines are totally ok,
                                                // although the first parameter of
                                                // `decompress_column_tuple` is a tuple,
                                                // since this single-element-tuple can be
                                                // constructed from a single pointer passed
                                                // to it. It is essentially the same thing
                                                // as `std::tuple<int> x = 1;`, which is
                                                // also valid.
                                                auto seq = std::index_sequence_for<t_out_fs ...>();
                                                auto resDecompr = decompress_column_tuple(resInternal, seq);
                                                delete_column_tuple(resInternal, seq);
                                                return resDecompr;
                                            }
                                        };
                                    };
                                };

                                using variant_t = std::tuple<
                                        abstract_operator_wrapper *,
                                        t_variant_key_ts ...
                                >;
                                
                                for_setting_keys (
                                        const std::vector<std::string> p_VariantCsvKeyColNames,
                                        const std::vector<std::string> p_SettingCsvKeyColNames,
                                        const std::vector<std::string> p_AddParamsCsvKeyColNames
                                ) : m_CsvVariantKeyColNames(p_VariantCsvKeyColNames) {
                                    check_csv_column_names(
                                            p_VariantCsvKeyColNames,
                                            "variant key",
                                            sizeof...(t_variant_key_ts)
                                    );
                                    check_csv_column_names(
                                            p_SettingCsvKeyColNames,
                                            "setting key",
                                            sizeof...(t_setting_key_ts)
                                    );
                                    check_csv_column_names(
                                            p_AddParamsCsvKeyColNames,
                                            "additional parameter",
                                            sizeof...(t_additional_param_ts)
                                    );
                                    
                                    m_CsvAllKeyColNames = p_VariantCsvKeyColNames;
                                    m_CsvAllKeyColNames.insert(
                                            m_CsvAllKeyColNames.end(),
                                            p_SettingCsvKeyColNames.begin(),
                                            p_SettingCsvKeyColNames.end()
                                    );
                                    m_CsvAllKeyColNames.insert(
                                            m_CsvAllKeyColNames.end(),
                                            p_AddParamsCsvKeyColNames.begin(),
                                            p_AddParamsCsvKeyColNames.end()
                                    );
                                }

                                void execute_variants(
                                        const std::vector<variant_t> p_Variants,
                                        t_setting_key_ts ... p_SettingKeys,
                                        const column<t_uncompr_in_fs> * ... p_InCols,
                                        t_additional_param_ts ... p_AdditionalParams
                                ) {
                                    column_cache cache;

                                    std::cerr << "Executing Variants" << std::endl << '\t';
                                    for(const auto & colName : m_CsvVariantKeyColNames)
                                        std::cerr << colName << '\t';
                                    std::cerr << std::endl;
                                    auto nullptrTuple = repeat_as_tuple<
                                            sizeof...(t_uncompr_out_fs),
                                            const column<uncompr_f> *,
                                            nullptr
                                    >::value;
                                    std::tuple<const column<t_uncompr_out_fs> * ...> referenceOutput = nullptrTuple;
                                    for(auto variant : p_Variants) {
                                        abstract_operator_wrapper * op = std::get<0>(variant);

                                        std::cerr
                                                << '\t'
                                                << doPrint('\t', std::get<t_VariantKeyIdxs + 1>(variant) ...)
                                                << ": started... ";
                                        std::cerr.flush();

                                        MONITORING_CREATE_MONITOR(
                                                MONITORING_MAKE_MONITOR(
                                                        std::get<t_VariantKeyIdxs + 1>(variant) ...,
                                                        p_SettingKeys ...,
                                                        p_AdditionalParams ...
                                                ),
                                                m_CsvAllKeyColNames
                                        );

                                        auto currentOutput = op->call_operator(
                                                cache,
                                                p_InCols ...,
                                                p_AdditionalParams ...,
                                                std::get<t_VariantKeyIdxs + 1>(variant) ...,
                                                p_SettingKeys ...
                                        );

                                        if(referenceOutput == nullptrTuple) {
                                            referenceOutput = currentOutput;
                                            MONITORING_ADD_INT_FOR(
                                                    m_CsvColNameCheck,
                                                    -1,
                                                    std::get<t_VariantKeyIdxs + 1>(variant) ...,
                                                    p_SettingKeys ...,
                                                    p_AdditionalParams ...
                                            );
                                        }
                                        else {
                                            MONITORING_ADD_INT_FOR(
                                                    m_CsvColNameCheck,
                                                    check_uncompr_column_tuples(
                                                            referenceOutput,
                                                            currentOutput,
                                                            std::index_sequence_for<t_uncompr_out_fs ...>()
                                                    ),
                                                    std::get<t_VariantKeyIdxs + 1>(variant) ...,
                                                    p_SettingKeys ...,
                                                    p_AdditionalParams ...
                                            );
                                            delete_uncompr_column_tuple(
                                                    currentOutput,
                                                    std::index_sequence_for<t_uncompr_out_fs ...>()
                                            );
                                        }
                                        std::cerr << "done." << std::endl;
                                    }
                                    delete_uncompr_column_tuple(
                                            referenceOutput,
                                            std::index_sequence_for<t_uncompr_out_fs ...>()
                                    );
                                }

                                void done() {
                                    // @todo Print a summary etc..
                                    MONITORING_PRINT_MONITORS(monitorCsvLog);
                                }
                            };
                        };
                    };
                };
            };
        };
    };

    const std::string variant_executor::m_CsvColNameRuntime = "runtime:µs";
    const std::string variant_executor::m_CsvColNameCheck = "check";
}
#endif //MORPHSTORE_CORE_UTILS_VARIANT_EXECUTOR_H
