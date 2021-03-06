if( BUILD_ALL OR BUILD_MICROBMS )
    add_executable( iterator_variants iterator_variants.cpp )
    target_compile_options( iterator_variants PRIVATE
                            $<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( iterator_variants PRIVATE "-ldl" )

    #add_executable( agg_sum_benchmark agg_sum_benchmark.cpp )
    #target_compile_options( agg_sum_benchmark PRIVATE
    #                        $<$<CONFIG:DEBUG>:-DDEBUG>
    #)
    #target_link_libraries( agg_sum_benchmark PRIVATE "-ldl" )

    add_executable( select_benchmark select_benchmark.cpp )
    target_compile_options( select_benchmark PRIVATE
                            $<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( select_benchmark PRIVATE "-ldl" )

    add_executable( select_benchmark_2_t select_benchmark_2.cpp )
    target_compile_options( select_benchmark_2_t PRIVATE
                            -DSELECT_BENCHMARK_2_TIME
                            $<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( select_benchmark_2_t PRIVATE "-ldl" )

    add_executable( select_benchmark_2_d select_benchmark_2.cpp )
    target_compile_options( select_benchmark_2_d PRIVATE
                            -USELECT_BENCHMARK_2_TIME
                            $<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( select_benchmark_2_d PRIVATE "-ldl" )

    add_executable( select_uncompr_benchmark select_uncompr.cpp )
    target_compile_options( select_uncompr_benchmark PRIVATE
                            $<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( select_uncompr_benchmark PRIVATE "-ldl" )

    add_executable( inc_benchmark inc_benchmark.cpp )
    target_compile_options( inc_benchmark PRIVATE
                            $<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( inc_benchmark PRIVATE "-ldl" )

    add_executable( project_benchmark_t project_benchmark.cpp )
    target_compile_options( project_benchmark_t PRIVATE
                            -DPROJECT_BENCHMARK_TIME
                           # $<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( project_benchmark_t PRIVATE "-ldl" )

    add_executable( project_benchmark_d project_benchmark.cpp )
    target_compile_options( project_benchmark_d PRIVATE
                            -UPROJECT_BENCHMARK_TIME
                           # $<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( project_benchmark_d PRIVATE "-ldl" )

    add_executable( calc_binary_benchmark calc_binary_benchmark.cpp )
    target_compile_options( calc_binary_benchmark PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( calc_binary_benchmark PRIVATE "-ldl" )

    add_executable( merge_benchmark merge_benchmark.cpp )
    target_compile_options( merge_benchmark PRIVATE
                            -Wno-unused-parameter
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( merge_benchmark PRIVATE "-ldl" )

    add_executable( join_benchmark join_benchmark.cpp )
    target_compile_options( join_benchmark PRIVATE
                            -Wno-unused-parameter
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( join_benchmark PRIVATE "-ldl" )

    add_executable( semi_join_benchmark semi_join_benchmark.cpp )
    target_compile_options( semi_join_benchmark PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( semi_join_benchmark PRIVATE "-ldl" )

    add_executable( select_sum_benchmark select_sum_benchmark.cpp )
    target_compile_options( select_sum_benchmark PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( select_sum_benchmark PRIVATE "-ldl" )

    add_executable( nvmds_bench nvmds_bench.cc)
    target_compile_options( nvmds_bench PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        nvmds_bench PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")
   
    add_executable( nvmds_join nvmds_join.cc)
    target_compile_options( nvmds_join PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        nvmds_join PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")
    
    add_executable( numa_effects numa_effects.cpp)
    target_compile_options( numa_effects PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        numa_effects PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")

    add_executable( numa_test numa_test.cpp)
    target_compile_options( numa_test PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        numa_test PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")

    add_executable( nvmds_selectivity nvmds_selectivity.cc)
    target_compile_options( nvmds_selectivity PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        nvmds_selectivity PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")

    add_executable( nvmds_doubleselect nvmds_doubleselect.cc)
    target_compile_options( nvmds_doubleselect PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        nvmds_doubleselect PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")
   
    add_executable( ds_sel_properties ds_sel_properties.cc)
    target_compile_options( ds_sel_properties PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        ds_sel_properties PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")

    add_executable( ds_join_bench ds_join_bench.cc)
    target_compile_options( ds_join_bench PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        ds_join_bench PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")
   
    add_executable( nvmds_parallel_select nvmds_parallel_select.cc)
    target_compile_options( nvmds_parallel_select PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        nvmds_parallel_select PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")

    add_executable( nvmds_advisor nvmds_advisor.cc)
    target_compile_options( nvmds_advisor PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        nvmds_advisor PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")

    add_executable( nvmds_advisor_double nvmds_advisor_double.cc)
    target_compile_options( nvmds_advisor_double PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries(
        nvmds_advisor_double PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")

endif( BUILD_ALL OR BUILD_MICROBMS )
