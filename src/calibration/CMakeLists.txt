if( BUILD_ALL OR BUILD_CALIB )
    add_executable( bw_prof bw_prof.cpp )
    target_compile_options( bw_prof PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( bw_prof PRIVATE "-ldl" )

    add_executable( bw_prof_casc bw_prof_casc.cpp )
    target_compile_options( bw_prof_casc PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( bw_prof_casc PRIVATE "-ldl" )

    add_executable( const_prof const_prof.cpp )
    target_compile_options( const_prof PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( const_prof PRIVATE "-ldl" )

    add_executable( uncompr uncompr.cpp )
    target_compile_options( uncompr PRIVATE
                            #$<$<CONFIG:DEBUG>:-DDEBUG>
    )
    target_link_libraries( uncompr PRIVATE "-ldl" )
endif( BUILD_ALL OR BUILD_CALIB )