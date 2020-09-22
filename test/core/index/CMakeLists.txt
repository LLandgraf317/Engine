if ( CTEST_ALL OR CTEST_INDEX )                                    
    FILE( REMOVE ${CMAKE_BINARY_DIR}/test/core/generation_test_app )
    FILE( REMOVE ${CMAKE_BINARY_DIR}/test/core/nodebucketlist_test_app )
    FILE( REMOVE ${CMAKE_BINARY_DIR}/test/core/phashmap_test_app )
    FILE( REMOVE ${CMAKE_BINARY_DIR}/test/core/vhashmap_test_app )
    FILE( REMOVE ${CMAKE_BINARY_DIR}/test/core/skiplistindex_test_app )
    FILE( REMOVE ${CMAKE_BINARY_DIR}/test/core/volatilebptree_test_app )

    add_executable( generation_test_app GenerationTest.cpp )           
    add_executable( nodebucketlist_test_app NodeBucketListTest.cpp )           
    add_executable( phashmap_test_app PHashMapTest.cpp )           
    add_executable( vhashmap_test_app VHashMapTest.cpp )           
    add_executable( skiplistindex_test_app SkipListIndexTest.cpp )           
    add_executable( volatilebptree_test_app VolatileBPTreeTest.cpp )           

    target_compile_options( generation_test_app PRIVATE             
    # space to add custom flags for THIS SPECIFIC TARGET
    )
    target_compile_options( nodebucketlist_test_app PRIVATE             
    # space to add custom flags for THIS SPECIFIC TARGET
    )                                                           
    target_compile_options( phashmap_test_app PRIVATE             
    # space to add custom flags for THIS SPECIFIC TARGET
    )
    target_compile_options( vhashmap_test_app PRIVATE             
    # space to add custom flags for THIS SPECIFIC TARGET
    )
    target_compile_options( skiplistindex_test_app PRIVATE             
    # space to add custom flags for THIS SPECIFIC TARGET
    )
    target_compile_options( volatilebptree_test_app PRIVATE             
    # space to add custom flags for THIS SPECIFIC TARGET
    )

    target_link_libraries( nodebucketlist_test_app PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")
    target_link_libraries( vhashmap_test_app PRIVATE pthread numa "-ldl")
    target_link_libraries( phashmap_test_app PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")
    target_link_libraries( generation_test_app PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")
    target_link_libraries( skiplistindex_test_app PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")
    target_link_libraries( volatilebptree_test_app PRIVATE pthread numa "-ldl")
                                                                
    add_test( volatilebptree_test volatilebptree_test_app )                     
    add_test( vhashmap_test vhashmap_test_app )                     
    add_test( nodebucketlist_test nodebucketlist_test_app )                     
    add_test( generation_test generation_test_app )                     
    add_test( phashmap_test phashmap_test_app )                     
    add_test( skiplistindex_test skiplistindex_test_app )                     
endif()
