if ( CTEST_ALL OR CTEST_INDEX )                                    
    FILE( REMOVE ${CMAKE_BINARY_DIR}/test/core/nodebucketlist_test_app )
    FILE( REMOVE ${CMAKE_BINARY_DIR}/test/core/phashmap_test_app )

    add_executable( nodebucketlist_test_app NodeBucketListTest.cpp )           
    add_executable( phashmap_test_app PHashMapTest.cpp )           

    target_compile_options( nodebucketlist_test_app PRIVATE             
    # space to add custom flags for THIS SPECIFIC TARGET
    )                                                           
    target_compile_options( phashmap_test_app PRIVATE             
    # space to add custom flags for THIS SPECIFIC TARGET
    )

    target_link_libraries( nodebucketlist_test_app PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")
    target_link_libraries( phashmap_test_app PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")
                                                                
    add_test( nodebucketlist_test nodebucketlist_test_app )                     
    add_test( phashmap_test phashmap_test_app )                     
endif()
