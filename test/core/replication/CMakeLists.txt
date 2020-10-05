if ( CTEST_ALL OR CTEST_INDEX )                                    
    FILE( REMOVE ${CMAKE_BINARY_DIR}/test/core/replicationmanager_test_app )

    add_executable( replicationmanager_test_app ReplicationManagerTest.cpp )

    target_compile_options( replicationmanager_test_app PRIVATE
    # space to add custom flags for THIS SPECIFIC TARGET
    )

    target_link_libraries( replicationmanager_test_app PRIVATE ${PMDK_LIBRARIES} pthread numa "-ldl")

    add_test( replicationmanager_test replicationmanager_test_app )                     

endif()
