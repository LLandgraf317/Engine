if ( CTEST_ALL OR CTEST_PERSISTENCE )
	FILE( REMOVE ${CMAKE_BINARY_DIR}/test/core/persistence/binary_io_test_app )
	
	add_executable( binary_io_test_app binary_io_test.cpp )
	target_compile_options( binary_io_test_app PRIVATE
		# space to add custom flags for THIS SPECIFIC TARGET
	)
	target_link_libraries( binary_io_test_app PRIVATE "-ldl" )
	add_test( binary_io_test binary_io_test_app )
endif()
