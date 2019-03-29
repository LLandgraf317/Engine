/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include <core/memory/mm_glob.h>
#include <core/storage/column.h>
#include <core/storage/column_gen.h>
#include <core/operators/scalar/agg_sum_uncompr.h>
#include <core/operators/vectorized/agg_sum_uncompr.h>
//#include <core/operators/vectorized/select_uncompr.h>
#include <core/operators/vectorized/project_uncompr.h>
#include <core/operators/vectorized/intersect_uncompr.h>
#include <core/operators/scalar/select_uncompr.h>

#include <iostream>

#include "core/operators/interfaces/intersect.h"


#define TEST_DATA_COUNT 10000

using namespace morphstore;

void init_data( column< uncompr_f > * const dataColumn ) {
   uint64_t * data = dataColumn->get_data( );
   size_t const count = TEST_DATA_COUNT / sizeof( uint64_t );
   for( size_t i = 0; i < count; ++i ) {
      data[ i ] = static_cast< uint64_t >( 1 );
   }
   dataColumn->set_meta_data( count, TEST_DATA_COUNT );
}

int main( void ) {
    column< uncompr_f > * testDataColumn = column<uncompr_f>::create_global_column(TEST_DATA_COUNT);
    const column< uncompr_f > * testDataColumnSorted = generate_sorted_unique(TEST_DATA_COUNT);
    init_data(testDataColumn);
    const column< uncompr_f > * testDataColumnSorted2 = generate_sorted_unique(TEST_DATA_COUNT,0,2);

    std::cout << "Start scalar aggregation...\n";
    auto sum_aggscalar_result=agg_sum<processing_style_t::scalar>( testDataColumn );//Do aggregation
    std::cout << "Done!\n";
    std::cout << "Should be "<< TEST_DATA_COUNT / sizeof( uint64_t ) << ". is: " << *((uint64_t*)(sum_aggscalar_result->get_data())) << "\n";
    
    std::cout << "Start aggregation with 128 bit registers...\n";
    auto sum_agg128_result=agg_sum<processing_style_t::vec128>( testDataColumn );//Do aggregation
    std::cout << "Done!\n";
    std::cout << "Should be "<< TEST_DATA_COUNT / sizeof( uint64_t ) << ". is: " << *((uint64_t*)(sum_agg128_result->get_data())) << "\n";
    
    std::cout << "Start aggregation with 256 bit registers...\n";
    auto sum_agg256_result=agg_sum<processing_style_t::vec256>( testDataColumn );//Do aggregation
    std::cout << "Done!\n";
    std::cout << "Should be "<< TEST_DATA_COUNT / sizeof( uint64_t ) << ". is: " << *((uint64_t*)(sum_agg256_result->get_data())) << "\n";
    
    
    std::cout << "Start select Tests...\n";
    auto selectscalar_result=morphstore::select<
                    std::less,
                    processing_style_t::scalar,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,8, TEST_DATA_COUNT);
    std::cout << "Scalar (Less)\n\t 1st 3 IDs: " << ((uint64_t*)(selectscalar_result->get_data()))[0] << ", " << ((uint64_t*)(selectscalar_result->get_data()))[1] << ", " << ((uint64_t*)(selectscalar_result->get_data()))[2] <<  "\n\t Count: " << selectscalar_result->get_count_values() << "\n";
    
   
    auto select128_result=morphstore::select<
                    std::less,
                    processing_style_t::vec128,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,8, TEST_DATA_COUNT );//Do aggregation
    std::cout << "128 bit (Less)\n\t 1st 3 IDs: " << ((uint64_t*)(select128_result->get_data()))[0] << ", " << ((uint64_t*)(select128_result->get_data()))[1] << ", " << ((uint64_t*)(select128_result->get_data()))[2] <<  "\n\t Count: " << select128_result->get_count_values() << "\n";
    
    auto select256_result=morphstore::select<
                std::less,
                processing_style_t::vec256,
                uncompr_f,
                uncompr_f
        >::apply( testDataColumnSorted,8, TEST_DATA_COUNT );//Do aggregation
    std::cout << "256 bit (Less)\n\t 1st 3 IDs: " << ((uint64_t*)(select256_result->get_data()))[0] << ", " << ((uint64_t*)(select256_result->get_data()))[1] << ", " << ((uint64_t*)(select256_result->get_data()))[2] <<  "\n\t Count: " << select256_result->get_count_values() << "\n";
    
    
    selectscalar_result=morphstore::select<
                    std::greater,
                    processing_style_t::scalar,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,9, TEST_DATA_COUNT);
    std::cout << "Scalar (Greater)\n\t 1st 3 IDs: " << ((uint64_t*)(selectscalar_result->get_data()))[0] << ", " << ((uint64_t*)(selectscalar_result->get_data()))[1] << ", " << ((uint64_t*)(selectscalar_result->get_data()))[2] <<  "\n\t Count: " << selectscalar_result->get_count_values() << "\n";
    
   
    select128_result=morphstore::select<
                    std::greater,
                    processing_style_t::vec128,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,9, TEST_DATA_COUNT );//Do aggregation
    std::cout << "128 bit (Greater)\n\t 1st 3 IDs: " << ((uint64_t*)(select128_result->get_data()))[0] << ", " << ((uint64_t*)(select128_result->get_data()))[1] << ", " << ((uint64_t*)(select128_result->get_data()))[2] <<  "\n\t Count: " << select128_result->get_count_values() << "\n";
    
    select256_result=morphstore::select<
                std::greater,
                processing_style_t::vec256,
                uncompr_f,
                uncompr_f
        >::apply( testDataColumnSorted,9, TEST_DATA_COUNT );//Do aggregation
    std::cout << "256 bit (Greater)\n\t 1st 3 IDs: " << ((uint64_t*)(select256_result->get_data()))[0] << ", " << ((uint64_t*)(select256_result->get_data()))[1] << ", " << ((uint64_t*)(select256_result->get_data()))[2] <<  "\n\t Count: " << select256_result->get_count_values() << "\n";
    
    
    selectscalar_result=morphstore::select<
                    std::equal_to,
                    processing_style_t::scalar,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,8, TEST_DATA_COUNT);
    std::cout << "Scalar (Equality)\n\t 1st 3 IDs: " << ((uint64_t*)(selectscalar_result->get_data()))[0] << ", " << ((uint64_t*)(selectscalar_result->get_data()))[1] << ", " << ((uint64_t*)(selectscalar_result->get_data()))[2] <<  "\n\t Count: " << selectscalar_result->get_count_values() << "\n";
    
   
    select128_result=morphstore::select<
                    std::equal_to,
                    processing_style_t::vec128,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,8, TEST_DATA_COUNT );//Do aggregation
    std::cout << "128 bit (Equality)\n\t 1st 3 IDs: " << ((uint64_t*)(select128_result->get_data()))[0] << ", " << ((uint64_t*)(select128_result->get_data()))[1] << ", " << ((uint64_t*)(select128_result->get_data()))[2] <<  "\n\t Count: " << select128_result->get_count_values() << "\n";
    
    select256_result=morphstore::select<
                    std::equal_to,
                    processing_style_t::vec256,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,8, TEST_DATA_COUNT );//Do aggregation
    std::cout << "256 bit (Equality)\n\t 1st 3 IDs: " << ((uint64_t*)(select256_result->get_data()))[0] << ", " << ((uint64_t*)(select256_result->get_data()))[1] << ", " << ((uint64_t*)(select256_result->get_data()))[2] <<  "\n\t Count: " << select256_result->get_count_values() << "\n";
    
    
    selectscalar_result=morphstore::select<
                    std::greater_equal,
                    processing_style_t::scalar,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,8, TEST_DATA_COUNT);
    std::cout << "Scalar (Greater Equal)\n\t 1st 3 IDs: " << ((uint64_t*)(selectscalar_result->get_data()))[0] << ", " << ((uint64_t*)(selectscalar_result->get_data()))[1] << ", " << ((uint64_t*)(selectscalar_result->get_data()))[2] <<  "\n\t Count: " << selectscalar_result->get_count_values() << "\n";
    
   
    select128_result=morphstore::select<
                    std::greater_equal,
                    processing_style_t::vec128,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,8, TEST_DATA_COUNT );//Do aggregation
    std::cout << "128 bit (Greater Equal)\n\t 1st 3 IDs: " << ((uint64_t*)(select128_result->get_data()))[0] << ", " << ((uint64_t*)(select128_result->get_data()))[1] << ", " << ((uint64_t*)(select128_result->get_data()))[2] <<  "\n\t Count: " << select128_result->get_count_values() << "\n";
    
    select256_result=morphstore::select<
                std::greater_equal,
                processing_style_t::vec256,
                uncompr_f,
                uncompr_f
        >::apply( testDataColumnSorted,8, TEST_DATA_COUNT );//Do aggregation
    std::cout << "256 bit (Greater Equal)\n\t 1st 3 IDs: " << ((uint64_t*)(select256_result->get_data()))[0] << ", " << ((uint64_t*)(select256_result->get_data()))[1] << ", " << ((uint64_t*)(select256_result->get_data()))[2] <<  "\n\t Count: " << select256_result->get_count_values() << "\n";
    
    
    selectscalar_result=morphstore::select<
                    std::less_equal,
                    processing_style_t::scalar,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,8, TEST_DATA_COUNT);
    std::cout << "Scalar (Less Equal)\n\t 1st 3 IDs: " << ((uint64_t*)(selectscalar_result->get_data()))[0] << ", " << ((uint64_t*)(selectscalar_result->get_data()))[1] << ", " << ((uint64_t*)(selectscalar_result->get_data()))[2] <<  "\n\t Count: " << selectscalar_result->get_count_values() << "\n";
    
   
    select128_result=morphstore::select<
                    std::less_equal,
                    processing_style_t::vec128,
                    uncompr_f,
                    uncompr_f
            >::apply( testDataColumnSorted,8, TEST_DATA_COUNT );//Do aggregation
    std::cout << "128 bit (Less Equal)\n\t 1st 3 IDs: " << ((uint64_t*)(select128_result->get_data()))[0] << ", " << ((uint64_t*)(select128_result->get_data()))[1] << ", " << ((uint64_t*)(select128_result->get_data()))[2] <<  "\n\t Count: " << select128_result->get_count_values() << "\n";
    
    select256_result=morphstore::select<
                std::less_equal,
                processing_style_t::vec256,
                uncompr_f,
                uncompr_f
        >::apply( testDataColumnSorted,8, TEST_DATA_COUNT );//Do aggregation
    std::cout << "256 bit (Less Equal)\n\t 1st 3 IDs: " << ((uint64_t*)(select256_result->get_data()))[0] << ", " << ((uint64_t*)(select256_result->get_data()))[1] << ", " << ((uint64_t*)(select256_result->get_data()))[2] <<  "\n\t Count: " << select256_result->get_count_values() << "\n";
    
    
    std::cout << "Start projection...\n";
    
    auto projectionscalar_result=project<processing_style_t::vec128, uncompr_f>(testDataColumnSorted,testDataColumn);
    std::cout << "Scalar Projection\n\t 1st 3 IDs: " << ((uint64_t*)(projectionscalar_result->get_data()))[0] << ", " << ((uint64_t*)(projectionscalar_result->get_data()))[1] << ", " << ((uint64_t*)(projectionscalar_result->get_data()))[2] <<  "\n\t Count: " << projectionscalar_result->get_count_values() << "\n";            
    auto projection128_result=project<processing_style_t::vec128, uncompr_f>(testDataColumnSorted,testDataColumn);
    std::cout << "128 bit Projection\n\t 1st 3 IDs: " << ((uint64_t*)(projection128_result->get_data()))[0] << ", " << ((uint64_t*)(projection128_result->get_data()))[1] << ", " << ((uint64_t*)(projection128_result->get_data()))[2] <<  "\n\t Count: " << projection128_result->get_count_values() << "\n";            
    auto projection256_result=project<processing_style_t::vec256, uncompr_f>(testDataColumnSorted,testDataColumn);
    std::cout << "256 bit Projection\n\t 1st 3 IDs: " << ((uint64_t*)(projection256_result->get_data()))[0] << ", " << ((uint64_t*)(projection256_result->get_data()))[1] << ", " << ((uint64_t*)(projection256_result->get_data()))[2] <<  "\n\t Count: " << projection256_result->get_count_values() << "\n";            
    
    int ok = memcmp(projectionscalar_result,projection128_result,projectionscalar_result->get_count_values()*sizeof(uint64_t));
    if (!ok) return ok;
    else std::cout << "Scalar and 128 bit Projections are equal\n";
    ok = memcmp(projectionscalar_result,projection256_result,projectionscalar_result->get_count_values()*sizeof(uint64_t));
    if (!ok) return ok;
    else std::cout << "Scalar and 256 bit Projections are equal\n";     
    
  
    auto intersect256_result=morphstore::intersect_sorted<processing_style_t::vec256, uncompr_f>(testDataColumnSorted,testDataColumnSorted2,TEST_DATA_COUNT);
    std::cout << "256 bit Intersection\n\t 1st 3 IDs: " << ((uint64_t*)(intersect256_result->get_data()))[0] << ", " << ((uint64_t*)(intersect256_result->get_data()))[1] << ", " << ((uint64_t*)(intersect256_result->get_data()))[2] <<  "\n\t Count: " << intersect256_result->get_count_values() << "\n";
    
    
    
    return 0;
}