//
// Created by jpietrzyk on 26.04.19.
//

#ifndef MORPHSTORE_CORE_OPERATORS_GENERAL_VECTORIZED_AGG_SUM_UNCOMPR_H
#define MORPHSTORE_CORE_OPERATORS_GENERAL_VECTORIZED_AGG_SUM_UNCOMPR_H

#include <vector/general_vector.h>
#include <vector/primitives/calc.h>
#include <vector/primitives/io.h>
#include <vector/primitives/create.h>
#include <core/utils/preprocessor.h>

namespace morphstore {

   /*template<class VectorExtension>
   const column<uncompr_f> *
      agg_sum(
      column< uncompr_f > const * const p_DataColumn
   ) {
      using namespace vector;

      IMPORT_VECTOR_BOILER_PLATE(VectorExtension)

      size_t const vectorCount = p_DataColumn->get_count_values() / vector_element_count::value;
      size_t const remainderCount = p_DataColumn->get_count_values() % vector_element_count::value;
      base_t const * dataPtr = p_DataColumn->get_data( );
      vector_t resultVec = set1<VectorExtension,base_type_size_bit::value>(0);// = setzero<VectorExtension>( );
      for( size_t i = 0; i < vectorCount; ++i ) {
         resultVec = add<VectorExtension, base_t_granularity::value>(
            resultVec, load<VectorExtension,iov::ALIGNED, vector_size_bit::value>( dataPtr )
         );
         dataPtr += vector_element_count::value;
      }

      base_t result = hadd<VectorExtension,vector_base_type_size_bit::value>( resultVec );

      if( remainderCount != 0) {
         base_t const * remainderPtr = dataPtr;
         for( size_t i = 0; i < remainderCount; ++i ) {
            result += *remainderPtr++;
         }
      }

      auto outDataCol = new column<uncompr_f>(sizeof(base_t));
      base_t * const outData = outDataCol->get_data();
      *outData=result;
      outDataCol->set_meta_data(1, sizeof(base_t));
      return outDataCol;
   }*/

   template<class VectorExtension>
   struct agg_sum_processing_unit {
      IMPORT_VECTOR_BOILER_PLATE(VectorExtension)
      struct state_t {
         vector_t resultVec;
         state_t(void): resultVec{ vector::set1<VectorExtension, vector_base_t_granularity::value>( 0 ) } { }
         state_t(vector_t const & p_Data): resultVec{ p_Data } { }
      };
      MSV_CXX_ATTRIBUTE_FORCE_INLINE static void apply(
         vector_t const & p_DataVector,
         state_t & p_State
      ) {
         p_State = vector::add<VectorExtension, vector_base_t_granularity::value>(
            p_State.resultVec, p_DataVector
         );
      }
      MSV_CXX_ATTRIBUTE_FORCE_INLINE static base_t finalize(
         state_t const & p_State
      ) {
         return vector::hadd<VectorExtension,vector_base_t_granularity::value>( p_State.resultVec );
      }
   };

   template<class VectorExtension>
   struct agg_sum_batch {
      IMPORT_VECTOR_BOILER_PLATE(VectorExtension)
      MSV_CXX_ATTRIBUTE_FORCE_INLINE static
      base_t
      apply(
         base_t *&p_DataPtr,
         size_t const p_Count,
         typename agg_sum_processing_unit<VectorExtension>::state_t &p_State
      ) {
         for(size_t i = 0; i < p_Count; ++i) {
            vector_t dataVector = vector::load<VectorExtension, vector::iov::ALIGNED, vector_size_bit::value>(p_DataPtr);
            agg_sum_processing_unit<VectorExtension>::apply(
               dataVector,
               p_State
            );
            p_DataPtr += vector_element_count::value;
         }
         return agg_sum_processing_unit<VectorExtension>::finalize( p_State );
      }
   };

   template<class VectorExtension>
   struct agg_sum {

      MSV_CXX_ATTRIBUTE_FORCE_INLINE static
      const column<uncompr_f> *
      apply(
         column< uncompr_f > const * const p_DataColumn
      ) {
         IMPORT_VECTOR_BOILER_PLATE(VectorExtension)
         typename agg_sum_processing_unit<VectorExtension>::state_t vectorState;

         size_t const vectorCount = p_DataColumn->get_count_values() / vector_element_count::value;
         size_t const remainderCount = p_DataColumn->get_count_values() % vector_element_count::value;
         base_t const * dataPtr = p_DataColumn->get_data( );

         typename agg_sum_processing_unit<vector::scalar<base_t>>::state_t scalarState(
            agg_sum_batch<VectorExtension>::apply( dataPtr, vectorCount, vectorState )
         );

         vector::scalar<base_t>::base_t result =
            agg_sum_batch<vector::scalar<base_t>>::apply( dataPtr, remainderCount, scalarState );

         auto outDataCol = new column<uncompr_f>(sizeof(base_t));
         base_t * const outData = outDataCol->get_data();
         *outData = result;
         outDataCol->set_meta_data(1, sizeof(base_t));
         return outDataCol;
      }
   };


}



#endif //MORPHSTORE_CORE_OPERATORS_GENERAL_VECTORIZED_AGG_SUM_UNCOMPR_H

