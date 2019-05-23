//
// Created by jpietrzyk on 26.04.19.
//

#ifndef MORPHSTORE_CORE_OPERATORS_GENERAL_VECTORIZED_CALC_UNCOMPR_H
#define MORPHSTORE_CORE_OPERATORS_GENERAL_VECTORIZED_CALC_UNCOMPR_H

#include <vector/general_vector.h>
#include <vector/primitives/io.h>
#include <vector/primitives/create.h>
#include <vector/primitives/compare.h>
#include <vector/primitives/calc.h>
#include <core/utils/preprocessor.h>
#include <vector/scalar/extension_skalar.h>
#include <vector/scalar/primitives/calc_scalar.h>
#include <vector/scalar/primitives/compare_scalar.h>
#include <vector/scalar/primitives/io_scalar.h>
#include <vector/scalar/primitives/create_scalar.h>
#include <cassert>

namespace morphstore {

       using namespace vector;
   template<class VectorExtension, template<class> class Operator>
   struct calc_unary_processing_unit {
      IMPORT_VECTOR_BOILER_PLATE(VectorExtension)
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static
      vector_mask_t const &
      apply(
         vector_t const & p_DataVector
      ) {
         return Operator<VectorExtension>::apply(p_DataVector);
      }
   };

   template<class VectorExtension, template<class> class Operator>
   struct calc_unary_batch {
      IMPORT_VECTOR_BOILER_PLATE(VectorExtension)
      MSV_CXX_ATTRIBUTE_FORCE_INLINE static void apply(
         base_t const *& p_DataPtr,
         base_t *& p_OutPtr,
         size_t const p_Count
      ) {
         for(size_t i = 0; i < p_Count; ++i) {
            vector_t result =
               calc_unary_processing_unit<VectorExtension, Operator>::apply(
                  vector::load<VectorExtension, vector::iov::ALIGNED, vector_size_bit::value>(p_DataPtr)
               );
            vector::store<VectorExtension, vector::iov::ALIGNED, vector_size_bit::value>(p_OutPtr, result);
            p_DataPtr += vector_element_count::value;
            p_OutPtr += vector_element_count::value;
         }
      }
   };

   template<class VectorExtension, template<class> class Comparator>
   struct calc_unary {
      IMPORT_VECTOR_BOILER_PLATE(VectorExtension)
      MSV_CXX_ATTRIBUTE_FORCE_INLINE static
      column<uncompr_f> const *
      apply(
         column< uncompr_f > const * const p_DataColumn,
         const size_t p_OutPosCountEstimate = 0
      ) {
         const size_t inDataCount = p_DataColumn->get_count_values();
         base_t const * inDataPtr = p_DataColumn->get_data( );

         size_t const sizeByte =
            bool(p_OutPosCountEstimate)
            ? (p_OutPosCountEstimate * sizeof(base_t))
            : p_DataColumn->get_size_used_byte();

         auto outDataCol = new column<uncompr_f>(sizeByte);
         base_t * outDataPtr = outDataCol->get_data( );

         size_t const vectorCount = inDataCount / vector_element_count::value;
         size_t const remainderCount = inDataCount % vector_element_count::value;

         calc_unary_batch<VectorExtension,Comparator>::apply(inDataPtr, outDataPtr, vectorCount);
         calc_unary_batch<vector::scalar<base_t>,Comparator>::apply(inDataPtr, outDataPtr, remainderCount);

         outDataCol->set_meta_data(inDataCount, sizeByte);

         return outDataCol;
      }
   };




template<class VectorExtension, int Granularity, template< class, int > class Operator>
   struct compare_binary_processing_unit {
      IMPORT_VECTOR_BOILER_PLATE(VectorExtension)
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static
      vector_mask_t
      apply(
         vector_t const p_Data1Vector,
         vector_t const p_Data2Vector
      ) {
         return Operator<VectorExtension,Granularity>::apply(
            p_Data1Vector,
            p_Data2Vector
         );
      }
   };

template<class VectorExtension, int Granularity, template< class, int > class Operator>
struct compare_binary_batch {
      IMPORT_VECTOR_BOILER_PLATE(VectorExtension)
      MSV_CXX_ATTRIBUTE_FORCE_INLINE static void apply(
         base_t const * p_Data1Ptr,
         base_t const * p_Data2Ptr,
         vector_mask_t * p_OutPtr,
         size_t const p_Count
      ) {
          vector_mask_t result=0;
          vector_mask_t inter_result=0;
          int v_count=sizeof(vector_mask_t)*8/vector_element_count::value;
         for(size_t i = 0; i < p_Count; ++i) {
             
             v_count--;
             
             vector_t vec1 = vector::load<VectorExtension, vector::iov::ALIGNED, vector_size_bit::value>(p_Data1Ptr);
             vector_t vec2 = vector::load<VectorExtension, vector::iov::ALIGNED, vector_size_bit::value>(p_Data2Ptr);
             
             inter_result = compare_binary_processing_unit<VectorExtension,Granularity,Operator>::apply(vec1, vec2);
             result |= ( inter_result << (vector_element_count::value*v_count));
             
            *p_OutPtr = result;
            p_Data1Ptr += vector_element_count::value;
            p_Data2Ptr += vector_element_count::value;
            if (v_count==0) {
                p_OutPtr++;
                result=0;
                v_count=sizeof(vector_mask_t)*8/vector_element_count::value;
            }
            //p_OutPtr += vector_element_count::value;
         }
          *p_OutPtr = result;
      }
   };

template<class VectorExtension, int Granularity, template< class, int > class Operator>
struct compare_binary {
      IMPORT_VECTOR_BOILER_PLATE(VectorExtension)
      MSV_CXX_ATTRIBUTE_FORCE_INLINE static
      column<uncompr_f> const *
      apply(
         column< uncompr_f > const * const p_Data1Column,
         column< uncompr_f > const * const p_Data2Column,
         const size_t p_OutPosCountEstimate = 0
      ) {
         const size_t inData1Count = p_Data1Column->get_count_values();
         assert(inData1Count == p_Data2Column->get_count_values());

         base_t const * inData1Ptr = p_Data1Column->get_data( );
         base_t const * inData2Ptr = p_Data2Column->get_data( );


         size_t const sizeByte =
            bool(p_OutPosCountEstimate)
            ? (p_OutPosCountEstimate * sizeof(base_t))
            : p_Data1Column->get_size_used_byte();

         auto outDataCol = new column<uncompr_f>(sizeByte);
         base_t * outDataPtr = outDataCol->get_data( );

         size_t const vectorCount = inData1Count / vector_element_count::value;
         size_t const remainderCount = inData1Count % vector_element_count::value;

         compare_binary_batch<VectorExtension,64,Operator>::apply(inData1Ptr, inData2Ptr, (vector_mask_t *)outDataPtr, vectorCount);
         compare_binary_batch<scalar<v64<uint64_t>>,64,Operator>::apply(inData1Ptr, inData2Ptr, (vector_mask_t *) outDataPtr, remainderCount);

         outDataCol->set_meta_data(inData1Count, sizeByte);

         return outDataCol;
      }
   };


}



#endif //MORPHSTORE_CORE_OPERATORS_GENERAL_VECTORIZED_AGG_SUM_UNCOMPR_H


