//
// Created by jpietrzyk on 20.05.19.
//

#ifndef MORPHSTORE_LOGIC_AVX2_H
#define MORPHSTORE_LOGIC_AVX2_H
#include <core/utils/preprocessor.h>
#include <core/memory/mm_glob.h>
#include <vector/simd/avx2/extension_avx2.h>
#include <vector/primitives/logic.h>


namespace vector {


   template<typename T>
   struct logic<avx2<v256<T>>, avx2<v256<T>>::vector_helper_t::size_bit::value > {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static
      typename avx2<v256<T>>::vector_t
      logical_and( typename avx2<v256<T>>::vector_t const & p_In1, typename avx2<v256<T>>::vector_t const & p_In2) {
         return _mm256_and_si256( p_In1, p_In2 );
      }

   };


}
#endif //MORPHSTORE_LOGIC_AVX2_H
