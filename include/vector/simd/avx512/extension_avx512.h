/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   extension_avx512.h
 * Author: Annett
 *
 * Created on 12. April 2019, 12:21
 */

#ifndef EXTENSION_AVX512_H
#define EXTENSION_AVX512_H

#include <cstdint>
#include <type_traits>
#include "immintrin.h"

#include "vector/general_vector.h"

namespace vector{
   template<class VectorReg>
   struct avx512;

   template<typename T>
   struct avx512< v512< T > > {
      static_assert(std::is_arithmetic<T>::value, "Base type of vector register has to be arithmetic.");
      using vector_helper_t = v512<T>;
      using base_t = typename vector_helper_t::base_t;
	  
      using vector_t =
      typename std::conditional<
         (1==1) == std::is_integral<T>::value,    // if T is integer
         __m512i,                       //    vector register = __m128i
         typename std::conditional<
            (1==1) == std::is_same<float, T>::value, // else if T is float
            __m512,                       //    vector register = __m128
            __m512d                       // else [T == double]: vector register = __m128d
         >::type
      >::type;

      using size = std::integral_constant<size_t, sizeof(vector_t)>;
      using mask_t = uint16_t;
   };
}

#endif /* EXTENSION_AVX512_H */

