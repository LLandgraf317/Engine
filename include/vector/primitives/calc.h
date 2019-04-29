/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   calc.h
 * Author: Annett
 *
 * Created on 17. April 2019, 11:07
 */

#ifndef CALC_H
#define CALC_H

#include <vector/general_vector.h>

namespace vector{
    
   
   template<class VectorExtension, int IOGranularity>
   struct calc;

   /*!
    * Add Vectors a and b component wise. Granularity gives the size of a component in bit, 
    * e.g. to add 64-bit integers, granularity is 64. To add float values, Granularity is 32.
    */
   template<class VectorExtension, int Granularity>
   typename VectorExtension::vector_t
   add(typename VectorExtension::vector_t a, typename VectorExtension::vector_t b ) {
       return calc<VectorExtension,  Granularity>::add( a, b );
   }
   
    /*!
    * Subtract Vector b from a component wise. Granularity gives the size of a component in bit, 
    * e.g. to subtract 64-bit integers, Granularity is 64. To subtract float values, Granularity is 32.
    */
   
   template<class VectorExtension, int Granularity>
   typename VectorExtension::vector_t
   sub(typename VectorExtension::vector_t a, typename VectorExtension::vector_t b ) {
       return calc<VectorExtension,  Granularity>::sub( a, b );
   }
   
    /*!
    * Builds the sum of all elements in vector a. This is really ugly if done 
    * vectorized using sse or avx2 (sequentially might be faster in these cases).
    */
   
   template<class VectorExtension, int Granularity>
   typename VectorExtension::base_t
   hadd(typename VectorExtension::vector_t a) {
       return calc<VectorExtension,  Granularity>::hadd( a );
   }
   
   
   /*! 
    * Multiplies vector a and b element wise. 
    * NOTE: There is no 64-bit multiply for SSE or AVX(2/512). So only the lower 32 bit 
    * of any element will be used for multiplication.
    * Benefit: Overflows are stored, too, since the result is still 64 bit
    * 
    */
   template<class VectorExtension, int Granularity>
   typename VectorExtension::vector_t
   mul(typename VectorExtension::vector_t a, typename VectorExtension::vector_t b ) {
       return calc<VectorExtension,  Granularity>::mul( a, b );
   }
   
   /*!
    * Divide a by b element wise.
    * NOTE: There is no intrinsic to divide integers. This implementation uses a double division.
    * Hence, 12 bit of every 64-bit value are lost for the sign and mantissa => Division only works 
    * if no element in either vector uses more than 52 bit.
    * 
    */
   template<class VectorExtension, int Granularity>
   typename VectorExtension::vector_t
   div(typename VectorExtension::vector_t a, typename VectorExtension::vector_t b ) {
       return calc<VectorExtension,  Granularity>::div( a, b );
   }
   
   /*!
    * a mod b element-wise.
    * NOTE: We need a division here but there is no intrinsic to divide integers. This implementation uses a double division.
    * Hence, 12 bit of every 64-bit value are lost for the sign and mantissa => Division only works 
    * if no element in either vector uses more than 52 bit.
    * 
    */
   template<class VectorExtension, int Granularity>
   typename VectorExtension::vector_t
   mod(typename VectorExtension::vector_t a, typename VectorExtension::vector_t b ) {
       return calc<VectorExtension,  Granularity>::mod( a, b );
   }
   
    /*!
    * Invert the sign of all numbers in vector a
    */
   template<class VectorExtension, int Granularity>
   typename VectorExtension::vector_t
   inv(typename VectorExtension::vector_t a ) {
       return calc<VectorExtension,  Granularity>::inv( a );
   }
   
      
}
#endif /* CALC_H */
