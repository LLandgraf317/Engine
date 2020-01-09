/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   compare_avx2.h
 * Author: Annett
 *
 * Created on 23. April 2019, 16:57
 */

#ifndef MORPHSTORE_VECTOR_SIMD_AVX2_PRIMITIVES_COMPARE_AVX2_H
#define MORPHSTORE_VECTOR_SIMD_AVX2_PRIMITIVES_COMPARE_AVX2_H


#include <core/utils/preprocessor.h>
#include <core/memory/mm_glob.h>
#include <vector/simd/avx2/extension_avx2.h>
#include <vector/primitives/compare.h>

#include <functional>

namespace vectorlib{


  //64bit
   template<>
   struct equal<avx2<v256<uint64_t>>, 64> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint64_t>>::mask_t
      apply(
         typename avx2<v256<uint64_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint64_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 64 bit integer values from two registers: == ? (avx2)" );
         return
            _mm256_movemask_pd(
               _mm256_castsi256_pd(
                  _mm256_cmpeq_epi64(p_vec1, p_vec2)
               )
            );
      }
   };
   template<>
   struct less<avx2<v256<uint64_t>>, 64> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint64_t>>::mask_t
      apply(
         typename avx2<v256<uint64_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint64_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 64 bit integer values from two registers: < ? (avx2)" );
         return
            _mm256_movemask_pd(
               _mm256_castsi256_pd(
                  _mm256_cmpgt_epi64(p_vec2, p_vec1)
               )
            );
      }
   };
   template<>
   struct lessequal<avx2<v256<uint64_t>>, 64> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint64_t>>::mask_t
      apply(
         typename avx2<v256<uint64_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint64_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 64 bit integer values from two registers: <= ? (avx2)" );
         return
            _mm256_movemask_pd(
               _mm256_castsi256_pd(
                  _mm256_or_si256(
                     _mm256_cmpeq_epi64(p_vec1, p_vec2),
                     _mm256_cmpgt_epi64(p_vec2, p_vec1)
                  )
               )
            );
      }
   };
   template<>
   struct greater<avx2<v256<uint64_t>>, 64> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint64_t>>::mask_t
      apply(
         typename avx2<v256<uint64_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint64_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 64 bit integer values from two registers: > ? (avx2)" );
         return
            _mm256_movemask_pd(
               _mm256_castsi256_pd(
                  _mm256_cmpgt_epi64(p_vec1, p_vec2)
               )
            );
      }
   };
   template<>
   struct greaterequal<avx2<v256<uint64_t>>, 64> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint64_t>>::mask_t
      apply(
         typename avx2<v256<uint64_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint64_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 64 bit integer values from two registers: >= ? (avx2)" );
         return
            _mm256_movemask_pd(
               _mm256_castsi256_pd(
                  _mm256_or_si256(
                     _mm256_cmpeq_epi64(p_vec1, p_vec2),
                     _mm256_cmpgt_epi64(p_vec1, p_vec2)
                  )
               )
            );
      }
   };
   template<>
   struct count_matches<avx2<v256<uint64_t>>> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static uint8_t
      apply(
         typename avx2<v256<uint64_t>>::mask_t const & p_mask
      ) {
#if tally
calc_unary_simd += 1;
#endif
         trace( "[VECTOR] - Count matches in a comparison mask (avx2)" );
         // @todo Which one is faster?
         // return __builtin_popcount(p_mask);
         return _mm_popcnt_u64(p_mask);
      }
   };
   //32bit
   template<>
   struct equal<avx2<v256<uint32_t>>, 32> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint32_t>>::mask_t
      apply(
         typename avx2<v256<uint32_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint32_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 32 bit integer values from two registers: == ? (avx2)" );
         return
            _mm256_movemask_ps( //mm256
               _mm256_castsi256_ps( //Casts vector of type __m256i to type __m256
                  _mm256_cmpeq_epi32(p_vec1, p_vec2) //mm256i
               )
            );
      }
   };

   template<>
   struct less<avx2<v256<uint32_t>>, 32> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint32_t>>::mask_t
      apply(
         typename avx2<v256<uint32_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint32_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 32 bit integer values from two registers: < ? (avx2)" );
         return
            _mm256_movemask_ps(
               _mm256_castsi256_ps(
                  _mm256_cmpgt_epi32(p_vec2, p_vec1)
               )
            );
      }
   };

   template<>
   struct lessequal<avx2<v256<uint32_t>>, 32> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint32_t>>::mask_t
      apply(
         typename avx2<v256<uint32_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint32_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 32 bit integer values from two registers: <= ? (avx2)" );
         return
            _mm256_movemask_ps(
               _mm256_castsi256_ps(
                  _mm256_or_si256(
                     _mm256_cmpeq_epi32(p_vec1, p_vec2),
                     _mm256_cmpgt_epi32(p_vec2, p_vec1)
                  )
               )
            );
      }
   };

   template<>
   struct greater<avx2<v256<uint32_t>>, 32> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint32_t>>::mask_t
      apply(
         typename avx2<v256<uint32_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint32_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 32 bit integer values from two registers: > ? (avx2)" );
         return
            _mm256_movemask_ps(
               _mm256_castsi256_ps(
                  _mm256_cmpgt_epi32(p_vec1, p_vec2)
               )
            );
      }
   };
   template<>
   struct greaterequal<avx2<v256<uint32_t>>, 32> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint32_t>>::mask_t
      apply(
         typename avx2<v256<uint32_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint32_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 32 bit integer values from two registers: >= ? (avx2)" );
         return
            _mm256_movemask_ps(
               _mm256_castsi256_ps(
                  _mm256_or_si256(
                     _mm256_cmpeq_epi32(p_vec1, p_vec2),
                     _mm256_cmpgt_epi32(p_vec1, p_vec2)
                  )
               )
            );
      }
   };
   template<>
   struct count_matches<avx2<v256<uint32_t>>> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static uint8_t
      apply(
         typename avx2<v256<uint32_t>>::mask_t const & p_mask
      ) {
#if tally
calc_unary_simd += 1;
#endif
         trace( "[VECTOR] - Count matches in a comparison mask (avx2)" );
         // @todo Which one is faster?
          return __builtin_popcount(p_mask);
        // return _mm_popcnt_u64(p_mask);
      }
   };
//the 16bit functions probably won't work because they use _mm256_movemask_ps (no equivalent for 16bit)
   template<>
   struct equal<avx2<v256<uint16_t>>, 16> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint16_t>>::mask_t
      apply(
         typename avx2<v256<uint16_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint16_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 16 bit integer values from two registers: == ? (avx2)" );
         return
            _mm256_movemask_ps( //mm256
               _mm256_castsi256_ps( //Casts vector of type __m256i to type __m256
                  _mm256_cmpeq_epi16(p_vec1, p_vec2) //mm256i
               )
            );
      }
   };

   template<>
   struct less<avx2<v256<uint16_t>>, 16> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint16_t>>::mask_t
      apply(
         typename avx2<v256<uint16_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint16_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 16 bit integer values from two registers: < ? (avx2)" );
         return
            _mm256_movemask_ps(
               _mm256_castsi256_ps(
                  _mm256_cmpgt_epi16(p_vec2, p_vec1)
               )
            );
      }
   };

   template<>
   struct lessequal<avx2<v256<uint16_t>>, 16> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint16_t>>::mask_t
      apply(
         typename avx2<v256<uint16_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint16_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 16 bit integer values from two registers: <= ? (avx2)" );
         return
            _mm256_movemask_ps(
               _mm256_castsi256_ps(
                  _mm256_or_si256(
                     _mm256_cmpeq_epi16(p_vec1, p_vec2),
                     _mm256_cmpgt_epi16(p_vec2, p_vec1)
                  )
               )
            );
      }
   };

   template<>
   struct greater<avx2<v256<uint16_t>>, 16> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint16_t>>::mask_t
      apply(
         typename avx2<v256<uint16_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint16_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 16 bit integer values from two registers: > ? (avx2)" );
         return
            _mm256_movemask_ps(
               _mm256_castsi256_ps(
                  _mm256_cmpgt_epi16(p_vec1, p_vec2)
               )
            );
      }
   };
   template<>
   struct greaterequal<avx2<v256<uint16_t>>, 16> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint16_t>>::mask_t
      apply(
         typename avx2<v256<uint16_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint16_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 16 bit integer values from two registers: >= ? (avx2)" );
         return
            _mm256_movemask_ps(
               _mm256_castsi256_ps(
                  _mm256_or_si256(
                     _mm256_cmpeq_epi16(p_vec1, p_vec2),
                     _mm256_cmpgt_epi16(p_vec1, p_vec2)
                  )
               )
            );
      }
   };
   template<>
   struct count_matches<avx2<v256<uint16_t>>> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static uint8_t
      apply(
         typename avx2<v256<uint16_t>>::mask_t const & p_mask
      ) {
#if tally
calc_unary_simd += 1;
#endif
         trace( "[VECTOR] - Count matches in a comparison mask (avx2)" );
         // @todo Which one is faster?
          return __builtin_popcount(p_mask);
        // return _mm_popcnt_u64(p_mask);
      }
   };
//8 Bit
	template<>
   struct equal<avx2<v256<uint8_t>>, 8> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint8_t>>::mask_t
      apply(
         typename avx2<v256<uint8_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint8_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 8 bit integer values from two registers: == ? (avx2)" );
         return
            _mm256_movemask_epi8(
               _mm256_cmpeq_epi8(p_vec1, p_vec2)
            );
      }
   };
   template<>
   struct less<avx2<v256<uint8_t>>, 8> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint8_t>>::mask_t
      apply(
         typename avx2<v256<uint8_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint8_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 8 bit integer values from two registers: < ? (avx2)" );
         return
            _mm256_movemask_epi8(
               _mm256_cmpgt_epi8(p_vec2, p_vec1)
            );
      }
   };

   template<>
   struct lessequal<avx2<v256<uint8_t>>, 8> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint8_t>>::mask_t
      apply(
         typename avx2<v256<uint8_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint8_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 8 bit integer values from two registers: <= ? (avx2)" );
         return
            _mm256_movemask_epi8(
               _mm256_or_si256(
                  _mm256_cmpeq_epi8(p_vec1, p_vec2),
                  _mm256_cmpgt_epi8(p_vec2, p_vec1)
               )
            );
      }
   };


   template<>
   struct greater<avx2<v256<uint8_t>>, 8> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint8_t>>::mask_t
      apply(
         typename avx2<v256<uint8_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint8_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 8 bit integer values from two registers: > ? (avx2)" );
         return
            _mm256_movemask_epi8(
               _mm256_cmpgt_epi8(p_vec1, p_vec2)
            );
      }
   };
   template<>
   struct greaterequal<avx2<v256<uint8_t>>, 8> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static typename avx2<v256<uint8_t>>::mask_t
      apply(
         typename avx2<v256<uint8_t>>::vector_t const & p_vec1,
         typename avx2<v256<uint8_t>>::vector_t const & p_vec2
      ) {
#if tally
compare_simd += 1;
#endif
         trace( "[VECTOR] - Compare 8 bit integer values from two registers: >= ? (avx2)" );
         return
            _mm256_movemask_epi8(
               _mm256_or_si256(
                  _mm256_cmpeq_epi8(p_vec1, p_vec2),
                  _mm256_cmpgt_epi8(p_vec1, p_vec2)
                )
            );
      }
   };

   template<>
   struct count_matches<avx2<v256<uint8_t>>> {
      MSV_CXX_ATTRIBUTE_FORCE_INLINE
      static uint8_t
      apply(
         typename avx2<v256<uint8_t>>::mask_t const & p_mask
      ) {
#if tally
calc_unary_simd += 1;
#endif
         trace( "[VECTOR] - Count matches in a comparison mask (avx2)" );
         // @todo Which one is faster?
         // return __builtin_popcount(p_mask);
         return _mm_popcnt_u64(p_mask);
      }
   };



/*
    template<typename T>
    struct compare<avx2<v256<T>>, 64> {

        template< typename U = T, typename std::enable_if< std::is_integral< U >::value, int >::type = 0 >
        MSV_CXX_ATTRIBUTE_INLINE
        static int
        equality( avx2< v256< uint64_t > >::vector_t p_vec1,  avx2< v256< uint64_t > >::vector_t p_vec2 ) {
            trace( "[VECTOR] - Compare 64 bit integer values from two registers (avx2)" );

            return _mm256_movemask_pd((__m256d)_mm256_cmpeq_epi64(p_vec1,p_vec2));

        }

        template< typename U = T, typename std::enable_if< std::is_integral< U >::value, int >::type = 0 >
        MSV_CXX_ATTRIBUTE_INLINE
        static int
        lessthan( avx2< v256< uint64_t > >::vector_t p_vec1,  avx2< v256< uint64_t > >::vector_t p_vec2 ) {
            trace( "[VECTOR] - Compare 64 bit integer values from two registers (avx2)" );

            return _mm256_movemask_pd((__m256d)_mm256_cmpgt_epi64(p_vec1,p_vec2));

        }

        template< typename U = T, typename std::enable_if< std::is_integral< U >::value, int >::type = 0 >
        MSV_CXX_ATTRIBUTE_INLINE
        static int
        greaterthan( avx2< v256< uint64_t > >::vector_t p_vec1,  avx2< v256< uint64_t > >::vector_t p_vec2 ) {
            trace( "[VECTOR] - Compare 64 bit integer values from two registers (avx2)" );

            return _mm256_movemask_pd((__m256d)_mm256_cmpgt_epi64(p_vec2,p_vec1));

        }

        template< typename U = T, typename std::enable_if< std::is_integral< U >::value, int >::type = 0 >
        MSV_CXX_ATTRIBUTE_INLINE
        static int
        greaterequal( avx2< v256< uint64_t > >::vector_t p_vec1,  avx2< v256< uint64_t > >::vector_t p_vec2 ) {
            trace( "[VECTOR] - Compare 64 bit integer values from two registers (sse)" );

            return _mm256_movemask_pd((__m256d)(_mm256_or_si256(_mm256_cmpeq_epi64(p_vec1,p_vec2),_mm256_cmpgt_epi64(p_vec1,p_vec2))));

        }

        template< typename U = T, typename std::enable_if< std::is_integral< U >::value, int >::type = 0 >
        MSV_CXX_ATTRIBUTE_INLINE
        static int
        lessequal( avx2< v256< uint64_t > >::vector_t p_vec1,  avx2< v256< uint64_t > >::vector_t p_vec2 ) {
            trace( "[VECTOR] - Compare 64 bit integer values from two registers (sse)" );

            return _mm256_movemask_pd((__m256d)(_mm256_or_si256(_mm256_cmpeq_epi64(p_vec1,p_vec2),_mm256_cmpgt_epi64(p_vec2,p_vec1))));

        }
    };

    template<typename T>
    struct compare<avx2<v256<T>>, 32> {

        template< typename U = T, typename std::enable_if< std::is_integral< U >::value, int >::type = 0 >
        MSV_CXX_ATTRIBUTE_INLINE
        static int
        equality( avx2< v256< uint64_t > >::vector_t p_vec1,  avx2< v256< uint64_t > >::vector_t p_vec2 ) {
            trace( "[VECTOR] - Compare 32 bit integer values from two registers (avx2)" );

            return _mm256_movemask_ps((__m256)_mm256_cmpeq_epi32(p_vec1,p_vec2));

        }

        template< typename U = T, typename std::enable_if< std::is_integral< U >::value, int >::type = 0 >
        MSV_CXX_ATTRIBUTE_INLINE
        static int
        lessthan( avx2< v256< uint64_t > >::vector_t p_vec1,  avx2< v256< uint64_t > >::vector_t p_vec2 ) {
            trace( "[VECTOR] - Compare 32 bit integer values from two registers (avx2)" );

            return _mm256_movemask_ps((__m256)_mm256_cmpgt_epi32(p_vec1,p_vec2));

        }

        template< typename U = T, typename std::enable_if< std::is_integral< U >::value, int >::type = 0 >
        MSV_CXX_ATTRIBUTE_INLINE
        static int
        greaterthan( avx2< v256< uint64_t > >::vector_t p_vec1,  avx2< v256< uint64_t > >::vector_t p_vec2 ) {
            trace( "[VECTOR] - Compare 32 bit integer values from two registers (avx2)" );

            return _mm256_movemask_ps((__m256)_mm256_cmpgt_epi32(p_vec2,p_vec1));

        }

        template< typename U = T, typename std::enable_if< std::is_integral< U >::value, int >::type = 0 >
        MSV_CXX_ATTRIBUTE_INLINE
        static int
        greaterequal( avx2< v256< uint64_t > >::vector_t p_vec1,  avx2< v256< uint64_t > >::vector_t p_vec2 ) {
            trace( "[VECTOR] - Compare 32 bit integer values from two registers (sse)" );

            return _mm256_movemask_ps((__m256)(_mm256_or_si256(_mm256_cmpeq_epi32(p_vec1,p_vec2),_mm256_cmpgt_epi32(p_vec1,p_vec2))));

        }

        template< typename U = T, typename std::enable_if< std::is_integral< U >::value, int >::type = 0 >
        MSV_CXX_ATTRIBUTE_INLINE
        static int
        lessequal( avx2< v256< uint64_t > >::vector_t p_vec1,  avx2< v256< uint64_t > >::vector_t p_vec2 ) {
            trace( "[VECTOR] - Compare 32 bit integer values from two registers (sse)" );

            return _mm256_movemask_ps((__m256)(_mm256_or_si256(_mm256_cmpeq_epi32(p_vec1,p_vec2),_mm256_cmpgt_epi32(p_vec2,p_vec1))));

        }
    };*/
}

#endif /* MORPHSTORE_VECTOR_SIMD_AVX2_PRIMITIVES_COMPARE_AVX2_H */
