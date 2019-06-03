//
// Created by jpietrzyk on 29.05.19.
//

#include <vector/primitives/logic.h>
#include <vector/primitives/io.h>

#include <vector/scalar/extension_scalar.h>
#include <vector/scalar/primitives/logic_scalar.h>
#include <vector/scalar/primitives/io_scalar.h>
#include <vector/scalar/primitives/calc_scalar.h>
#include <vector/scalar/primitives/compare_scalar.h>
#include <vector/scalar/primitives/create_scalar.h>


#include <vector/simd/avx2/extension_avx2.h>
#include <vector/simd/avx2/primitives/logic_avx2.h>
#include <vector/simd/avx2/primitives/io_avx2.h>
#include <vector/simd/avx2/primitives/calc_avx2.h>
#include <vector/simd/avx2/primitives/compare_avx2.h>
#include <vector/simd/avx2/primitives/create_avx2.h>
#include <vector/simd/sse/extension_sse.h>
#include <vector/simd/sse/primitives/logic_sse.h>
#include <vector/simd/sse/primitives/io_sse.h>
#include <vector/simd/sse/primitives/calc_sse.h>
#include <vector/simd/sse/primitives/create_sse.h>
#include <vector/simd/sse/primitives/compare_sse.h>

#include <vector/datastructures/hash_based/strategies/linear_probing.h>
#include <vector/datastructures/hash_based/hash_utils.h>
#include <vector/datastructures/hash_based/hash_map.h>
#include <core/operators/general_vectorized/join.h>

#include <vector/complex/hash.h>


#include <core/memory/mm_glob.h>
#include <core/morphing/format.h>
#include <core/storage/column.h>
#include <core/storage/column_gen.h>
#include <core/utils/basic_types.h>
#include <core/utils/printing.h>


#include <vector>
#include <algorithm>


int main( void ) {

   using namespace vector;
   using namespace morphstore;

   size_t const dataCount = 131;


   auto col1 = generate_sorted_unique(dataCount, 1, 1);
   auto col2 = generate_sorted_unique(dataCount, 1, 0);

   const column<uncompr_f> * col3;
   const column<uncompr_f> * col4;
   std::tie(col3, col4) =
      equi_join<
         uncompr_f,
         sse<v128<uint64_t>>,
         hash_map<
            avx2<v256<uint64_t>>,
            multiply_mod_hash,
            size_policy_hash::EXPONENTIAL,
            scalar_key_vectorized_linear_search,
            60
         >
      >::apply(col1, col2);

   print_columns(
      print_buffer_base::decimal,
      col1,
      col2,
      col3,
      col4,
      "LEFT",
      "RIGHT",
      "LEFT_POS",
      "RIGHT_POS"
   );

   return 0;
}