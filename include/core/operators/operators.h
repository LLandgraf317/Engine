#include <core/storage/column.h>
#include <core/storage/VolatileColumn.h>

#include <tuple>
#include <unordered_map>
#include <map>

using storage::VolatileColumn;

template<class ColPtr, template<typename> class t_op>
struct select_col_t {
    static VolatileColumn * apply( ColPtr  inDataCol,  uint64_t value)
    {
         size_t inDataCount = inDataCol->get_count_values();
         uint64_t *  inData = inDataCol->get_data();

        auto outPosCol = new VolatileColumn(inDataCol->get_size(), 0);

        uint64_t* outpos = outPosCol->get_data();
         uint64_t* initOutPos = outpos;
        t_op<uint64_t> op;

        for (uint32_t i = 0; i < inDataCount; i++) {
            if (op(inData[i], value)) { // todo: replace with predicate functor
                *outpos = i;
                outpos++;
            }
        }

         size_t outPosCount = outpos - initOutPos;
        outPosCol->set_meta_data(outPosCount, outPosCount * sizeof(uint64_t));

        return outPosCol;
    }
};

template<class ColPtrL, class ColPtrR>
VolatileColumn * project( ColPtrL inPosCol, ColPtrR inDataCol)
{
	 size_t inPosCount = inPosCol->get_count_values();
	 uint64_t *  inData = inDataCol->get_data();
	 uint64_t *  inPos = inPosCol->get_data();

	 size_t inPosSize = inPosCol->get_size_used_byte();
	// Exact allocation size (for uncompressed data).
	auto outDataCol = new VolatileColumn(inPosSize, 0);
	uint64_t * outData = outDataCol->get_data();

	for(unsigned i = 0; i < inPosCount; i++) {
		*outData = inData[inPos[i]];
		outData++;
	}

	outDataCol->set_meta_data(inPosCount, inPosSize);

	return outDataCol;
}

template<class ColPtrL, class ColPtrR>
 std::tuple<
         VolatileColumn *,
         VolatileColumn *
>
nested_loop_join( ColPtrL inDataLCol, ColPtrR inDataRCol)
{
    uint64_t outCountEstimate = (inDataLCol->get_size() > inDataRCol->get_size() ? inDataRCol->get_size() : inDataLCol->get_size());
     size_t inDataLCount = inDataLCol->get_count_values();
     size_t inDataRCount = inDataRCol->get_count_values();

    // Ensure that the left column is the larger one, swap the input and output
    // column order if necessary.
    if(inDataLCount < inDataRCount) {
        auto outPosRL = nested_loop_join(
                inDataRCol,
                inDataLCol
        );
        return std::make_tuple(std::get<1>(outPosRL), std::get<0>(outPosRL));
    }

     uint64_t *  inDataL = inDataLCol->get_data();
     uint64_t *  inDataR = inDataRCol->get_data();

    // If no estimate is provided: Pessimistic allocation size (for
    // uncompressed data), reached only if the result is the cross product of
    // the two input columns.
     size_t size = bool(outCountEstimate)
            // use given estimate
            ? (outCountEstimate * sizeof(uint64_t))
            // use pessimistic estimate
            : (inDataLCount * inDataRCount * sizeof(uint64_t));
    auto outPosLCol = new VolatileColumn(size, 0);
    auto outPosRCol = new VolatileColumn(size, 0);
    uint64_t *  outPosL = outPosLCol->get_data();
    uint64_t *  outPosR = outPosRCol->get_data();

    unsigned iOut = 0;
    for(unsigned iL = 0; iL < inDataLCount; iL++)
        for(unsigned iR = 0; iR < inDataRCount; iR++)
            if(inDataL[iL] == inDataR[iR]) {
                outPosL[iOut] = iL;
                outPosR[iOut] = iR;
                iOut++;
            }

     size_t outSize = iOut * sizeof(uint64_t);
    outPosLCol->set_meta_data(iOut, outSize);
    outPosRCol->set_meta_data(iOut, outSize);

    return std::make_tuple(outPosLCol, outPosRCol);
}

template<class ColPtrL, class ColPtrR>
VolatileColumn*
left_semi_nto1_nested_loop_join(
        ColPtrL const inDataLCol,
        ColPtrR const inDataRCol//,
        //const size_t outCountEstimate
) {
    const size_t inDataLCount = inDataLCol->get_count_values();
    const size_t inDataRCount = inDataRCol->get_count_values();
    
    const uint64_t * const inDataL = inDataLCol->get_data();
    const uint64_t * const inDataR = inDataRCol->get_data();
    
    // If no estimate is provided: Pessimistic allocation size (for
    // uncompressed data), reached only if all data elements in the left input
    // column have a join partner in the right input column.
    const size_t size =
             (inDataLCount * sizeof(uint64_t));
    auto outPosLCol = new VolatileColumn(size, 0);
    uint64_t * const outPosL = outPosLCol->get_data();
    
    unsigned iOut = 0;
    for(unsigned iL = 0; iL < inDataLCount; iL++)
        for(unsigned iR = 0; iR < inDataRCount; iR++)
            if(inDataL[iL] == inDataR[iR]) {
                outPosL[iOut] = iL;
                iOut++;
                break;
            }
    
    outPosLCol->set_meta_data(iOut, iOut * sizeof(uint64_t));
    
    return outPosLCol;
}

template<class ColPtrL, class ColPtrR>
 std::tuple<
         VolatileColumn *,
         VolatileColumn *
>
group( ColPtrL inGrCol, ColPtrR inDataCol)
{
    // This implementation of the binary group-operator covers also the unary
    // case if inGrCol == nullptr .

     size_t inDataCount = inDataCol->get_count_values();
    size_t outExtCountEstimate = inDataCol->get_count_values();


    if(inGrCol != nullptr && inDataCount != inGrCol->get_count_values())
        throw std::runtime_error(
                "binary group: inGrCol and inDataCol must contain the same "
                "number of data elements"
        );

     uint64_t *  inGr = (inGrCol == nullptr)
            ? nullptr
            : inGrCol->get_data();
     uint64_t *  inData = inDataCol->get_data();

     size_t inDataSize = inDataCol->get_size_used_byte();
    // Exact allocation size (for uncompressed data).
    auto outGrCol = new VolatileColumn(inDataSize, 0);
    // If no estimate is provided: Pessimistic allocation size (for
    // uncompressed data), reached only if there are as many groups as data
    // elements.
    auto outExtCol = new VolatileColumn(
            bool(outExtCountEstimate)
            ? (outExtCountEstimate * sizeof(uint64_t)) // use given estimate
            : inDataSize // use pessimistic estimate
            ,0
    );
    uint64_t * outGr = outGrCol->get_data();
    uint64_t * outExt = outExtCol->get_data();
     uint64_t *  initOutExt = outExt;

    // For both cases: Note that the []-operator creates a new entry with the
    // group-id(value) 0 in the map if the data item(key) is not found. Hence,
    // we use the group-id(value) 0 to indicate that the data item(key) was not
    // found. Consequently, we should not store a group-id(value) 0. Therefore,
    // the data items(keys) are mapped to the group-id(value) + 1 .
    if(inGrCol == nullptr) {
        // Unary group-operator.
        std::unordered_map<
            uint64_t,
            uint64_t
            > groupIds;
        for(unsigned i = 0; i < inDataCount; i++) {
            uint64_t & groupId = groupIds[inData[i]];
            if(!groupId) { // The data item(key) was not found.
                groupId = groupIds.size();
                *outExt = i;
                outExt++;
            }
            *outGr = groupId - 1;
            outGr++;
        }
    }
    else {
        // Binary group-operator.
        // We have to use std::map, since std::pair is not hashable.
        std::map<std::pair<uint64_t, uint64_t>, uint64_t> groupIds;
        for(unsigned i = 0; i < inDataCount; i++) {
            uint64_t & groupId = groupIds[std::make_pair(inGr[i], inData[i])];
            if(!groupId) { // The data item(key) was not found.
                groupId = groupIds.size();
                *outExt = i;
                outExt++;
            }
            *outGr = groupId - 1;
            outGr++;
        }
    }

     size_t outExtCount = outExt - initOutExt;
    outGrCol->set_meta_data(inDataCount, inDataSize);
    outExtCol->set_meta_data(outExtCount, outExtCount * sizeof(uint64_t));

    return std::make_tuple(outGrCol, outExtCol);
}

 VolatileColumn * calc( VolatileColumn * /*in*/)
{

    return nullptr;
}

 VolatileColumn * intersect( VolatileColumn * /* in */)
{

    return nullptr;
}

 VolatileColumn * merge( VolatileColumn * inPosLCol,  VolatileColumn * inPosRCol)
{
     uint64_t * inPosL = inPosLCol->get_data();
     uint64_t * inPosR = inPosRCol->get_data();
     uint64_t *  inPosLEnd = inPosL + inPosLCol->get_count_values();
     uint64_t *  inPosREnd = inPosR + inPosRCol->get_count_values();

    // If no estimate is provided: Pessimistic allocation size (for
    // uncompressed data), reached only if the two input columns are disjoint.
    auto outPosCol = new VolatileColumn(
                    inPosLCol->get_size_used_byte() +
                    inPosRCol->get_size_used_byte()
                    ,0
    );
    uint64_t * outPos = outPosCol->get_data();
     uint64_t *  initOutPos = outPos;

    while(inPosL < inPosLEnd && inPosR < inPosREnd) {
        if(*inPosL < *inPosR) {
            *outPos = *inPosL;
            inPosL++;
        }
        else if(*inPosR < *inPosL) {
            *outPos = *inPosR;
            inPosR++;
        }
        else { // *inPosL == *inPosR
            *outPos = *inPosL;
            inPosL++;
            inPosR++;
        }
        outPos++;
    }
    // At this point, at least one of the operands has been fully consumed and
    // the other one might still contain data elements, which must be output.
    while(inPosL < inPosLEnd) {
        *outPos = *inPosL;
        outPos++;
        inPosL++;
    }
    while(inPosR < inPosREnd) {
        *outPos = *inPosR;
        outPos++;
        inPosR++;
    }

     size_t outPosCount = outPos - initOutPos;
    outPosCol->set_meta_data(outPosCount, outPosCount * sizeof(uint64_t));

    return outPosCol;
}

 VolatileColumn * append( VolatileColumn * /*in1*/,  VolatileColumn * /*n2*/)
{
    //TODO: implement or copy
    return nullptr;
}

 VolatileColumn * agg_sum( VolatileColumn * inDataCol)
{
     size_t inDataCount = inDataCol->get_count_values();
     uint64_t *  inData = inDataCol->get_data();

    // Exact allocation size (for uncompressed data).
    auto outDataCol = new VolatileColumn(sizeof(uint64_t), 0);
    uint64_t *  outData = outDataCol->get_data();

    *outData = 0;
    for(unsigned i = 0; i < inDataCount; i++)
        *outData += inData[i];

    outDataCol->set_meta_data(1, sizeof(uint64_t));

    return outDataCol;

}
