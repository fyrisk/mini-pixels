#ifndef PIXELS_BINARYCOLUMNVECTOR_H
#define PIXELS_BINARYCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include "duckdb.h"
#include "duckdb/common/types/vector.hpp"
#include <string>
#include <cstring>
#include <algorithm>
#include <stdexcept>

/**
 * BinaryColumnVector derived from org.apache.hadoop.hive.ql.exec.vector.
 * <p>
 * This class supports string and binary data by value reference -- i.e. each field is
 * explicitly present, as opposed to provided by a dictionary reference.
 * In some cases, all the values will be in the same byte array to begin with,
 * but this need not be the case. If each value is in a separate byte
 * array to start with, or not all the values are in the same original
 * byte array, you can still assign data by reference into this column vector.
 * This gives flexibility to use this in multiple situations.
 * <p>
 * When setting data by reference, the caller
 * is responsible for allocating the byte arrays used to hold the data.
 * You can also set data by value, as long as you call the initBuffer() method first.
 * You can mix "by value" and "by reference" in the same column vector,
 * though that use is probably not typical.
 */

class BinaryColumnVector: public ColumnVector {
public:
    duckdb::string_t * vector;    // Pointer to hold the binary data
    int* start;                  // Start offset of each field
    int* lens;                   // Length of each field
    uint8_t* buffer;             // Optional buffer for holding the data
    int nextFree;                // Next free position in the buffer
    int bufferLength;            // Current buffer length
    int bufferAllocationCount;   // Counter for buffer allocations

    uint8_t* smallBuffer;
    int smallBufferNextFree;
    // Constants for buffer allocation
    static const int DEFAULT_BUFFER_SIZE = 16 * 1024; // Adjust as needed
    static const int MAX_SIZE_FOR_SMALL_BUFFER = 1024 * 1024;

    static const float EXTRA_SPACE_FACTOR;

    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    explicit BinaryColumnVector(uint64_t len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false);
    ~BinaryColumnVector();

    /**
     * Set a field by reference.
     *
     * @param elementNum index within column vector to set
     * @param sourceBuf  container of source data
     * @param start      start byte position within source
     * @param length     length of source byte sequence
     */
    void setRef(int elementNum, uint8_t * const & sourceBuf, int start, int length);

    void * current() override;
    void close() override;
    //void print(int rowCount) override;

    void add(std::string &value) override;
    void add(uint8_t* v, int length);
    void setVal(int elementNum, uint8_t* sourceBuf, int start, int length);
    void ensureSize(uint64_t size, bool preserveData) override;
};

#endif // PIXELS_BINARYCOLUMNVECTOR_H