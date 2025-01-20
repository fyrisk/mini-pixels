#include "vector/BinaryColumnVector.h"
#include <iostream>
#include <cstring>

const float BinaryColumnVector::EXTRA_SPACE_FACTOR = 1.2f;

BinaryColumnVector::BinaryColumnVector(uint64_t len, bool encoding) 
    : ColumnVector(len, encoding), 
      vector(nullptr), 
      start(nullptr), 
      lens(nullptr), 
      buffer(nullptr), 
      nextFree(0), 
      bufferLength(0), 
      bufferAllocationCount(0), 
      smallBuffer(nullptr), 
      smallBufferNextFree(0)
{
    std::cout << "Entering BinaryColumnVector constructor" << std::endl;
    posix_memalign(reinterpret_cast<void**>(&vector), 32, len * sizeof(duckdb::string_t *));
    posix_memalign(reinterpret_cast<void**>(&start), 32, len * sizeof(int));
    posix_memalign(reinterpret_cast<void**>(&lens), 32, len * sizeof(int));
    memoryUsage += sizeof(int) * (len * 4);
    std::cout << "BinaryColumnVector constructed with len: " << len << std::endl;
}
BinaryColumnVector::~BinaryColumnVector() {
    std::cout << "Entering BinaryColumnVector destructor" << std::endl;
    if(!closed) {
        BinaryColumnVector::close();
    }
    std::cout << "Exiting BinaryColumnVector destructor" << std::endl;
}

void BinaryColumnVector::close() {
    std::cout << "Entering BinaryColumnVector::close" << std::endl;
    if (!closed) {
        ColumnVector::close();
        /*delete[] start;
        delete[] lens;
        delete[] vector;
        delete[] smallBuffer;
        delete[] buffer;*/
        std::cout << "BinaryColumnVector resources released" << std::endl;
    }
    std::cout << "Exiting BinaryColumnVector::close" << std::endl;
}


void BinaryColumnVector::setRef(int elementNum, uint8_t * const &sourceBuf, int startPos, int length) {
    std::cout << "Entering BinaryColumnVector::setRef for elementNum: " << elementNum << std::endl;
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }

    // Set the reference to the source buffer data
    //this->vector[elementNum] = duckdb::string_t(reinterpret_cast<const char*>(sourceBuf + startPos), length);
    this->vector[elementNum] = duckdb::string_t((char *)(sourceBuf + startPos), length);
    this->start[elementNum] = 0;
    this->lens[elementNum] = length;
    this->isNull[elementNum] = (sourceBuf == nullptr);
    std::cout << "setRef completed for elementNum: " << elementNum << ", length: " << length << std::endl;
    // 单独使用sourceBuf, startPos, length打印字符内容
    std::cout << "Source string: " << std::string((vector[elementNum].GetData()), length) << std::endl;

}
void * BinaryColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}


void BinaryColumnVector::add(std::string &value) {
    std::cout << "Entering BinaryColumnVector::add with string value: " << value << std::endl;
    size_t len = value.size();
    uint8_t* buffert = new uint8_t[len];
    std::memcpy(buffert, value.data(), len);
    add(buffert, len);
    //delete[] buffert;
    std::cout << "Exiting BinaryColumnVector::add" << std::endl;
}

void BinaryColumnVector::add(uint8_t *v, int len) {
    std::cout << "Entering BinaryColumnVector::add with len: " << len << std::endl;
    if (writeIndex >= getLength()) {
        ensureSize(writeIndex * 2, true);
    }
    setVal(writeIndex++, v, 0, len);
    std::cout << "Exiting BinaryColumnVector::add" << std::endl;
}

void BinaryColumnVector::setVal(int elementNum, uint8_t* sourceBuf, int startPos, int length) {
    std::cout << "Entering BinaryColumnVector::setVal for elementNum: " << elementNum << ", length: " << length << std::endl;
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }

    // Update the metadata
    this->vector[elementNum] = duckdb::string_t(reinterpret_cast<const char*>(sourceBuf + startPos), length);
    this->start[elementNum] = 0;
    this->lens[elementNum] = length;
    this->isNull[elementNum] = false;

    // Update the free index for the next insertion
    std::cout << "Exiting BinaryColumnVector::setVal" << std::endl;
}

void BinaryColumnVector::ensureSize(uint64_t size, bool preserveData) {
    // 调用父类的ensureSize函数，处理可能的基本列数据扩展
    ColumnVector::ensureSize(size, preserveData);
    
    // 如果当前长度小于新指定的大小，进行扩展
    if (getLength() < size) {
        // 先保存旧的数据，以便在必要时复制
        duckdb::string_t* oldVector = vector;
        int* oldStart = start;
        int* oldLens = lens;

        // 为vector分配新的内存
        posix_memalign(reinterpret_cast<void**>(&vector), 32, size * sizeof(duckdb::string_t*));
        posix_memalign(reinterpret_cast<void**>(&start), 32, size * sizeof(int));
        posix_memalign(reinterpret_cast<void**>(&lens), 32, size * sizeof(int));

        // 如果preserveData为真，保留原有的数据
        if (preserveData) {
            std::copy(oldVector, oldVector + getLength(), vector);
            std::copy(oldStart, oldStart + getLength(), start);
            std::copy(oldLens, oldLens + getLength(), lens);
        }

        // 释放之前的内存
        delete[] oldVector;
        delete[] oldStart;
        delete[] oldLens;

        // 更新内存使用量
        memoryUsage += (size - getLength()) * sizeof(duckdb::string_t*);
        memoryUsage += (size - getLength()) * sizeof(int) * 2;  // start 和 lens 的内存

        // 更新列向量的大小
        resize(size);
    }
}