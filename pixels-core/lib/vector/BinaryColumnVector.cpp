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

    posix_memalign(reinterpret_cast<void**>(&vector), 32, len * sizeof(uint8_t *));
    posix_memalign(reinterpret_cast<void**>(&start), 32, len * sizeof(int));
    posix_memalign(reinterpret_cast<void**>(&lens), 32, len * sizeof(int));
    memoryUsage += sizeof(int) * (len * 4);
}

BinaryColumnVector::~BinaryColumnVector() {
	if(!closed) {
		BinaryColumnVector::close();
	}
}


void BinaryColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        delete[] start;
        delete[] lens;
        delete[] vector;
        delete[] smallBuffer;
        delete[] buffer;
    }
}

void BinaryColumnVector::setRef(int elementNum, uint8_t * const &sourceBuf, int startPos, int length) {
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }

    // Set the reference to the source buffer data
    this->vector[elementNum] = sourceBuf;
    this->start[elementNum] = startPos;
    this->lens[elementNum] = length;
    this->isNull[elementNum] = (sourceBuf == nullptr);
}

void * BinaryColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}


void BinaryColumnVector::add(std::string value) {
    size_t len = value.size();
    uint8_t* buffert = new uint8_t[len];
    std::memcpy(buffert, value.data(), len);
    add(buffert,len);
    delete[] buffert;
}

void BinaryColumnVector::add(uint8_t *v,int len) {
    if(writeIndex>=getLength()) {
        ensureSize(writeIndex*2,true);
    }
    setVal(writeIndex++,v,0,len);
}

void BinaryColumnVector::setVal(int elementNum, uint8_t* sourceBuf, int startPos, int length) {
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }

    // Ensure the buffer has enough space
    if (buffer == nullptr) {
        initBuffer(0);
    } else if ((nextFree + length) > bufferLength) {
        increaseBufferSpace(length);
    }

    // Copy the data into the buffer
    std::memcpy(buffer + nextFree, sourceBuf + startPos, length);

    // Update the metadata
    this->vector[elementNum] = buffer ;
    this->start[elementNum] = nextFree;
    this->lens[elementNum] = length;
    this->isNull[elementNum] = false;

    // Update the free index for the next insertion
    nextFree += length;
}
void BinaryColumnVector::initBuffer(int estimatedValueSize) {
    nextFree = 0;
    smallBufferNextFree = 0;

    {
        // 如果 buffer 没有分配，申请空间，并考虑一些额外空间以避免频繁重新分配
        int bufferSize = static_cast<int>(getLength() * estimatedValueSize * EXTRA_SPACE_FACTOR);
        if (bufferSize < DEFAULT_BUFFER_SIZE) {
            bufferSize = DEFAULT_BUFFER_SIZE;
        }
        // 使用 posix_memalign 进行内存分配，要求 32 字节对齐
        posix_memalign(reinterpret_cast<void**>(&buffer), 32, bufferSize);
        memoryUsage += bufferSize;
        smallBuffer = buffer;  // 初始化时将 smallBuffer 也指向 buffer
        bufferLength = bufferSize;  // 更新 bufferLength
    }
    
    bufferAllocationCount = 0;
}

void BinaryColumnVector::increaseBufferSpace(int nextElemLength) {
    if (nextElemLength > MAX_SIZE_FOR_SMALL_BUFFER) {
        // 对于较大的分配，直接分配一个新 buffer，而不是使用小缓冲区
        uint8_t* newBuffer;
        posix_memalign(reinterpret_cast<void**>(&newBuffer), 32, nextElemLength);
        memoryUsage += nextElemLength;
        ++bufferAllocationCount;

        // 如果当前 buffer 指向 smallBuffer，则保存当前 smallBuffer 的状态
        if (smallBuffer == buffer) {
            smallBufferNextFree = nextFree;
        }

        // 将 buffer 指向新分配的大缓冲区
        buffer = newBuffer;
        nextFree = 0;
        bufferLength = nextElemLength;
    } else {
        // 如果之前分配了大于小缓冲区的内存，应该将 buffer 恢复为 smallBuffer
        if (smallBuffer != buffer) {
            buffer = smallBuffer;
            nextFree = smallBufferNextFree;
        }

        // smallBuffer 可能已经不够空间了
        if ((nextFree + nextElemLength) > bufferLength) {
            int newLength = bufferLength * 2;
            while (newLength < nextElemLength) {
                if (newLength < 0) {
                    throw std::runtime_error("Overflow of newLength. bufferLength=" + std::to_string(bufferLength) + ", nextElemLength=" + std::to_string(nextElemLength));
                }
                newLength *= 2;
            }
            posix_memalign(reinterpret_cast<void**>(&smallBuffer), 32, newLength);
            memoryUsage += newLength;
            memoryUsage += newLength;
            ++bufferAllocationCount;
            smallBufferNextFree = 0;

            // 更新 buffer 为 smallBuffer
            buffer = smallBuffer;
            bufferLength = newLength;
            nextFree = 0;
        }
    }
}
void BinaryColumnVector::ensureSize(uint64_t size, bool preserveData) {
    // 调用父类的ensureSize函数，处理可能的基本列数据扩展
    ColumnVector::ensureSize(size, preserveData);
    
    // 如果当前长度小于新指定的大小，进行扩展
    if (getLength() < size) {
        // 先保存旧的数据，以便在必要时复制
        uint8_t** oldVector = vector;
        int* oldStart = start;
        int* oldLens = lens;

        // 为vector分配新的内存
        posix_memalign(reinterpret_cast<void**>(&vector), 32, size * sizeof(uint8_t*));
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
        memoryUsage += (size - getLength()) * sizeof(uint8_t*);
        memoryUsage += (size - getLength()) * sizeof(int) * 2;  // start 和 lens 的内存

        // 更新列向量的大小
        resize(size);
    }
}
