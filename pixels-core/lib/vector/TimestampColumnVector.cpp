//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    this->precision = precision;
    // if(encoding) {
    //     posix_memalign(reinterpret_cast<void **>(&this->times), 64,
    //                    len * sizeof(long));
    // } else {
    //     this->times = nullptr;
    // }
    this->times = nullptr;
    posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                    len * sizeof(long));
    memoryUsage += (long) sizeof(long) * len;
}

void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    ColumnVector::ensureSize(size, preserveData);
	if (length < size)
	{
		long *oldVector = times;
		posix_memalign(reinterpret_cast<void **>(&times), 64,
						size * sizeof(int64_t));
		if (preserveData) {
			std::copy(oldVector, oldVector + length, times);
		}
		delete[] oldVector;
		memoryUsage += (int) sizeof(long) * (size - length);
		resize(size);
	}
}

inline int TimestampColumnVector::date2j(int y, int m, int d)
{
	int			julian;
	int			century;

	if (m > 2)
	{
		m += 1;
		y += 4800;
	}
	else
	{
		m += 13;
		y += 4799;
	}

	century = y / 100;
	julian = y * 365 - 32167;
	julian += y / 4 - century + century / 4;
	julian += 7834 * m / 256 + d;

	return julian;
}	

void TimestampColumnVector::add(std::string &value)
{
	if (writeIndex >= length)
	{
		ensureSize(writeIndex * 2, true);
	}
	int index = writeIndex ++;

    int year    = (value[0] - '0') * 1000 + (value[1] - '0') * 100 + (value[2] - '0') * 10 + (value[3] - '0');
    int month   = (value[5] - '0') * 10 + (value[6] - '0');
    int day     = (value[8] - '0') * 10 + (value[9] - '0');
    
    int hour    = (value[10] - '0') * 10 + (value[11] - '0');
    int minute  = (value[13] - '0') * 10 + (value[14] - '0');
    int second = (value[16] - '0') * 10 + (value[17] - '0');

    long v = (long)(date2j(year, month, day) - 2440588) * 24 * 60 * 60;
    v += hour * 60 * 60;
    v += minute * 60;
    v += second;

    v *= 1000 * 1000;

    times[index] = v;
    isNull[index] = false;

    // std::cout << "## " << v << std::endl;
}

