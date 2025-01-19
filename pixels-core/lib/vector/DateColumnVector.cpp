//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"

DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
	if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	} else {
		this->dates = nullptr;
        posix_memalign(reinterpret_cast<void **>(&this->dates), 32,
                       len * sizeof(int32_t));
	}
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
	if(elementNum >= writeIndex) {
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
	// TODO: isNull
}

void * DateColumnVector::current() {
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}


void DateColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    ColumnVector::ensureSize(size, preserveData);
	if (length < size)
	{
		int *oldVector = dates;
		posix_memalign(reinterpret_cast<void **>(&dates), 32,
						size * sizeof(int32_t));
		if (preserveData) {
			std::copy(oldVector, oldVector + length, dates);
		}
		delete[] oldVector;
		memoryUsage += (int) sizeof(int) * (size - length);
		resize(size);
	}
}

inline int DateColumnVector::date2j(int y, int m, int d)
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

void DateColumnVector::add(std::string &value)
{
	if (writeIndex >= length)
	{
		ensureSize(writeIndex * 2, true);
	}
	int index = writeIndex ++;

    int year    = (value[0] - '0') * 1000 + (value[1] - '0') * 100 + (value[2] - '0') * 10 + (value[3] - '0');
    int month   = (value[5] - '0') * 10 + (value[6] - '0');
    int day     = (value[8] - '0') * 10 + (value[9] - '0');

	int v = date2j(year, month, day) - 2440588;
	// std::cout << "test1" << std::endl;
	dates[index] = v;
	// std::cout << "test2" << std::endl;
	isNull[index] = false;
	// std::cout << "test3" << std::endl;
}