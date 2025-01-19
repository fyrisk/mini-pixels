/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

#include "writer/DecimalColumnWriter.h"
#include "utils/BitUtils.h"

DecimalColumnWriter::DecimalColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption) :
ColumnWriter(type, writerOption), curPixelVector(pixelStride)
{
    runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
    if (runlengthEncoding)
    {
        encoder = std::make_unique<RunLenIntEncoder>();
    }
}

int DecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    std::cout<<"In DecimalColumnWriter"<<std::endl;
    auto columnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }
    long* values = columnVector->vector;

    int curPartLength;         // size of the partition which belongs to current pixel
    int curPartOffset = 0;     // starting offset of the partition which belongs to current pixel
    int nextPartLength = size; // size of the partition which belongs to next pixel

    // do the calculation to partition the vector into current pixel and next one
    // doing this pre-calculation to eliminate branch prediction inside the for loop
    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartDecimal(columnVector, values, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartDecimal(columnVector, values, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}

void DecimalColumnWriter::close()
{
    if (runlengthEncoding && encoder)
    {
        encoder->clear();
    }
    ColumnWriter::close();
}
void DecimalColumnWriter::writeCurPartDecimal(std::shared_ptr<ColumnVector> columnVector, long *values, int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++)
    {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset])
        {
            hasNull = true;
            if (nullsPadding)
            {
                // padding 0 for nulls
                curPixelVector[curPixelVectorIndex++] = 0L;
            }
        }
        else
        {
            curPixelVector[curPixelVectorIndex++] = values[i + curPartOffset];
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

bool DecimalColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption)
{
    if (writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2))
    {
        return false;
    }
    return writerOption->isNullsPadding();
}

void DecimalColumnWriter::newPixel()
{

    {
        std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
        EncodingUtils encodingUtils;
        {
            curVecPartitionBuffer = std::make_shared<ByteBuffer>(curPixelVectorIndex * sizeof(long));
            if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
            {
                for (int i = 0; i < curPixelVectorIndex; i++)
                {
                    encodingUtils.writeLongLE(curVecPartitionBuffer, curPixelVector[i]);
                }
            }
            else
            {
                for (int i = 0; i < curPixelVectorIndex; i++)
                {
                    encodingUtils.writeLongBE(curVecPartitionBuffer, curPixelVector[i]);
                }
            }
        }
        outputStream->putBytes(curVecPartitionBuffer->getPointer(), curVecPartitionBuffer->getWritePos());
    }

    ColumnWriter::newPixel();
}

pixels::proto::ColumnEncoding DecimalColumnWriter::getColumnChunkEncoding() const
{
    pixels::proto::ColumnEncoding columnEncoding;
    if (runlengthEncoding)
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
    }
    else
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    }
    return columnEncoding;
}