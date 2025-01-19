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
#include "writer/StringColumnWriter.h"

StringColumnWriter::StringColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption), curPixelVector(pixelStride) {
    std::cout << "Entering StringColumnWriter constructor" << std::endl;
    encodingUtils = std::make_shared<EncodingUtils>();
    runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
    if (runlengthEncoding) {
        encoder = std::make_unique<RunLenIntEncoder>();
    }
    startsArray = std::make_shared<DynamicIntArray>();
    std::cout << "StringColumnWriter constructed" << std::endl;
}

void StringColumnWriter::flush() {
    std::cout << "Entering StringColumnWriter::flush" << std::endl;
    ColumnWriter::flush();
    flushStarts();
    std::cout << "Exiting StringColumnWriter::flush" << std::endl;
}

void StringColumnWriter::flushStarts() {
    std::cout << "Entering StringColumnWriter::flushStarts" << std::endl;
    int startsFieldOffset = outputStream->size();
    startsArray->add(startOffset);
    std::cout << "StartsFieldOffset: " << startsFieldOffset << ", startsArray size: " << startsArray->size() << std::endl;

    if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN) {
        for (int i = 0; i < startsArray->size(); i++) {
            encodingUtils->writeIntLE(outputStream, startsArray->get(i));
        }
    } else {
        for (int i = 0; i < startsArray->size(); i++) {
            encodingUtils->writeIntBE(outputStream, startsArray->get(i));
        }
    }

    startsArray->clear();
    std::shared_ptr<ByteBuffer> offsetBuffer = std::make_shared<ByteBuffer>(4);
    offsetBuffer->putInt(startsFieldOffset);
    outputStream->putBytes(offsetBuffer->getPointer(), offsetBuffer->getWritePos());
    std::cout << "Exiting StringColumnWriter::flushStarts" << std::endl;
}

int StringColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length) {
    std::cout << "Entering StringColumnWriter::write with length: " << length << std::endl;
    auto columnVector = std::dynamic_pointer_cast<BinaryColumnVector>(vector);
    auto values = columnVector->vector;
    auto vLens = columnVector->lens;
    auto vOffsets = columnVector->start;

    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = length;

    // directly add to outputStream if not using dictionary encoding
    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
        curPartLength = pixelStride - curPixelIsNullIndex;
        std::cout << "Writing current part without dictionary encoding, curPartLength: " << curPartLength << std::endl;
        writeCurPartWithoutDict(columnVector, values, vLens, vOffsets, curPartLength, curPartOffset);
        newPixels();
        curPartOffset += curPartLength;
        nextPartLength = length - curPartOffset;
    }

    curPartLength = nextPartLength;
    std::cout << "Writing final part, curPartLength: " << curPartLength << std::endl;
    writeCurPartWithoutDict(columnVector, values, vLens, vOffsets, curPartLength, curPartOffset);

    std::cout << "Exiting StringColumnWriter::write, outputStream size: " << outputStream->size() << std::endl;
    return outputStream->size();
}

void StringColumnWriter::writeCurPartWithoutDict(std::shared_ptr<BinaryColumnVector> columnVector, duckdb::string_t* values,
                                                 int* vLens, int* vOffsets, int curPartLength, int curPartOffset) {
    std::cout << "Entering StringColumnWriter::writeCurPartWithoutDict for curPartLength: " << curPartLength << std::endl;
    for (int i = 0; i < curPartLength; i++) {
        curPixelEleIndex++;
        if (columnVector->isNull[curPartOffset + i]) {
            hasNull = true;
            pixelStatRecorder.increment();
            if (nullsPadding) {
                startsArray->add(0);
            }
        } else {
            // Extract the string data using `GetData()`
            const char* data = values[curPartOffset + i].GetData();
            uint32_t len = vLens[curPartOffset + i];
            uint32_t offset = vOffsets[curPartOffset + i];
            
            // Use ByteBuffer's putBytes method to write the string data
            std::cout << "Writing string data, length: " << len << ", offset: " << offset << std::endl;
            outputStream->putBytes(reinterpret_cast<uint8_t*>(const_cast<char*>(data)), len);
            
            startsArray->add(startOffset);
            startOffset += vLens[curPartOffset + i];
        }
    }
    for (int i = 0; i < curPartLength; ++i) {
        isNull[curPixelIsNullIndex + i] = (bool) columnVector->isNull[curPartOffset + i];
    }
    curPixelIsNullIndex += curPartLength;
    std::cout << "Exiting StringColumnWriter::writeCurPartWithoutDict" << std::endl;
}

void StringColumnWriter::newPixels() {
    std::cout << "Entering StringColumnWriter::newPixels" << std::endl;
    ColumnWriter::newPixel();
    std::cout << "Exiting StringColumnWriter::newPixels" << std::endl;
}

void StringColumnWriter::close() {
    std::cout << "Entering StringColumnWriter::close" << std::endl;
    startsArray->clear();
    ColumnWriter::close();
    std::cout << "Exiting StringColumnWriter::close" << std::endl;
}
