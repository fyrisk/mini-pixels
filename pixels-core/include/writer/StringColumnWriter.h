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

//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_STRINGCOLUMNWRITER_H
#define DUCKDB_STRINGCOLUMNWRITER_H

#include "ColumnWriter.h"
#include "utils/DynamicIntArray.h"
#include "utils/EncodingUtils.h"
#include "encoding/RunLenIntEncoder.h"
#include <memory>
#include <vector>
#include <cstdint>
class StringColumnWriter : public ColumnWriter {
public:
  StringColumnWriter(std::shared_ptr<TypeDescription> type,std::shared_ptr<PixelsWriterOption> writerOption);

  // vector should be converted to BinaryColumnVector
  int write(std::shared_ptr<ColumnVector> vector,int length) override;
  void close() override;
  void newPixels() ;

  bool decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) override {};

  void writeCurPartWithoutDict(std::shared_ptr<BinaryColumnVector> writerOption,duckdb::string_t * values,
                               int* vLens,int* vOffsets,int curPartLength,int curPartOffset);

  void flush() override;

  void flushStarts();



    std::vector<long> curPixelVector;
    bool runlengthEncoding;
    bool dictionaryEncoding;
    std::shared_ptr<DynamicIntArray> startsArray;
    std::shared_ptr<EncodingUtils> encodingUtils;
    std::unique_ptr<RunLenIntEncoder> encoder;
    int startOffset = 0;
    int curPixelIsNullIndex = 0;
    bool hasNull = false;


};
#endif // DUCKDB_STRINGCOLUMNWRITER_H
