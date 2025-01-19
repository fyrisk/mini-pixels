/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core.vector;

import io.pixelsdb.pixels.core.utils.Bitmap;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.utils.JvmUtils.unsafe;
import static io.pixelsdb.pixels.core.utils.BitUtils.longBytesToLong;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

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
 * You can also set data by value. You can mix "by value" and "by reference" in the
 * same column vector, though that use is probably not typical.
 */
public class BinaryColumnVector extends ColumnVector
{
    public byte[][] vector;
    public int[] start;          // start offset of each field

    /*
     * The length of each field. If the value repeats for every entry, then it is stored
     * in vector[0] and isRepeating from the superclass is set to true.
     */
    public int[] lens;

    // A call to increaseBufferSpace() or ensureValPreallocated() will ensure that buffer[] points to
    // a byte[] with sufficient space for the specified size.
    public byte[] buffer;   // optional buffer to use when actually copying in data
    private int nextFree;    // next free position in buffer

    // Hang onto a byte array for holding smaller byte values
    private byte[] smallBuffer;
    private int smallBufferNextFree;

    private int bufferAllocationCount = 0;

    // Estimate that there will be 16 bytes per entry
    static final int DEFAULT_BUFFER_SIZE = 16 * VectorizedRowBatch.DEFAULT_SIZE;

    // Proportion of extra space to provide when allocating more buffer space.
    static final float EXTRA_SPACE_FACTOR = (float) 1.2;

    // Largest size allowed in smallBuffer
    static final int MAX_SIZE_FOR_SMALL_BUFFER = 1024 * 1024;

    /**
     * Use this constructor for normal operation.
     */
    public BinaryColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    /**
     * @param size number of elements in the column vector
     */
    public BinaryColumnVector(int size)
    {
        super(size);
        vector = new byte[size][];
        start = new int[size];
        lens = new int[size];
        memoryUsage += Integer.BYTES * (size * 2 + 2);
    }

    /**
     * Additional reset work for BinaryColumnVector (releasing scratch bytes for by value strings).
     */
    @Override
    public void reset()
    {
        super.reset();
        // fill null to release memory.
        Arrays.fill(vector, null);
        resetBuffer();
    }

    /**
     * Set a field by reference.
     *
     * @param elementNum index within column vector to set
     * @param sourceBuf  container of source data
     * @param start      start byte position within source
     * @param length     length of source byte sequence
     */
    public void setRef(int elementNum, byte[] sourceBuf, int start, int length)
    {
        if (elementNum >= writeIndex)
        {
            writeIndex = elementNum + 1;
        }
        this.vector[elementNum] = sourceBuf;
        this.start[elementNum] = start;
        this.lens[elementNum] = length;
        this.isNull[elementNum] = sourceBuf == null;
        if (sourceBuf == null)
        {
            this.noNulls = false;
        }
    }

    /**
     * Provide the estimated number of bytes needed to hold
     * a full column vector worth of byte string data.
     *
     * @param estimatedValueSize Estimated size of buffer space needed
     */
    private void initBuffer(int estimatedValueSize)
    {
        nextFree = 0;
        smallBufferNextFree = 0;

        // if buffer is already allocated, keep using it, don't re-allocate
        if (buffer != null)
        {
            // Free up any previously allocated buffers that are referenced by vector
            if (bufferAllocationCount > 0)
            {
                Arrays.fill(vector, null);
                buffer = smallBuffer; // In case last row was a large bytes value
            }
        }
        else
        {
            // allocate a little extra space to limit need to re-allocate
            int bufferSize = this.vector.length * (int) (estimatedValueSize * EXTRA_SPACE_FACTOR);
            if (bufferSize < DEFAULT_BUFFER_SIZE)
            {
                bufferSize = DEFAULT_BUFFER_SIZE;
            }
            buffer = new byte[bufferSize];
            memoryUsage += Byte.BYTES * bufferSize;
            smallBuffer = buffer;
        }
        bufferAllocationCount = 0;
    }


    @Override
    public void add(byte[] v)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        setVal(writeIndex++, v);
    }

    @Override
    public void add(String value)
    {
        add(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Set a field by actually copying in to a local buffer.
     * If you must actually copy data in to the array, use this method.
     * DO NOT USE this method unless it's not practical to set data by reference with setRef().
     * Setting data by reference tends to run a lot faster than copying data in.
     *
     * @param elementNum index within column vector to set
     * @param sourceBuf  container of source data
     * @param start      start byte position within source
     * @param length     length of source byte sequence
     */
    public void setVal(int elementNum, byte[] sourceBuf, int start, int length)
    {
        if (elementNum >= writeIndex)
        {
            writeIndex = elementNum + 1;
        }
        if (buffer == null)
        {
            // Issue #367: Lazy allocation only when necessary (i.e., setVal is used instead of setRef).
            initBuffer(0);
        }
        else if ((nextFree + length) > buffer.length)
        {
            increaseBufferSpace(length);
        }
        System.arraycopy(sourceBuf, start, buffer, nextFree, length);
        this.vector[elementNum] = buffer;
        this.start[elementNum] = nextFree;
        this.lens[elementNum] = length;
        this.isNull[elementNum] = false;
        nextFree += length;
    }

    /**
     * Set a field by actually copying in to a local buffer.
     * If you must actually copy data in to the array, use this method.
     * DO NOT USE this method unless it's not practical to set data by reference with setRef().
     * Setting data by reference tends to run a lot faster than copying data in.
     *
     * @param elementNum index within column vector to set
     * @param sourceBuf  container of source data
     */
    public void setVal(int elementNum, byte[] sourceBuf)
    {
        setVal(elementNum, sourceBuf, 0, sourceBuf.length);
    }

    /**
     * Increase buffer space enough to accommodate next element.
     * This uses an exponential increase mechanism to rapidly
     * increase buffer size to enough to hold all data.
     * As batches get re-loaded, buffer space allocated will quickly
     * stabilize.
     *
     * @param nextElemLength size of next element to be added
     */
    public void increaseBufferSpace(int nextElemLength)
    {
        // A call to increaseBufferSpace() or ensureValPreallocated() will ensure that buffer[] points to
        // a byte[] with sufficient space for the specified size.
        // This will either point to smallBuffer, or to a newly allocated byte array for larger values.

        if (nextElemLength > MAX_SIZE_FOR_SMALL_BUFFER)
        {
            // Larger allocations will be special-cased and will not use the normal buffer.
            // buffer/nextFree will be set to a newly allocated array just for the current row.
            // The next row will require another call to increaseBufferSpace() since this new buffer should be used up.
            byte[] newBuffer = new byte[nextElemLength];
            memoryUsage += Byte.BYTES * nextElemLength;
            ++bufferAllocationCount;
            // If the buffer was pointing to smallBuffer, then nextFree keeps track of the current state
            // of the free index for smallBuffer. We now need to save this value to smallBufferNextFree
            // so we don't lose this. A bit of a weird dance here.
            if (smallBuffer == buffer)
            {
                smallBufferNextFree = nextFree;
            }
            buffer = newBuffer;
            nextFree = 0;
        }
        else
        {
            // This value should go into smallBuffer.
            if (smallBuffer != buffer)
            {
                // Previous row was for a large bytes value ( > MAX_SIZE_FOR_SMALL_BUFFER).
                // Use smallBuffer if possible.
                buffer = smallBuffer;
                nextFree = smallBufferNextFree;
            }

            // smallBuffer might still be out of space
            if ((nextFree + nextElemLength) > buffer.length)
            {
                int newLength = smallBuffer.length * 2;
                while (newLength < nextElemLength)
                {
                    if (newLength < 0)
                    {
                        throw new RuntimeException("Overflow of newLength. smallBuffer.length="
                                + smallBuffer.length + ", nextElemLength=" + nextElemLength);
                    }
                    newLength *= 2;
                }
                smallBuffer = new byte[newLength];
                memoryUsage += Byte.BYTES * newLength;
                ++bufferAllocationCount;
                smallBufferNextFree = 0;
                // Update buffer
                buffer = smallBuffer;
                nextFree = 0;
            }
        }
    }

    @Override
    public void ensureSize(int size, boolean preserveData)
    {
        super.ensureSize(size, preserveData);
        if (size > vector.length)
        {
            int[] oldStart = start;
            start = new int[size];
            int[] oldLength = lens;
            lens = new int[size];
            byte[][] oldVector = vector;
            vector = new byte[size][];
            memoryUsage += Integer.BYTES * size * 2;
            length = size;
            if (preserveData)
            {
                if (isRepeating)
                {
                    vector[0] = oldVector[0];
                    start[0] = oldStart[0];
                    lens[0] = oldLength[0];
                }
                else
                {
                    System.arraycopy(oldVector, 0, vector, 0, oldVector.length);
                    System.arraycopy(oldStart, 0, start, 0, oldStart.length);
                    System.arraycopy(oldLength, 0, length, 0, oldLength.length);
                }
            }
        }
    }

    @Override
    public void close()
    {
        super.close();
        this.start = null;
        this.lens = null;
        this.buffer = null;
        this.smallBuffer = null;
        if (this.vector != null)
        {
            for (int i = 0; i < this.vector.length; ++i)
            {
                this.vector[i] = null;
            }
            this.vector = null;
        }
    }
}