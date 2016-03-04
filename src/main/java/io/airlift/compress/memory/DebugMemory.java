/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.memory;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class DebugMemory
        implements Memory
{
    @Override
    public Object getBaseObject(byte[] byteArray, int offset, int length)
    {
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);
        buffer.position(offset).limit(offset + length);
        return buffer.slice()
                .order(LITTLE_ENDIAN);
    }

    @Override
    public int getBaseAddress(byte[] byteArray, int offset)
    {
        return 0;
    }

    @Override
    public int getBaseLimit(byte[] byteArray, int offset, int length)
    {
        return length;
    }

    @Override
    public Object getBaseObject(ByteBuffer buffer)
    {
        return buffer.slice().order(LITTLE_ENDIAN);
    }

    @Override
    public long getBaseAddress(ByteBuffer buffer)
    {
        return 0;
    }

    @Override
    public long getBaseLimit(ByteBuffer buffer)
    {
        return buffer.remaining();
    }

    @Override
    public byte getByte(Object base, long offset)
    {
        return ((ByteBuffer) base).get(positiveInt(offset));
    }

    @Override
    public void putByte(Object base, long offset, byte value)
    {
        ((ByteBuffer) base).put(positiveInt(offset), value);
    }

    @Override
    public short getShort(Object base, long offset)
    {
        return ((ByteBuffer) base).getShort(positiveInt(offset));
    }

    @Override
    public void putShort(Object base, long offset, short value)
    {
        ((ByteBuffer) base).putShort(positiveInt(offset), value);
    }

    @Override
    public int getInt(Object base, long offset)
    {
        return ((ByteBuffer) base).getInt(positiveInt(offset));
    }

    @Override
    public void putInt(Object base, long offset, int value)
    {
        ((ByteBuffer) base).putInt(positiveInt(offset), value);
    }

    @Override
    public long getLong(Object base, long offset)
    {
        return ((ByteBuffer) base).getLong(positiveInt(offset));
    }

    @Override
    public void putLong(Object base, long offset, long value)
    {
        ((ByteBuffer) base).putLong(positiveInt(offset), value);
    }

    @Override
    public void copyMemory(Object source, long sourceOffset, Object destination, long destinationOffset, long length)
    {
        ByteBuffer sourceBuffer = ((ByteBuffer) source).duplicate();
        sourceBuffer.position(positiveInt(sourceOffset));
        sourceBuffer.limit(positiveInt(sourceOffset) + positiveInt(length));

        ByteBuffer destinationBuffer = ((ByteBuffer) destination).duplicate();
        destinationBuffer.position(positiveInt(destinationOffset));
        destinationBuffer.limit(positiveInt(destinationOffset) + positiveInt(length));

        destinationBuffer.put(sourceBuffer);
    }

    private static int positiveInt(long offset)
    {
        if (offset < 0 || offset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("invalid offset");
        }
        return (int) offset;
    }
}
