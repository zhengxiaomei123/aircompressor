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

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

@SuppressWarnings("unused")
public class UnsafeMemory
        implements Memory
{
    public final Unsafe unsafe;

    public UnsafeMemory(Unsafe unsafe)
    {
        if (unsafe == null) {
            throw new NullPointerException("unsafe is null");
        }
        this.unsafe = unsafe;
    }

    @Override
    public Object getBaseObject(byte[] byteArray, int offset, int length)
    {
        return byteArray;
    }

    @Override
    public int getBaseAddress(byte[] byteArray, int offset)
    {
        return ARRAY_BYTE_BASE_OFFSET + offset;
    }

    @Override
    public int getBaseLimit(byte[] byteArray, int offset, int length)
    {
        return getBaseAddress(byteArray, offset) + length;
    }

    @Override
    public Object getBaseObject(ByteBuffer buffer)
    {
        if (buffer instanceof DirectBuffer) {
            return null;
        }
        else if (buffer.hasArray()) {
            return buffer.array();
        }
        else {
            throw new IllegalArgumentException("Unsupported ByteBuffer implementation " + buffer.getClass().getName());
        }
    }

    @Override
    public long getBaseAddress(ByteBuffer buffer)
    {
        if (buffer instanceof DirectBuffer) {
            DirectBuffer direct = (DirectBuffer) buffer;
            return direct.address() + buffer.position();
        }
        else if (buffer.hasArray()) {
            return ARRAY_BYTE_BASE_OFFSET + buffer.arrayOffset() + buffer.position();
        }
        else {
            throw new IllegalArgumentException("Unsupported ByteBuffer implementation " + buffer.getClass().getName());
        }
    }

    @Override
    public long getBaseLimit(ByteBuffer buffer)
    {
        if (buffer instanceof DirectBuffer) {
            DirectBuffer direct = (DirectBuffer) buffer;
            return direct.address() + buffer.limit();
        }
        else if (buffer.hasArray()) {
            return ARRAY_BYTE_BASE_OFFSET + buffer.arrayOffset() + buffer.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported ByteBuffer implementation " + buffer.getClass().getName());
        }
    }

    @Override
    public byte getByte(Object base, long offset)
    {
        return unsafe.getByte(base, offset);
    }

    @Override
    public void putByte(Object base, long offset, byte value)
    {
        unsafe.putByte(base, offset, value);
    }

    @Override
    public short getShort(Object base, long offset)
    {
        return unsafe.getShort(base, offset);
    }

    @Override
    public void putShort(Object base, long offset, short value)
    {
        unsafe.putShort(base, offset, value);
    }

    @Override
    public int getInt(Object base, long offset)
    {
        return unsafe.getInt(base, offset);
    }

    @Override
    public void putInt(Object base, long offset, int value)
    {
        unsafe.putInt(base, offset, value);
    }

    @Override
    public long getLong(Object base, long offset)
    {
        return unsafe.getLong(base, offset);
    }

    @Override
    public void putLong(Object base, long offset, long value)
    {
        unsafe.putLong(base, offset, value);
    }

    @Override
    public void copyMemory(Object source, long sourceOffset, Object destination, long destinationOffset, long length)
    {
        unsafe.copyMemory(source, sourceOffset, destination, destinationOffset, length);
    }
}
