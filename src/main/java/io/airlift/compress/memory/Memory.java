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

public interface Memory
{
    Object getBaseObject(byte[] byteArray, int offset, int length);

    int getBaseAddress(byte[] byteArray, int offset);

    int getBaseLimit(byte[] byteArray, int offset, int length);

    Object getBaseObject(ByteBuffer buffer);

    long getBaseAddress(ByteBuffer buffer);

    long getBaseLimit(ByteBuffer buffer);

    byte getByte(Object base, long offset);

    void putByte(Object base, long offset, byte value);

    short getShort(Object base, long offset);

    void putShort(Object base, long offset, short value);

    int getInt(Object base, long offset);

    void putInt(Object base, long offset, int value);

    long getLong(Object base, long offset);

    void putLong(Object base, long offset, long value);

    void copyMemory(Object source, long sourceOffset, Object destination, long destinationOffset, long length);
}
