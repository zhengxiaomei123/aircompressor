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

import java.nio.ByteOrder;

public final class MemoryFactory
{
    private MemoryFactory() {}

    public static Memory createDefaultMemoryInstance(sun.misc.Unsafe unsafe)
    {
        if (unsafe == null) {
            throw new NullPointerException("unsafe is null");
        }

        // The current compressor code assumes the machine is little endian, and will
        // not work correctly on big endian CPUs.
        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
            throw new IllegalStateException("Only little endian computers are supported");
        }

        Memory memory;
        if ("true".equalsIgnoreCase(System.getProperty("io.airlift.compress.debug"))) {
            memory = new DebugMemory();
        }
        else {
            memory = new UnsafeMemory(unsafe);
        }

        byte[] testData = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        if ((memory.getByte(memory.getBaseObject(testData, 1, 1), memory.getBaseAddress(testData, 1)) & 0xFF) != 0x02) {
            throw new AssertionError("Memory implementation is broken!");
        }
        if ((memory.getShort(memory.getBaseObject(testData, 1, 2), memory.getBaseAddress(testData, 1)) & 0xFFFF) != 0x0302) {
            throw new AssertionError("Memory implementation is broken!");
        }
        if (memory.getInt(memory.getBaseObject(testData, 1, 4), memory.getBaseAddress(testData, 1)) != 0x05040302) {
            throw new AssertionError("Memory implementation is broken!");
        }
        if (memory.getLong(memory.getBaseObject(testData, 1, 8), memory.getBaseAddress(testData, 1)) != 0x09080706_05040302L) {
            throw new AssertionError("Memory implementation is broken!");
        }

        return memory;
    }
}
