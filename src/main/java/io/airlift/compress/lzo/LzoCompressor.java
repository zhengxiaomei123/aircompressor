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
package io.airlift.compress.lzo;

import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.airlift.compress.lzo.UnsafeUtil.UNSAFE;

/**
 * This class is not thread-safe
 */
public class LzoCompressor
    implements Compressor
{
    private final int[] table = new int[LzoRawCompressor.STREAM_SIZE];

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return LzoRawCompressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        Arrays.fill(table, 0);

        Object inputBase = UNSAFE.getBaseObject(input, inputOffset, inputLength);
        long inputAddress = UNSAFE.getBaseAddress(input, inputOffset);
        long inputLimit = UNSAFE.getBaseLimit(input, inputOffset, inputLength);
        Object outputBase = UNSAFE.getBaseObject(output, outputOffset, maxOutputLength);
        long outputAddress = UNSAFE.getBaseAddress(output, outputOffset);
        long outputLimit = UNSAFE.getBaseLimit(output, outputOffset, maxOutputLength);

        return LzoRawCompressor.compress(
                inputBase,
                inputAddress,
                (int) (inputLimit - inputAddress),
                outputBase,
                outputAddress,
                outputLimit - outputAddress,
                table);
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        Object inputBase = UNSAFE.getBaseObject(input);
        long inputAddress = UNSAFE.getBaseAddress(input);
        long inputLimit = UNSAFE.getBaseLimit(input);
        Object outputBase = UNSAFE.getBaseObject(output);
        long outputAddress = UNSAFE.getBaseAddress(output);
        long outputLimit = UNSAFE.getBaseLimit(output);

        // HACK: Assure JVM does not collect Slice wrappers while compressing, since the
        // collection may trigger freeing of the underlying memory resulting in a segfault
        // There is no other known way to signal to the JVM that an object should not be
        // collected in a block, and technically, the JVM is allowed to eliminate these locks.
        synchronized (input) {
            synchronized (output) {
                int written = LzoRawCompressor.compress(
                        inputBase,
                        inputAddress,
                        (int) (inputLimit - inputAddress),
                        outputBase,
                        outputAddress,
                        outputLimit - outputAddress,
                        table);
                output.position(output.position() + written);
            }
        }
    }
}
