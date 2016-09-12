/*
 Copyright 2016 Goldman Sachs.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */

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

/*
    Copyright Adrien Grand (based on Yann Collet's BSD licensed LZ4 implementation)
    changes copyright Goldman Sachs, licensed under Apache 2.0 license
*/
package com.gs.fw.common.mithra.util.lz4;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

/**
 * Streaming LZ4.
 * <p/>
 * This class compresses data into fixed-size blocks of compressed data.
 *
 * @see net.jpountz.lz4.LZ4BlockInputStream
 */
public final class LZ4BlockOutputStream extends FilterOutputStream
{

    static final byte[] MAGIC = new byte[]{'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
    static final int MAGIC_LENGTH = MAGIC.length;

    static final int HEADER_LENGTH =
            MAGIC_LENGTH // magic bytes
                    + 1          // token
                    + 4          // compressed length
                    + 4          // decompressed length
                    + 4;         // checksum

    static final int COMPRESSION_LEVEL_BASE = 10;
    static final int MIN_BLOCK_SIZE = 64;
    static final int MAX_BLOCK_SIZE = 1 << (COMPRESSION_LEVEL_BASE + 0x0F);

    static final int COMPRESSION_METHOD_RAW = 0x10;
    static final int COMPRESSION_METHOD_LZ4 = 0x20;

    static final int DEFAULT_SEED = 0x9747b28c;

    private static int compressionLevel(int blockSize)
    {
        if (blockSize < MIN_BLOCK_SIZE)
        {
            throw new IllegalArgumentException("blockSize must be >= " + MIN_BLOCK_SIZE + ", got " + blockSize);
        }
        else if (blockSize > MAX_BLOCK_SIZE)
        {
            throw new IllegalArgumentException("blockSize must be <= " + MAX_BLOCK_SIZE + ", got " + blockSize);
        }
        int compressionLevel = 32 - Integer.numberOfLeadingZeros(blockSize - 1); // ceil of log2
        assert (1 << compressionLevel) >= blockSize;
        assert blockSize * 2 > (1 << compressionLevel);
        compressionLevel = Math.max(0, compressionLevel - COMPRESSION_LEVEL_BASE);
        assert compressionLevel >= 0 && compressionLevel <= 0x0F;
        return compressionLevel;
    }

    private final int blockSize;
    private final int compressionLevel;
    private final LZ4HCJavaSafeCompressor compressor;
    private final Checksum checksum;
    private final byte[] buffer;
    private final byte[] compressedBuffer;
    private final boolean syncFlush;
    private boolean finished;
    private int o;

    public LZ4BlockOutputStream(OutputStream out, boolean syncFlush)
    {
        this(out, 64*1024, syncFlush);
    }
    /**
     * Create a new {@link java.io.OutputStream} with configurable block size. Large
     * blocks require more memory at compression and decompression time but
     * should improve the compression ratio.
     *
     * @param out        the {@link java.io.OutputStream} to feed
     * @param blockSize  the maximum number of bytes to try to compress at once,
     *                   must be >= 64 and <= 32 M
     * @param syncFlush  true if pending data should also be flushed on {@link #flush()}
     */
    public LZ4BlockOutputStream(OutputStream out, int blockSize, boolean syncFlush)
    {
        super(out);
        this.blockSize = blockSize;
        this.compressor = new LZ4HCJavaSafeCompressor();
        this.checksum = new Adler32();
        this.compressionLevel = compressionLevel(blockSize);
        this.buffer = new byte[blockSize];
        final int compressedBlockSize = HEADER_LENGTH + compressor.maxCompressedLength(blockSize);
        this.compressedBuffer = new byte[compressedBlockSize];
        this.syncFlush = syncFlush;
        o = 0;
        finished = false;
        System.arraycopy(MAGIC, 0, compressedBuffer, 0, MAGIC_LENGTH);
    }

    public void reset(OutputStream out)
    {
        this.out = out;
        Arrays.fill(buffer, (byte) 0);
        Arrays.fill(compressedBuffer, (byte) 0);
        o = 0;
        finished = false;
        System.arraycopy(MAGIC, 0, compressedBuffer, 0, MAGIC_LENGTH);
    }

    private void ensureNotFinished()
    {
        if (finished)
        {
            throw new IllegalStateException("This stream is already closed");
        }
    }

    @Override
    public void write(int b) throws IOException
    {
        ensureNotFinished();
        if (o == blockSize)
        {
            flushBufferedData();
        }
        buffer[o++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        ensureNotFinished();

        while (o + len > blockSize)
        {
            final int l = blockSize - o;
            System.arraycopy(b, off, buffer, o, blockSize - o);
            o = blockSize;
            flushBufferedData();
            off += l;
            len -= l;
        }
        System.arraycopy(b, off, buffer, o, len);
        o += len;
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        ensureNotFinished();
        write(b, 0, b.length);
    }

    @Override
    public void close() throws IOException
    {
        if (finished)
        {
            return;
        }
        ensureNotFinished();
        finish();
        out.close();
    }

    private void flushBufferedData() throws IOException
    {
        if (o == 0)
        {
            return;
        }
        checksum.reset();
        checksum.update(buffer, 0, o);
        final int check = (int) checksum.getValue();
        int compressedLength = compressor.compress(buffer, 0, o, compressedBuffer, HEADER_LENGTH);
        final int compressMethod;
        if (compressedLength >= o)
        {
            compressMethod = COMPRESSION_METHOD_RAW;
            compressedLength = o;
            System.arraycopy(buffer, 0, compressedBuffer, HEADER_LENGTH, o);
        }
        else
        {
            compressMethod = COMPRESSION_METHOD_LZ4;
        }

        compressedBuffer[MAGIC_LENGTH] = (byte) (compressMethod | compressionLevel);
        writeIntLE(compressedLength, compressedBuffer, MAGIC_LENGTH + 1);
        writeIntLE(o, compressedBuffer, MAGIC_LENGTH + 5);
        writeIntLE(check, compressedBuffer, MAGIC_LENGTH + 9);
        assert MAGIC_LENGTH + 13 == HEADER_LENGTH;
        out.write(compressedBuffer, 0, HEADER_LENGTH + compressedLength);
        o = 0;
    }

    /**
     * Flush this compressed {@link java.io.OutputStream}.
     * <p/>
     * If the stream has been created with <code>syncFlush=true</code>, pending
     * data will be compressed and appended to the underlying {@link java.io.OutputStream}
     * before calling {@link java.io.OutputStream#flush()} on the underlying stream.
     * Otherwise, this method just flushes the underlying stream, so pending
     * data might not be available for reading until {@link #finish()} or
     * {@link #close()} is called.
     */
    @Override
    public void flush() throws IOException
    {
        if (syncFlush)
        {
            flushBufferedData();
        }
        out.flush();
    }

    /**
     * Same as {@link #close()} except that it doesn't close the underlying stream.
     * This can be useful if you want to keep on using the underlying stream.
     */
    public void finish() throws IOException
    {
        ensureNotFinished();
        flushBufferedData();
        compressedBuffer[MAGIC_LENGTH] = (byte) (COMPRESSION_METHOD_RAW | compressionLevel);
        writeIntLE(0, compressedBuffer, MAGIC_LENGTH + 1);
        writeIntLE(0, compressedBuffer, MAGIC_LENGTH + 5);
        writeIntLE(0, compressedBuffer, MAGIC_LENGTH + 9);
        assert MAGIC_LENGTH + 13 == HEADER_LENGTH;
        out.write(compressedBuffer, 0, HEADER_LENGTH);
        finished = true;
        out.flush();
    }

    private static void writeIntLE(int i, byte[] buf, int off)
    {
        buf[off++] = (byte) i;
        buf[off++] = (byte) (i >>> 8);
        buf[off++] = (byte) (i >>> 16);
        buf[off++] = (byte) (i >>> 24);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(out=" + out + ", blockSize=" + blockSize
                + ", compressor=" + compressor + ", checksum=" + checksum + ")";
    }

}
