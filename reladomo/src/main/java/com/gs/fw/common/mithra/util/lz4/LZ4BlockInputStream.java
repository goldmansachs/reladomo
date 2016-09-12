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

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import static com.gs.fw.common.mithra.util.lz4.LZ4BlockOutputStream.*;
import static com.gs.fw.common.mithra.util.lz4.LZ4Utils.*;

/**
 * {@link java.io.InputStream} implementation to decode data written with
 * {@link net.jpountz.lz4.LZ4BlockOutputStream}. This class is not thread-safe and does not
 * support {@link #mark(int)}/{@link #reset()}.
 *
 * @see net.jpountz.lz4.LZ4BlockOutputStream
 */
public final class LZ4BlockInputStream extends FilterInputStream
{

    private final Checksum checksum;
    private byte[] buffer;
    private byte[] compressedBuffer;
    private int originalLen;
    private int o;
    private boolean finished;

    /**
     * Create a new {@link java.io.InputStream}.
     *
     * @param in           the {@link java.io.InputStream} to poll
     */
    public LZ4BlockInputStream(InputStream in)
    {
        super(in);
        this.checksum = new Adler32();
        this.buffer = new byte[0];
        this.compressedBuffer = new byte[HEADER_LENGTH];
        o = originalLen = 0;
        finished = false;
    }

    public void reset(InputStream in)
    {
        this.in = in;
        Arrays.fill(buffer, (byte) 0);
        Arrays.fill(compressedBuffer, (byte) 0);
        o = originalLen = 0;
        finished = false;
    }

    @Override
    public int available() throws IOException
    {
        return originalLen - o;
    }

    @Override
    public int read() throws IOException
    {
        if (finished)
        {
            return -1;
        }
        if (o == originalLen)
        {
            refill();
        }
        if (finished)
        {
            return -1;
        }
        return buffer[o++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        if (finished)
        {
            return -1;
        }
        if (o == originalLen)
        {
            refill();
        }
        if (finished)
        {
            return -1;
        }
        len = Math.min(len, originalLen - o);
        System.arraycopy(buffer, o, b, off, len);
        o += len;
        return len;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public long skip(long n) throws IOException
    {
        if (finished)
        {
            return -1;
        }
        if (o == originalLen)
        {
            refill();
        }
        if (finished)
        {
            return -1;
        }
        final int skipped = (int) Math.min(n, originalLen - o);
        o += skipped;
        return skipped;
    }

    private void refill() throws IOException
    {
        readFully(compressedBuffer, HEADER_LENGTH);
        for (int i = 0; i < MAGIC_LENGTH; ++i)
        {
            if (compressedBuffer[i] != MAGIC[i])
            {
                throw new IOException("Stream is corrupted");
            }
        }
        final int token = compressedBuffer[MAGIC_LENGTH] & 0xFF;
        final int compressionMethod = token & 0xF0;
        final int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
        if (compressionMethod != COMPRESSION_METHOD_RAW && compressionMethod != COMPRESSION_METHOD_LZ4)
        {
            throw new IOException("Stream is corrupted");
        }
        final int compressedLen = readIntLE(compressedBuffer, MAGIC_LENGTH + 1);
        originalLen = readIntLE(compressedBuffer, MAGIC_LENGTH + 5);
        final int check = readIntLE(compressedBuffer, MAGIC_LENGTH + 9);
        assert HEADER_LENGTH == MAGIC_LENGTH + 13;
        if (originalLen > 1 << compressionLevel
                || originalLen < 0
                || compressedLen < 0
                || (originalLen == 0 && compressedLen != 0)
                || (originalLen != 0 && compressedLen == 0)
                || (compressionMethod == COMPRESSION_METHOD_RAW && originalLen != compressedLen))
        {
            throw new IOException("Stream is corrupted");
        }
        if (originalLen == 0 && compressedLen == 0)
        {
            if (check != 0)
            {
                throw new IOException("Stream is corrupted");
            }
            finished = true;
            return;
        }
        if (buffer.length < originalLen)
        {
            buffer = new byte[Math.max(originalLen, buffer.length * 3 / 2)];
        }
        switch (compressionMethod)
        {
            case COMPRESSION_METHOD_RAW:
                readFully(buffer, originalLen);
                break;
            case COMPRESSION_METHOD_LZ4:
                if (compressedBuffer.length < originalLen)
                {
                    compressedBuffer = new byte[Math.max(compressedLen, compressedBuffer.length * 3 / 2)];
                }
                readFully(compressedBuffer, compressedLen);
                try
                {
                    final int compressedLen2 = decompress(compressedBuffer, 0, buffer, 0, originalLen);
                    if (compressedLen != compressedLen2)
                    {
                        throw new IOException("Stream is corrupted");
                    }
                }
                catch (RuntimeException e)
                {
                    throw new IOException("Stream is corrupted", e);
                }
                break;
            default:
                throw new AssertionError();
        }
        checksum.reset();
        checksum.update(buffer, 0, originalLen);
        if ((int) checksum.getValue() != check)
        {
            throw new IOException("Stream is corrupted");
        }
        o = 0;
    }

    private void readFully(byte[] b, int len) throws IOException
    {
        int read = 0;
        while (read < len)
        {
            final int r = in.read(b, read, len - read);
            if (r < 0)
            {
                throw new EOFException("Stream ended prematurely");
            }
            read += r;
        }
        assert len == read;
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    @SuppressWarnings("sync-override")
    @Override
    public void mark(int readlimit)
    {
        // unsupported
    }

    @SuppressWarnings("sync-override")
    @Override
    public void reset() throws IOException
    {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(in=" + in
                + ", checksum=" + checksum + ")";
    }

    public int decompress(byte[] src, final int srcOff, byte[] dest, final int destOff, int destLen) throws IOException
    {
        if (destLen == 0)
        {
            if (src[srcOff] != 0)
            {
                throw new IOException("Malformed input at " + srcOff);
            }
            return 1;
        }

        final int destEnd = destOff + destLen;

        int sOff = srcOff;
        int dOff = destOff;

        while (true)
        {
            final int token = src[sOff++] & 0xFF;

            // literals
            int literalLen = token >>> ML_BITS;
            if (literalLen == RUN_MASK)
            {
                byte len;
                while ((len = src[sOff++]) == (byte) 0xFF)
                {
                    literalLen += 0xFF;
                }
                literalLen += len & 0xFF;
            }

            final int literalCopyEnd = dOff + literalLen;
            if (literalCopyEnd > destEnd - COPY_LENGTH)
            {
                if (literalCopyEnd != destEnd)
                {
                    throw new IOException("Malformed input at " + sOff);
                }
                else
                {
                    safeArraycopy(src, sOff, dest, dOff, literalLen);
                    sOff += literalLen;
                    break; // EOF
                }
            }

            wildArraycopy(src, sOff, dest, dOff, literalLen);
            sOff += literalLen;
            dOff = literalCopyEnd;

            // matchs
            final int matchDec = (src[sOff++] & 0xFF) | ((src[sOff++] & 0xFF) << 8);
            int matchOff = dOff - matchDec;

            if (matchOff < destOff)
            {
                throw new IOException("Malformed input at " + sOff);
            }

            int matchLen = token & ML_MASK;
            if (matchLen == ML_MASK)
            {
                byte len;
                while ((len = src[sOff++]) == (byte) 0xFF)
                {
                    matchLen += 0xFF;
                }
                matchLen += len & 0xFF;
            }
            matchLen += MIN_MATCH;

            final int matchCopyEnd = dOff + matchLen;

            if (matchCopyEnd > destEnd - COPY_LENGTH)
            {
                if (matchCopyEnd > destEnd)
                {
                    throw new IOException("Malformed input at " + sOff);
                }
                safeIncrementalCopy(dest, matchOff, dOff, matchLen);
            }
            else
            {
                wildIncrementalCopy(dest, matchOff, dOff, matchCopyEnd);
            }
            dOff = matchCopyEnd;
        }

        return sOff - srcOff;
    }

}
