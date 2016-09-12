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

package com.gs.fw.common.mithra.notification.server;

import java.io.OutputStream;
import java.io.IOException;


public class BlockOutputStream extends OutputStream
{
    protected OutputStream out;
    protected byte buf[];

    protected int count;

    public BlockOutputStream(OutputStream out, int size)
    {
        if (size <= 0)
        {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        this.out = out;
        buf = new byte[size];
    }

    private void flushBuffer() throws IOException
    {
        if (count > 0)
        {
            out.write(buf, 0, count);
            count = 0;
        }
    }

    public synchronized void write(int b) throws IOException
    {
        buf[count++] = (byte) b;
        if (count >= buf.length)
        {
            flushBuffer();
        }
    }

    public synchronized void write(byte b[], int off, int len) throws IOException
    {
        int bufLength = buf.length;
        int left = bufLength - count;
        if (len >= left)
        {
            System.arraycopy(b, off, buf, count, left);
            count += left;
            flushBuffer();
            len -= left;
            off += left;
            int blocksToWrite = len/ bufLength;
            if (blocksToWrite > 0)
            {
                int toWrite = blocksToWrite * bufLength;
                out.write(b, off, toWrite);
                len -= toWrite;
                off += toWrite;
            }
        }
        if (len > 0)
        {
            System.arraycopy(b, off, buf, count, len);
            count += len;
        }
    }

    public synchronized void flush() throws IOException
    {
        flushBuffer();
        out.flush();
    }
}
