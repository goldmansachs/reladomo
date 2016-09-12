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

package com.gs.fw.common.mithra.util.fileparser;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class BitsInBytes
{
    private byte[] stuff;

    public BitsInBytes(int size)
    {
        stuff = new byte[numBytes(size)];
    }

    protected static int numBytes(int size)
    {
        return (size >> 3) + ((size & 7) > 0 ? 1 : 0);
    }

    public void set(int index)
    {
        int pos = index >> 3;
        int inByte = index & 7;
        stuff[pos] |= (1 << inByte);
    }

    public boolean get(int index)
    {
        int pos = index >> 3;
        int inByte = index & 7;
        return (stuff[pos] & (1 << inByte)) != 0;
    }

    public void writeToOutputStream(OutputStream out) throws IOException
    {
        out.write(this.stuff);
    }

    public int size()
    {
        return stuff.length << 3;
    }

    public static BitsInBytes readFromInputStream(InputStream in, int size) throws IOException
    {
        int actualSize = numBytes(size);
        BitsInBytes result = new BitsInBytes(size);
        if (!fullyRead(in, result.stuff))
        {
            throw new IOException("Expecting more bytes");
        }
        return result;
    }

    public static boolean fullyRead(InputStream inputStream, byte[] bytes) throws IOException
    {
        int read = 0;
        while (read < bytes.length)
        {
            int localRead = inputStream.read(bytes, read, bytes.length - read);
            if (localRead == -1)
            {
                return false;
            }
            read += localRead;
        }
        return true;
    }
}
