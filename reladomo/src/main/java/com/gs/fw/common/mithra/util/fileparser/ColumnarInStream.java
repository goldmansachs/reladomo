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


import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ColumnarInStream
{
    private InputStream in;

    public ColumnarInStream(InputStream dataIn)
    {
        this.in = dataIn;
    }

    public int read() throws IOException
    {
        return in.read();
    }

    public BitsInBytes decodeColumnarNull(SingleColumnAttribute attr, List data) throws IOException
    {
        int hasNulls = in.read();
        if (hasNulls == 0)
        {
            return null;
        }
        BitsInBytes result = BitsInBytes.readFromInputStream(in, data.size());
        Attribute nullableAttribute = (Attribute) attr;
        for(int i=0;i<data.size();i++)
        {
            if (result.get(i))
            {
                nullableAttribute.setValueNull(data.get(i));
            }
        }
        return result;
    }

    public boolean fullyRead(byte[] bytes) throws IOException
    {
        int read = 0;
        while (read < bytes.length)
        {
            int localRead = in.read(bytes, read, bytes.length - read);
            if (localRead == -1)
            {
                return false;
            }
            read += localRead;
        }
        return true;
    }

    public int readInt() throws IOException
    {
        int ch1 = readWithException();
        int ch2 = readWithException();
        int ch3 = readWithException();
        int ch4 = readWithException();
        return (ch1 << 24) | (ch2 << 16) | (ch3 << 8) | ch4;
    }

    public final long readLong() throws IOException
    {
        return (((long)readWithException() << 56) |
                ((long)(readWithException() & 0xFF) << 48) |
                ((long)(readWithException() & 0xFF) << 40) |
                ((long)(readWithException() & 0xFF) << 32) |
                ((long)(readWithException() & 0xFF) << 24) |
                ((long)(readWithException() & 0xFF) << 16) |
                ((long)(readWithException() & 0xFF) <<  8) |
                ((long)(readWithException() & 0xFF)));
    }

    public byte readWithExceptionAsByte() throws IOException
    {
        return (byte) (readWithException() & 0xFF);
    }
    public int readWithException() throws IOException
    {
        int result = in.read();
        if (result == -1)
        {
            throw new EOFException();
        }
        return result;
    }

    public InputStream getInputStream()
    {
        return in;
    }

    public void setTimezoneId(String timezoneId)
    {
        //ignore for now
    }
}
