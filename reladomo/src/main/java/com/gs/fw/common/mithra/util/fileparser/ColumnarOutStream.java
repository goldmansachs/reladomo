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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class ColumnarOutStream
{
    private OutputStream out;

    public ColumnarOutStream(OutputStream dataOut)
    {
        this.out = dataOut;
    }

    public void write(byte[] b) throws IOException
    {
        out.write(b);
    }

    public void write(int b) throws IOException
    {
        out.write(b);
    }

    public BitsInBytes encodeColumnarNull(List data, SingleColumnAttribute attr) throws IOException
    {
        BitsInBytes result = null;
        Attribute a = (Attribute) attr;
        for(int i=0;i<data.size();i++)
        {
            if (a.isAttributeNull(data.get(i)))
            {
                if (result == null)
                {
                    result = new BitsInBytes(data.size());
                }
                result.set(i);
            }
        }
        if (result == null)
        {
            out.write(0);
        }
        else
        {
            out.write(1);
            result.writeToOutputStream(out);
        }
        return result;
    }

    public void writeInt(int i) throws IOException
    {
        out.write((i >>> 24) & 0xFF);
        out.write((i >>> 16) & 0xFF);
        out.write((i >>>  8) & 0xFF);
        out.write(i & 0xFF);
    }


    public OutputStream getOutputStream()
    {
        return out;
    }

    public final void writeLong(long v) throws IOException
    {
        out.write((byte)(v >>> 56));
        out.write((byte)(v >>> 48));
        out.write((byte)(v >>> 40));
        out.write((byte)(v >>> 32));
        out.write((byte)(v >>> 24));
        out.write((byte)(v >>> 16));
        out.write((byte)(v >>>  8));
        out.write((byte)v);
    }
}
