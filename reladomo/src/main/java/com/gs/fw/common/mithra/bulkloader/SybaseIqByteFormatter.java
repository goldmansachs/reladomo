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

package com.gs.fw.common.mithra.bulkloader;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;


public class SybaseIqByteFormatter extends SybaseIqOutputFormatter
{

    public SybaseIqByteFormatter(boolean nullable, String columnName)
    {
        super(nullable, columnName, "byte");
    }

    public void write(BigDecimal obj, OutputStream os) throws IOException
    {
        write(obj.byteValue(), os);
    }

    public void write(byte b, OutputStream os) throws IOException
    {
        os.write(b);
        writePrimitiveNullTerminator(os);
    }

    public void write(double d, OutputStream os) throws IOException
    {
        write((byte) d, os);
    }

    public void write(float f, OutputStream os) throws IOException
    {
        write((byte)f, os);
    }

    public void write(int i, OutputStream os) throws IOException
    {
        write((byte)i, os);
    }

    public void write(long l, OutputStream os) throws IOException
    {
        write((byte)l, os);
    }

    public void write(short s, OutputStream os) throws IOException
    {
        write((byte)s, os);
    }

    @Override
    public void write(boolean b, OutputStream os) throws IOException
    {
        write((byte)(b ? 1 : 0), os);
    }

    public void writeNull(OutputStream os) throws IOException
    {
        checkNullability();
        os.write(0);
        writePrimitiveNullTerminatorForNull(os);
    }

    public String getColumnSpec(boolean previousNullByte)
    {
        return getPrimitiveColumnSpec();
    }

    public boolean hasNullByte()
    {
        return this.isNullable();
    }
}
