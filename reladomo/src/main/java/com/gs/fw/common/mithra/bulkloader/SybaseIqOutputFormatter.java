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

import com.gs.fw.common.mithra.attribute.OutputStreamFormatter;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;


public abstract class SybaseIqOutputFormatter implements OutputStreamFormatter
{

    protected static final byte QUOTE = "'".getBytes()[0];
    protected static final byte SPACE = " ".getBytes()[0];
    protected static final byte DELIMITER = ",".getBytes()[0];
    
    private boolean isNullable;
    private String columnName;
    private String type;
    protected static final String ISO_8859_1 = "ISO-8859-1";

    protected SybaseIqOutputFormatter(boolean nullable, String columnName, String type)
    {
        isNullable = nullable;
        this.columnName = columnName;
        this.type = type;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public boolean isNullable()
    {
        return isNullable;
    }

    protected void throwConversionError(String from)
    {
        throw new RuntimeException("cannot convert column "+columnName+" of type "+from+" to type "+type);
    }

    protected void writeLong(long v, OutputStream os) throws IOException
    {
        os.write((byte)v);
        os.write((byte)(v >>>  8));
        os.write((byte)(v >>> 16));
        os.write((byte)(v >>> 24));
        os.write((byte)(v >>> 32));
        os.write((byte)(v >>> 40));
        os.write((byte)(v >>> 48));
        os.write((byte)(v >>> 56));
    }

    protected void writeInt(int v, OutputStream os) throws IOException
    {
        os.write((byte) v);
        os.write((byte)(v >>>  8));
        os.write((byte)(v >>> 16));
        os.write((byte)(v >>> 24));
    }

    public void write(Object obj, OutputStream os) throws IOException
    {
        throwConversionError(obj.getClass().getName());
    }

    public void write(BigDecimal obj, OutputStream os) throws IOException
    {
        throwConversionError("BigDecimal");
    }

    public void write(boolean b, OutputStream os) throws IOException
    {
        throwConversionError("boolean");
    }

    public void write(byte b, OutputStream os) throws IOException
    {
        throwConversionError("byte");
    }

    public void write(char c, OutputStream os) throws IOException
    {
        throwConversionError("char");
    }

    public void write(double d, OutputStream os) throws IOException
    {
        throwConversionError("double");
    }

    public void write(float f, OutputStream os) throws IOException
    {
        throwConversionError("float");
    }

    public void write(int i, OutputStream os) throws IOException
    {
        throwConversionError("int");
    }

    public void write(long l, OutputStream os) throws IOException
    {
        throwConversionError("long");
    }

    public void write(short s, OutputStream os) throws IOException
    {
        throwConversionError("short");
    }

    protected void writeNonPrimitiveNull(OutputStream os) throws IOException
    {
        checkNullability();
        os.write(0);
        os.write(DELIMITER);
    }

    protected void checkNullability()
    {
        if (!isNullable())
        {
            throw new RuntimeException("the column "+this.columnName+" cannot be null");
        }
    }

    protected void writeUnquotedString(String s, OutputStream os) throws IOException
    {
        byte[] bytes = s.getBytes(ISO_8859_1);
        for(int i=0;i<bytes.length;i++)
        {
            os.write(bytes[i]);
            if (bytes[i] == QUOTE) os.write(QUOTE);
        }
    }

    protected String getPrimitiveColumnSpec()
    {
        String result = this.columnName + " BINARY";
//        if (this.isNullable) result += " WITH NULL BYTE";
        if (this.isNullable) result += " WITH NULL BYTE, FILLER(',')";
        return result;
    }

    protected String getAsciiColumnSpec()
    {
        String result = this.getColumnName() + " ','";
        if (this.isNullable())
        {
            result += " NULL(ZEROS)";
        }
        return result;
    }

    public boolean hasNullByte()
    {
        return false;
    }

    protected void writePrimitiveNullTerminator(OutputStream os)
            throws IOException
    {
        if (this.isNullable())
        {
            os.write(0);
            os.write(0);
            os.write(DELIMITER);
        }
    }

    protected void writePrimitiveNullTerminatorForNull(OutputStream os) throws IOException
    {
        os.write(1);
        os.write(1);
        os.write(DELIMITER);
    }
}
