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


public class SybaseIqDecimalFormatter extends SybaseIqOutputFormatter
{

    private int scale;

    public SybaseIqDecimalFormatter(boolean nullable, String columnName, int scale)
    {
        super(nullable, columnName, "decimal");
        this.scale = scale;
    }

    public void write(BigDecimal obj, OutputStream os) throws IOException
    {
        BigDecimal decimal = obj.setScale(this.scale, BigDecimal.ROUND_HALF_UP);
        os.write(decimal.toString().getBytes(ISO_8859_1));
        os.write(DELIMITER);
    }

    public void write(byte b, OutputStream os) throws IOException
    {
        write((double) b, os);
    }

    public void write(double d, OutputStream os) throws IOException
    {
        write(new BigDecimal(d), os);
    }

    public void write(float f, OutputStream os) throws IOException
    {
        write((double)f, os);
    }

    public void write(int i, OutputStream os) throws IOException
    {
        write((double)i, os);
    }

    public void write(long l, OutputStream os) throws IOException
    {
        write((double)l, os);
    }

    public void write(short s, OutputStream os) throws IOException
    {
        write((double)s, os);
    }

    @Override
    public void write(boolean b, OutputStream os) throws IOException
    {
        write(b ? 1.0 : 0.0, os);
    }

    public void writeNull(OutputStream os) throws IOException
    {
        writeNonPrimitiveNull(os);
    }

    public String getColumnSpec(boolean previousNullByte)
    {
//        return previousNullByte ? "FILLER(1), "+getAsciiColumnSpec() : getAsciiColumnSpec();
        return getAsciiColumnSpec();
    }
}
