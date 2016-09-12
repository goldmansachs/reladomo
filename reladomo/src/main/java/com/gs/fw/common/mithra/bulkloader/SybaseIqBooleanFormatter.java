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


public class SybaseIqBooleanFormatter extends SybaseIqOutputFormatter
{

    public SybaseIqBooleanFormatter(boolean nullable, String columnName)
    {
        super(nullable, columnName, "boolean");
    }

    @Override
    public void write(boolean b, OutputStream os) throws IOException
    {
        os.write(b ? 1 : 0);
    }

    public void writeNull(OutputStream os) throws IOException
    {
        throw new RuntimeException("boolean cannot be null in column "+this.getColumnName());
    }

    public String getColumnSpec(boolean previousNullByte)
    {
        return this.getColumnName()+" BINARY";
    }

    @Override
    public void write(int i, OutputStream os) throws IOException
    {
        if (i == 0 || i == 1)
        {
            os.write(i);
        }
        else throwConversionError("int");
    }

    @Override
    public void write(byte i, OutputStream os) throws IOException
    {
        if (i == 0 || i == 1)
        {
            os.write(i);
        }
        else throwConversionError("byte");
    }

    @Override
    public void write(short i, OutputStream os) throws IOException
    {
        if (i == 0 || i == 1)
        {
            os.write(i);
        }
        else throwConversionError("short");
    }
}
