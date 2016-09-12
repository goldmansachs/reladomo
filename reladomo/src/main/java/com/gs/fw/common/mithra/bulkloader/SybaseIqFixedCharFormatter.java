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


public class SybaseIqFixedCharFormatter extends SybaseIqOutputFormatter
{

    private int length;

    public SybaseIqFixedCharFormatter(boolean nullable, String columnName, int length)
    {
        super(nullable, columnName, "char");
        this.length = length;
    }

    public void writeNull(OutputStream os) throws IOException
    {
        writeNonPrimitiveNull(os);
    }

    @Override
    public void write(char c, OutputStream os) throws IOException
    {
        this.write(Character.toString(c), os);
    }

    @Override
    public void write(Object obj, OutputStream os) throws IOException
    {
        if (obj instanceof String)
        {
            String s = (String) obj;
            if (s.length() > length) s = s.substring(0, length);
            os.write(QUOTE);
            super.writeUnquotedString(s, os);
            int spaces = this.length - s.length();
            while(spaces > 0)
            {
                spaces--;
                os.write(SPACE);
            }
            os.write(QUOTE);
            os.write(DELIMITER);
        }
        else
        {
            throwConversionError("char");
        }
    }

    public String getColumnSpec(boolean previousNullByte)
    {
        return getAsciiColumnSpec();
//        return previousNullByte ? "FILLER(1), "+getAsciiColumnSpec() : getAsciiColumnSpec();
    }
}
