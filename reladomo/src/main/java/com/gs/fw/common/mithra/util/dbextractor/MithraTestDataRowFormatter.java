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

package com.gs.fw.common.mithra.util.dbextractor;

import com.gs.collections.api.block.function.Function;
import javax.xml.bind.DatatypeConverter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.NumberFormat;



public class MithraTestDataRowFormatter implements Function<Object, String>
{
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final NumberFormat NUMBER_FORMAT = new DecimalFormat("#.############");

    private static final String QUOTE = "\"";
    private static final String NULL = "null";

    public String valueOf(Object object)
    {
        if (object == null)
        {
            return NULL;
        }
        else if (object instanceof Boolean)
        {
            return object.toString();
        }
        else if (object instanceof Number)
        {
            return NUMBER_FORMAT.format(Double.valueOf(object.toString()));
        }
        else if (object instanceof Timestamp)
        {
            return QUOTE + DATE_FORMAT.print(((Timestamp)object).getTime()) + QUOTE;
        }
        else if (object instanceof byte[])
        {
            return QUOTE + DatatypeConverter.printHexBinary((byte[]) object) + QUOTE;
        }
        else
        {
            return QUOTE + escapedString(object) + QUOTE;
        }

    }

    private String escapedString(Object object)
    {
        String str = object.toString();
        StringBuilder builder = new StringBuilder(str.length());
        for(int i=0;i<str.length();i++)
        {
            char c = str.charAt(i);
            if (c == '\t')
            {
                builder.append('\\').append('t');
            }
            else if (c == '\r')
            {
                builder.append('\\').append('r');
            }
            else if (c == '\n')
            {
                builder.append('\\').append('n');
            }
            else if (c == '\"')
            {
                builder.append('\\').append('\"');
            }
            else if (c == '\\')
            {
                builder.append('\\').append('\\');
            }
            else if (c == '\b')
            {
                builder.append('\\').append('b');
            }
            else if (c == '\f')
            {
                builder.append('\\').append('f');
            }
            else
            {
                builder.append(c);
            }
        }
        return builder.toString();
    }
}
