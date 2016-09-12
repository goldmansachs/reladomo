
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

import org.slf4j.Logger;
import com.gs.fw.common.mithra.attribute.ToStringFormatter;


public class VarCharFormatter extends ToStringFormatter
{

    protected final Logger logger;
    protected final String columnName;
    protected final int length;

    public VarCharFormatter(Logger sqlLogger, String columnName, int length)
    {
        this.logger = sqlLogger;
        this.columnName = columnName;
        this.length = length;
    }

    public String format(char c)
    {
        return Character.toString(c);
    }

    public String format(Object obj)
    {
        // Return nothing for a null object
        if (obj == null)
        {
            return "";
        }

        String result;

        // There might be a Character here hence the call to toString rather than just a cast
        String original = obj.toString();
        int originalLength = original.length();

        if (originalLength > this.length)
        {
            if (logger.isWarnEnabled())
            {
                logger.warn("Overflow in '" + this.columnName + "' ('" + original + "'). Trimming down to " + this.length + " characters.");
            }

            result = original.substring(0, this.length);
        }
        else
        {
            result = original;
        }

        return result;
    }
}
