
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


public class DbCharFormatter extends VarCharFormatter
{

    public DbCharFormatter(Logger sqlLogger, String columnName, int length)
    {
        super(sqlLogger, columnName, length);
    }

    public String format(char c)
    {
        String result = null;
        if (this.length > 1)
        {
            // Pad out the character
            StringBuffer buffer = new StringBuffer();
            buffer.append(c);

            for (int i = 1; i < this.length; i++)
            {
                buffer.append(' ');
            }

            result = buffer.toString();
        }
        else
        {
            result = Character.toString(c);
        }

        return result;
    }

    public String format(Object obj)
    {
        String result = null;

        // There might be a Character here hence the call to toString rather than just a cast
        String original = obj.toString();
        int originalLength = original.length();

        if (original.length() < this.length)
        {
            StringBuffer buffer = new StringBuffer(original);

            // Pad out to the right by the missing characters
            int padBy = this.length - originalLength;
            for (int i = 0; i < padBy; i++)
            {
                buffer.append(' ');
            }

            result = buffer.toString();
        }
        else
        {
            result = super.format(obj);
        }

        return result;
    }
}
