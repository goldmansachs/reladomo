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

import com.gs.fw.common.mithra.attribute.TimestampAttribute;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;


public class SybaseIqTimestampWithZoneFormatter extends SybaseIqOutputFormatter
{

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private TimestampAttribute timestampAttribute;
    private TimeZone timeZone;

    public SybaseIqTimestampWithZoneFormatter(boolean nullable, String columnName, TimeZone timeZone, TimestampAttribute timestampAttribute)
    {
        super(nullable, columnName, "datetime");
        this.timestampAttribute = timestampAttribute;
        this.timeZone = timeZone;
    }

    public void writeNull(OutputStream os) throws IOException
    {
        checkNullability();
        for(int i=0;i<"yyyy-MM-dd HH:mm:ss.SSS".length();i++)
        {
            os.write(0);
        }
    }

    @Override
    public void write(Object obj, OutputStream os) throws IOException
    {
        if (obj instanceof Timestamp)
        {
            os.write(dateFormat.format(timestampAttribute.zConvertTimezoneIfNecessary((Timestamp) obj, this.timeZone)).getBytes(ISO_8859_1));
        }
        else
        {
            throwConversionError(obj.getClass().getName());
        }
    }

    public String getColumnSpec(boolean previousNullByte)
    {
        String result = this.getColumnName() + " DATE ('YYYY-MM-DD HH:NN:SS.SSS')";
        if (this.isNullable())
        {
            result += " NULL(ZEROS)";
        }
        return result;
    }
}
