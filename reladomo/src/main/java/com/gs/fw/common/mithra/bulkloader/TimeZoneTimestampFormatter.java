
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

import java.sql.Timestamp;
import java.util.TimeZone;

import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.util.MithraTimestamp;


public class TimeZoneTimestampFormatter extends TimestampFormatter
{
    private final TimeZone timeZone;
    private final TimestampAttribute timestampAttribute;

    public TimeZoneTimestampFormatter(String timestampFormat, TimeZone timeZone, TimestampAttribute timestampAttribute)
    {
        super(timestampFormat);
        this.timeZone = timeZone;
        this.timestampAttribute = timestampAttribute;
    }

    public String format(Object obj)
    {
        return super.format(timestampAttribute.zConvertTimezoneIfNecessary((Timestamp) obj, this.timeZone));
    }
}
