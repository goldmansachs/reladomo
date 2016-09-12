

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

package com.gs.fw.common.mithra.util;

import java.util.Calendar;

public class DefaultInfinityTimestamp
{

    private static final MithraTimestamp DEFAULT_INFINITY;

    private static final MithraTimestamp SYBASE_IQ_INFINITY;

    private static final MithraTimestamp CSW_INFINITY;

    private static final MithraTimestamp DEFAULT_SMALL_INFINITY;

    static
    {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 9999);
        cal.set(Calendar.MONTH, 11);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        DEFAULT_INFINITY = new MithraTimestamp(cal.getTime().getTime(), false);

        cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 9999);
        cal.set(Calendar.MONTH, 11);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.AM_PM, Calendar.AM);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        SYBASE_IQ_INFINITY = new MithraTimestamp(cal.getTime().getTime(), false);

        cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 9999);
        cal.set(Calendar.MONTH, 11);
        cal.set(Calendar.DAY_OF_MONTH, 31);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 0);
        CSW_INFINITY = new MithraTimestamp(cal.getTime().getTime(), false);

        cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 2079);
        cal.set(Calendar.MONTH, 5);
        cal.set(Calendar.DAY_OF_MONTH, 6);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        DEFAULT_SMALL_INFINITY = new MithraTimestamp(cal.getTime().getTime(), false);
    }

    public static MithraTimestamp getDefaultInfinity()
    {
        return DEFAULT_INFINITY;
    }

    public static MithraTimestamp getCswInfinity()
    {
        return CSW_INFINITY;
    }

    public static MithraTimestamp getDefaultSmalldateInfinity()
    {
        return DEFAULT_SMALL_INFINITY;
    }

    public static MithraTimestamp getSybaseIqInfinity()
    {
        return SYBASE_IQ_INFINITY;
    }
}
