
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

package com.gs.fw.common.mithra.test.domain;

import com.gs.fw.common.mithra.util.MithraTimestamp;

import java.util.Calendar;

public abstract class InfinityTimestamp
{

    private static final MithraTimestamp PARA_INFINITY;
    // TAMS_INFINITY is Jun 6, 2079 11:59:00 PM
    private static final MithraTimestamp TAMS_INFINITY;
    // DAWN_OF_TIME is Jan 1, 1900 12:00:00 AM
    private static final MithraTimestamp DAWN_OF_TIME;
    //TPARTY Table uses Jan 1, 9999
    private static final MithraTimestamp PARTY_INFINITY;

    private static final MithraTimestamp FUNDING_INFINITY;

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
        PARA_INFINITY = new MithraTimestamp(cal.getTime().getTime(), false);

        cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 2079);
        cal.set(Calendar.MONTH, 5);
        cal.set(Calendar.DAY_OF_MONTH, 6);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        TAMS_INFINITY = new MithraTimestamp(cal.getTime().getTime(), false);

        cal.set(Calendar.YEAR, 1900);
        cal.set(Calendar.MONTH, 0);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        DAWN_OF_TIME = new MithraTimestamp(cal.getTime().getTime(), false);

        cal.set(Calendar.YEAR, 9999);
        cal.set(Calendar.MONTH, 0);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        PARTY_INFINITY = new MithraTimestamp(cal.getTime().getTime(), false);

        cal.set(Calendar.YEAR, 9999);
        cal.set(Calendar.MONTH, 11);
        cal.set(Calendar.DAY_OF_MONTH, 31);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        FUNDING_INFINITY = new MithraTimestamp(cal.getTime().getTime(), false);
    }

    public static MithraTimestamp getParaInfinity()
    {
        return PARA_INFINITY;
    }

    public static MithraTimestamp getTamsInfinity()
    {
        return TAMS_INFINITY;
    }

    public static MithraTimestamp getPartyInfinity()
    {
        return PARTY_INFINITY;
    }

    public static MithraTimestamp getFundingInfinity()
    {
        return FUNDING_INFINITY;
    }
}
