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

package kata.util;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

public class TimestampProvider
{
    private TimestampProvider()
    {
        throw new UnsupportedOperationException("utility methods only -- not instantiable");
    }

    private static final Timestamp INFINITY_DATE = TimestampProvider.create(9999, 11, 1, Calendar.PM, 23, 59, 0, 0);

    private static Timestamp create(int year, int month, int dayOfMonth, int amPm, int hourOfDay, int minute, int second, int millisecond)
    {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month);
        cal.set(Calendar.DAY_OF_MONTH, dayOfMonth);
        cal.set(Calendar.AM_PM, amPm);
        cal.set(Calendar.HOUR_OF_DAY, hourOfDay);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);
        cal.set(Calendar.MILLISECOND, millisecond);
        return new Timestamp(cal.getTimeInMillis());
    }

    /**
     * Infinity reference date.
     *
     * @return A timestamp representing date "9999-12-01 23:59:00.0"
     */
    public static Timestamp getInfinityDate()
    {
        return INFINITY_DATE;
    }

    public static Timestamp createBusinessDate(Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        setBusinessDateTime(cal);
        return new Timestamp(cal.getTimeInMillis());
    }

    private static void setBusinessDateTime(Calendar cal)
    {
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 18);
        cal.set(Calendar.MINUTE, 30);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
    }

    /**
     * Converts the date passed to a Timestamp that is at 18:30 of the same day as the argument passed.
     * @return a timestamp at 18:30 at the same day as the argument
     */
    public static Timestamp ensure1830(Date date)
    {
        return createBusinessDate(date);
    }

    public static Timestamp getNextDay(Timestamp businessDay)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(businessDay);
        cal.add(Calendar.DATE, 1);
        setBusinessDateTime(cal);
        return new Timestamp(cal.getTimeInMillis());
    }
}
