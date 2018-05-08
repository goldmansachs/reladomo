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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.attribute.BigDecimalAttribute;
import com.gs.fw.common.mithra.behavior.GenericBiTemporalDirector;

import java.sql.Timestamp;
import java.util.Calendar;



public class ParaLikeBiTemporalDirector extends GenericBiTemporalDirector
{
    private static Timestamp cbdDate = null;

    public ParaLikeBiTemporalDirector(AsOfAttribute businessDateAttribute, AsOfAttribute processingDateAttribute,
            DoubleAttribute[] doubleAttributes, BigDecimalAttribute[] bigDecimalAttributes)
    {
        super(businessDateAttribute, processingDateAttribute, doubleAttributes, bigDecimalAttributes);
    }

    protected Timestamp getDateWith1830(Timestamp date)
    {
        Calendar cal = this.getCalendar();
        cal.setTime(date);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 18);
        cal.set(Calendar.MINUTE, 30);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Timestamp result = new Timestamp(cal.getTimeInMillis());
        return result;
    }

    public Timestamp getBusinessFromDateForBusinessDate(Timestamp businessDate)
    {
        return addDays(this.getDateWith1830(businessDate), -1);
    }

    public Timestamp getBusinessToDateForBusinessDate(Timestamp asOfDate)
    {
        throw new RuntimeException("not implemented");
    }

    protected boolean inactivateOnSameDayUpdate(Timestamp asOfDate)
    {
        return !asOfDate.equals(this.getCurrentBusinessDate());
    }

    protected Timestamp getCurrentBusinessDate()
    {
        if (cbdDate != null)
        {
            return cbdDate;
        }
        else
        {
            return this.getDateWith1830(new Timestamp(System.currentTimeMillis()));
        }
    }

    public static void setCbdDate(Timestamp cbdDate)
    {
        ParaLikeBiTemporalDirector.cbdDate = cbdDate;
    }

    public static Timestamp getStoredCbdDate()
    {
        return cbdDate;
    }
}
