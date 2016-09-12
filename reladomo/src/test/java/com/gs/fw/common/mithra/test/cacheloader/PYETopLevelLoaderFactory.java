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

package com.gs.fw.common.mithra.test.cacheloader;


import com.gs.fw.common.mithra.cacheloader.FullyMilestonedTopLevelLoaderFactory;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

import java.sql.Timestamp;

public class PYETopLevelLoaderFactory extends FullyMilestonedTopLevelLoaderFactory
{
    protected Timestamp shiftBusinessDate(Timestamp businessDate)
    {
        LocalDate localDate = new LocalDate(businessDate);
        int year = localDate.getYear();
        LocalDateTime pye = new LocalDateTime(year - 1, 12, 31, 23, 59, 0, 0);
        int dayOfWeek = pye.dayOfWeek().get();
        if (dayOfWeek > 5)
        {
            pye = pye.minusDays(dayOfWeek - 5);
        }
        return new Timestamp(pye.toDateTime().getMillis());
    }

}
