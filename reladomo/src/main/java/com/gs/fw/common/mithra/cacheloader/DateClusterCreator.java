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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.cacheloader;


import org.eclipse.collections.impl.list.mutable.FastList;
import org.joda.time.DateTime;
import org.joda.time.Days;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

public class DateClusterCreator
{
    private static final int MAX_DISTANCE = 5;

    public static List<DateCluster> createMultiDateClusters(List<Timestamp> businessDates)
    {
        Collections.sort(businessDates);
        return new DateClusterCreatorHelper(MAX_DISTANCE).create(businessDates);
    }

    public static List<DateCluster> createSingleDateClusters(List<Timestamp> businessDates)
    {
        Collections.sort(businessDates);
        List<DateCluster> dateClusters = FastList.newList();
        for (Timestamp businessDate : businessDates)
        {
            dateClusters.add(new DateCluster(businessDate));
        }
        return dateClusters;
    }

    private static class DateClusterCreatorHelper
    {
        private final List<DateCluster> dateClusters = FastList.newList();
        private final int maxDistance;

        private DateClusterCreatorHelper(int maxDistance)
        {
            this.maxDistance = maxDistance;
        }

        private List<DateCluster> create(List<Timestamp> businessDates)
        {
            if (businessDates != null && !businessDates.isEmpty())
            {
                List<DateTime> dateTimes = this.toDateTimes(businessDates);
                this.createFromDateTimes(dateTimes);
            }
            return this.dateClusters;
        }

        private List<DateTime> toDateTimes(List<Timestamp> businessDates)
        {
            List<DateTime> dateTimes = FastList.newList(businessDates.size());
            for (Timestamp businessDate : businessDates)
            {
                dateTimes.add(new DateTime(businessDate));
            }
            return dateTimes;
        }

        private void createFromDateTimes(List<DateTime> sortedDateTimes)
        {
            List<DateTime> datesForCurrentCluster = FastList.newListWith(sortedDateTimes.remove(0));
            for (DateTime nextDateTime : sortedDateTimes)
            {
                if (this.isPartOfCurrentCluster(datesForCurrentCluster, nextDateTime))
                {
                    datesForCurrentCluster.add(nextDateTime);
                }
                else
                {
                    this.createCluster(datesForCurrentCluster);
                    datesForCurrentCluster = FastList.newListWith(nextDateTime);
                }
            }
            this.createCluster(datesForCurrentCluster);
        }

        private void createCluster(List<DateTime> datesForCluster)
        {
            List<Timestamp> businessDates = FastList.newList(datesForCluster.size());
            for (DateTime dateTime : datesForCluster)
            {
                businessDates.add(toTimestamp(dateTime));
            }
            this.dateClusters.add(new DateCluster(businessDates));
        }

        private boolean isPartOfCurrentCluster(List<DateTime> datesForCurrentCluster, DateTime nextDateTime)
        {
            return Days.daysBetween(datesForCurrentCluster.get(datesForCurrentCluster.size() - 1), nextDateTime).getDays() <= this.maxDistance;
        }

        private static Timestamp toTimestamp(DateTime dateTime)
        {
            return new Timestamp(dateTime.getMillis());
        }
    }
}
