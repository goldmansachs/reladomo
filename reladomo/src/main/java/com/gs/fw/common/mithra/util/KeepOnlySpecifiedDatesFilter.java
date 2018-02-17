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

package com.gs.fw.common.mithra.util;

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import org.eclipse.collections.api.block.predicate.primitive.LongPredicate;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;


/**
 * true if object asofAttribute does NOT cover the list of specified business dates.
 */
public class KeepOnlySpecifiedDatesFilter extends AbstractBooleanFilter
{
    private final AsOfAttribute businessDate;
    private final long[] dates;

    public KeepOnlySpecifiedDatesFilter(AsOfAttribute businessDate, List<Timestamp> datesToKeep)
    {
        this.businessDate = businessDate;
        dates = new long[datesToKeep.size()];
        for (int i = 0; i < datesToKeep.size(); i++)
        {
            dates[i] = datesToKeep.get(i).getTime();
        }
        Arrays.sort(dates);
    }

    private KeepOnlySpecifiedDatesFilter(AsOfAttribute businessDate, long[] sortedDates)
    {
        this.businessDate = businessDate;
        this.dates = sortedDates;
    }

    public boolean matches(Object o)
    {
        long start = businessDate.getFromAttribute().timestampValueOfAsLong(o);
        long end = businessDate.getToAttribute().timestampValueOfAsLong(o);

        if (businessDate.isToIsInclusive())
        {
            for (int i = 0; i < dates.length && end >= dates[i]; i++)
            {
                if (start < dates[i])
                {
                    return false;
                }
            }
        }
        else
        {
            for (int i = 0; i < dates.length && end > dates[i]; i++)
            {
                if (start <= dates[i])
                {
                    return false;
                }
            }
        }
        return true;
    }

    public String toString()
    {
        StringBuilder buffer = new StringBuilder(this.getClass().getSimpleName());
        buffer.append(" ");
        for (int i=0; i<dates.length; i++)
        {
            buffer.append(new Timestamp(dates[i])).append (" ");
        }

        return buffer.toString();
    }

    @Override
    public BooleanFilter and(Filter that)
    {
        if (that instanceof KeepOnlySpecifiedDatesFilter &&
                this.businessDate.equals(((KeepOnlySpecifiedDatesFilter) that).businessDate))
        {
            MutableLongSet set = new LongHashSet();
            for (int i = 0; i < dates.length; i++) set.add(dates[i]);
            long[] thatDates = ((KeepOnlySpecifiedDatesFilter) that).dates;
            for (int i = 0; i < thatDates.length; i++) set.add(thatDates[i]);

            long[] sortedDates = set.toArray();
            Arrays.sort(sortedDates);

            return new KeepOnlySpecifiedDatesFilter(this.businessDate, sortedDates);
        }
        else
        {
            return super.and(that);
        }
    }

    @Override
    public BooleanFilter or(Filter that)
    {
        if (that instanceof KeepOnlySpecifiedDatesFilter &&
                this.businessDate.equals(((KeepOnlySpecifiedDatesFilter) that).businessDate))
        {
            LongHashSet thisSet = new LongHashSet(dates.length);
            thisSet.addAll(dates);

            long[] thatDates = ((KeepOnlySpecifiedDatesFilter) that).dates;
            LongHashSet thatSet = new LongHashSet(thatDates.length);
            thatSet.addAll(thatDates);

            LongHashSet smallerSet;
            final LongHashSet largerSet;
            if (thisSet.size() > thatSet.size())
            {
                smallerSet = thatSet;
                largerSet = thisSet;
            }
            else
            {
                smallerSet = thisSet;
                largerSet = thatSet;
            }
            long[] sortedDates = smallerSet.select(new LongPredicate() //TODO: Upgrade this to smallerSet.retainAll(largerSet).toSortedArray() with GS Collections upgrade 4.1+
            {
                @Override
                public boolean accept(long value)
                {
                    return largerSet.contains(value);
                }
            }).toSortedArray();

            return new KeepOnlySpecifiedDatesFilter(this.businessDate, sortedDates);
        }
        else
        {
            return super.or(that);
        }
    }
}
