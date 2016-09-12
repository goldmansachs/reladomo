
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

package com.gs.fw.common.mithra.attribute;

import com.gs.fw.common.mithra.util.NullDataTimestamp;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.asofop.AsOfEqInfiniteNullOperation;
import com.gs.fw.common.mithra.extractor.Extractor;

import java.sql.Timestamp;
import java.util.Map;

public abstract class AsOfAttributeInfiniteNull<T> extends AsOfAttribute<T>
{

    protected AsOfAttributeInfiniteNull()
    {
    }

    protected AsOfAttributeInfiniteNull(String attributeName, String busClassNameWithDots, String busClassName, boolean isNullable,
                                        boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties, boolean transactional,
                                        boolean isOptimistic, TimestampAttribute fromAttribute, TimestampAttribute toAttribute, Timestamp infinityDate,
                                        boolean futureExpiringRowsExist, boolean toIsInclusive, Timestamp defaultDate, boolean isProcessingDate)
    {
        super(attributeName, busClassNameWithDots, busClassName, isNullable, hasBusDate, relatedFinder, properties,
                transactional, isOptimistic, fromAttribute, new TimestampAttributeAsOfAttributeToInfiniteNull(toAttribute),
                NullDataTimestamp.getInstance(), futureExpiringRowsExist, toIsInclusive, NullDataTimestamp.getInstance(), isProcessingDate);
    }

    public boolean isInfinityNull()
    {
        return true;
    }

    public boolean valueEquals(T first, T second)
    {
        if (first == second) return true;
        Timestamp firstValue = this.timestampValueOf(first);
        Timestamp secondValue = this.timestampValueOf(second);
        if (firstValue == secondValue) return true; // takes care of both null

        return (firstValue != null) && firstValue.equals(secondValue);
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O, Timestamp> secondExtractor)
    {
        Timestamp firstValue = this.timestampValueOf(first);
        Timestamp secondValue = secondExtractor.valueOf(second);
        if (firstValue == secondValue) return true; // takes care of both null

        return (firstValue != null) && firstValue.equals(secondValue);
    }

    public Operation eq(Timestamp asOf)
    {
        if (asOf == null)
        {
            return new None(this);
        }
        return new AsOfEqInfiniteNullOperation(this, asOf);
    }

    public int appendWhereClauseForValue(Timestamp value, StringBuffer whereClause)
    {
        if (value == null ||
                (this.isFutureExpiringRowsExist() && value.getTime() >= System.currentTimeMillis()))
        {
            whereClause.append("("+this.getToAttribute().getColumnName());
            whereClause.append(" = ? or "+this.getToAttribute().getColumnName()+" is null)");
            return 1;
        }
        else
        {
            if (this.isToIsInclusive())
            {
                whereClause.append(this.getFromAttribute().getColumnName());
                whereClause.append(" < ? and ");
                whereClause.append("("+this.getToAttribute().getColumnName());
                whereClause.append(" >= ? or "+this.getToAttribute().getColumnName()+" is null)");
            }
            else
            {
                whereClause.append(this.getFromAttribute().getColumnName());
                whereClause.append(" <= ? and ");
                whereClause.append("("+this.getToAttribute().getColumnName());
                whereClause.append(" > ? or "+this.getToAttribute().getColumnName()+" is null)");
            }
            return 2;
        }
    }

    public void appendInfinityWhereClause(StringBuffer whereClause)
    {
        whereClause.append(this.getToAttribute().getColumnName());
        whereClause.append(" is null");
    }

    public int appendWhereClauseForRange(Timestamp start, Timestamp end, StringBuffer whereClause)
    {
        int numParams = 1;
        if (!end.equals(NullDataTimestamp.getInstance()))
        {
            whereClause.append(this.getFromAttribute().getColumnName());
            // note: this would be <= if we wanted to stich
            whereClause.append(" < ? and ");
            numParams = 2;
        }
        whereClause.append("("+this.getToAttribute().getColumnName());
        // note: this would be >= if we wanted to stich
        whereClause.append(" > ? or "+this.getToAttribute().getColumnName()+" is null)");
        return numParams;
    }

    public Timestamp getDefaultDate()
    {
        return NullDataTimestamp.getInstance();
    }

    public int valueHashCode(T o)
    {
        Timestamp value = this.timestampValueOf(o);
        return value == null ? NullDataTimestamp.getInstance().hashCode() : value.hashCode();
    }

    public static AsOfAttribute generate(String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic,TimestampAttribute fromAttribute, TimestampAttribute toAttribute, Timestamp infinityDate,
            boolean futureExpiringRowsExist, boolean toIsInclusive, Timestamp defaultDate, boolean isProcessingDate, boolean isInfinityNull)
    {
        return AsOfAttribute.generate(attributeName,busClassNameWithDots, busClassName, isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, fromAttribute, new TimestampAttributeAsOfAttributeToInfiniteNull(toAttribute), infinityDate, futureExpiringRowsExist, toIsInclusive, defaultDate, isProcessingDate, isInfinityNull);
    }
}
