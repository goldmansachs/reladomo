
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

import com.gs.collections.api.block.procedure.primitive.IntObjectProcedure;
import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.aggregate.attribute.StringAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.StringToIntegerNumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.StringToLowerCaseCalculator;
import com.gs.fw.common.mithra.attribute.calculator.SubstringCalculator;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorString;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorString;
import com.gs.fw.common.mithra.attribute.calculator.procedure.StringProcedure;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.StringUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.StringExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.string.*;
import com.gs.fw.common.mithra.util.*;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public abstract class StringAttribute<Owner> extends NonPrimitiveAttribute<Owner, String> implements com.gs.fw.finder.attribute.StringAttribute<Owner>, StringExtractor<Owner>
{
    private int maxLength = Integer.MAX_VALUE;
    private boolean mustTrim = true;

    private static final long serialVersionUID = -9020218080533972291L;

    public StringAttribute()
    {
    }

    public StringAttribute(int maxLength, boolean mustTrim)
    {
        this.maxLength = maxLength;
        this.mustTrim = mustTrim;
    }

    public int getMaxLength()
    {
        return this.maxLength;
    }

    protected boolean mustTrim()
    {
        return this.mustTrim;
    }

    protected void setMaxLength(int maxLength)
    {
        this.maxLength = maxLength;
    }

    protected void setMustTrim(boolean mustTrim)
    {
        this.mustTrim = mustTrim;
    }

    public Operation nonPrimitiveEq(Object other)
    {
        return this.eq(((String) other));
    }

    public Operation eq(String other)
    {
        return (other == null) ? this.isNull() : new NonPrimitiveEqOperation(this, other);
    }

    public Operation notEq(String other)
    {
        return (other == null) ? this.isNotNull() : new NonPrimitiveNotEqOperation(this, other);
    }

    public Operation eqWithTrim(String other)
    {
        return (other == null) ? this.isNull() : this.eq(other.trim());
    }

    public Operation greaterThan(String target)
    {
        return new NonPrimitiveGreaterThanOperation(this, target);
    }

    public Operation greaterThanEquals(String target)
    {
        return new NonPrimitiveGreaterThanEqualsOperation(this, target);
    }

    public Operation lessThan(String target)
    {
        return new NonPrimitiveLessThanOperation(this, target);
    }

    public Operation lessThanEquals(String target)
    {
        return new NonPrimitiveLessThanEqualsOperation(this, target);
    }

    public Operation startsWith(String start)
    {
        return new StringStartsWithOperation(this, start);
    }

    /**
     * Wild cards can include * and ?, which mean 'zero or many' and 'exactly one' character respectively.
     * A wild card can be escaped using single-quote. A single-quote must be escaped with another single quote
     * Examples:
     * "a?b*" matches "acbdefg" and "agb", but not "ab"
     * "a'*" matches "a*" (a followed with the star character)
     * "a''*" matches "a'" and "a'bced" (the two quotes are an escaped single-quote)
     * "a'''*" matches "a'*" (a followed by single-quote and then the star character)
     *
     * A single quote that is not followed by ',?, or * is ignored, so the pattern "a'b" is the same as "ab", which matches just "ab"
     *
     * @param pattern the pattern to match
     * @return a wild card operation
     */
    public Operation wildCardEq(String pattern)
    {
        if (pattern == null)
        {
            return this.isNull();
        }
        AnalyzedWildcardPattern wildcardPattern = WildcardParser.getAnalyzedPattern(pattern, null);
        if (wildcardPattern.getPlain() != null)
        {
            return this.eq(pattern);
        }
        else if (wildcardPattern.getContains() != null)
        {
            return this.contains(wildcardPattern.getContains().iterator().next());
        }
        else if (wildcardPattern.getEndsWith() != null)
        {
            return this.endsWith(wildcardPattern.getEndsWith().iterator().next());
        }
        else if (wildcardPattern.getSubstring() != null)
        {
            Iterator<Set<String>> intObjectIterator = wildcardPattern.getSubstring().iterator();
            return this.startsWith(intObjectIterator.next().iterator().next());
        }
        return new StringWildCardEqOperation(this, wildcardPattern.getWildcard().iterator().next());
    }

    public Operation wildCardIn(Set<String> patterns)
    {
        switch (patterns.size())
        {
            case 0:
                return new None(this);
            case 1:
                return this.wildCardEq(patterns.iterator().next());
        }
        AnalyzedWildcardPattern analyzedPatterns = null;
        for(String pattern: patterns)
        {
            analyzedPatterns = WildcardParser.getAnalyzedPattern(pattern, analyzedPatterns);
        }
        Operation op = NoOperation.instance();
        if (analyzedPatterns.getPlain() != null)
        {
            op = op.or(this.in(analyzedPatterns.getPlain()));
        }
        if (analyzedPatterns.getContains() != null)
        {
            for(String s: analyzedPatterns.getContains())
            {
                op = op.or(this.contains(s));
            }
        }
        if (analyzedPatterns.getEndsWith() != null)
        {
            for(String s: analyzedPatterns.getEndsWith())
            {
                op = op.or(this.endsWith(s));
            }
        }
        IntObjectHashMap<Set<String>> substringSets = analyzedPatterns.getSubstring();
        if (substringSets != null)
        {
            OperationConstructionProcedure procedure = new OperationConstructionProcedure(op);
            substringSets.forEachKeyValue(procedure);
            op = procedure.localOp;
        }
        if (analyzedPatterns.getWildcard() != null)
        {
            for(String s: analyzedPatterns.getWildcard())
            {
                op = op.or(new StringWildCardEqOperation(this, s));
            }
        }
        return op;
    }

    public Operation wildCardNotEq(String pattern)
    {
        if (pattern == null)
        {
            return this.isNotNull();
        }
        AnalyzedWildcardPattern wildcardPattern = WildcardParser.getAnalyzedPattern(pattern, null);
        if (wildcardPattern.getPlain() != null)
        {
            return this.notEq(pattern);
        }
        else if (wildcardPattern.getContains() != null)
        {
            return this.notContains(wildcardPattern.getContains().iterator().next());
        }
        else if (wildcardPattern.getEndsWith() != null)
        {
            return this.notEndsWith(wildcardPattern.getEndsWith().iterator().next());
        }
        else if (wildcardPattern.getSubstring() != null)
        {
            Iterator<Set<String>> intObjectIterator = wildcardPattern.getSubstring().iterator();
            return this.notStartsWith(intObjectIterator.next().iterator().next());
        }
        return new StringWildCardNotEqOperation(this, pattern);
    }

    public Operation endsWith(String start)
    {
        return new StringEndsWithOperation(this, start);
    }

    public Operation contains(String searchString)
    {
        return new StringContainsOperation(this, searchString);
    }

    public Operation notStartsWith(String start)
    {
        return new StringNotStartsWithOperation(this, start);
    }

    public Operation notEndsWith(String start)
    {
        return new StringNotEndsWithOperation(this, start);
    }

    public Operation notContains(String start)
    {
        return new StringNotContainsOperation(this, start);
    }

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    public abstract Operation eq(StringAttribute other);

    public abstract Operation joinEq(StringAttribute other);

    public abstract Operation filterEq(StringAttribute other);

    public abstract Operation notEq(StringAttribute other);

    public StringAttribute<Owner> toLowerCase()
    {
        return new CalculatedStringAttribute<Owner>(new StringToLowerCaseCalculator(this));
    }

    /**
     * A lenient substring. If the end is passed the actual end of the string, this returns substring(start, size).
     * Some databases may not be lenient; ensure start and end are sensible values.
     * Setting end to -1 means to the end of the string
     * @param start the start index, inclusive. This is zero based index (the same as Java's String.substring)
     * @param end the end index, exclusive
     * @return A queriable string attribute
     */
    public StringAttribute<Owner> substring(int start, int end)
    {
        return new CalculatedStringAttribute<Owner>(new SubstringCalculator(this, start, end));
    }

    public void setSqlParameter(int index, PreparedStatement ps, Object o, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        if (o != null)
        {
            ps.setString(index, (String) o);
        }
        else
        {
            ps.setNull(index, java.sql.Types.VARCHAR);
        }
    }

    public String valueOf(Owner o)
    {
        return this.stringValueOf(o);
    }

    public void setValue(Owner o, String newValue)
    {
        this.setStringValue(o, newValue);
    }

    public Class valueType()
    {
        return String.class;
    }

    public void parseStringAndSet(String value, Owner data, int lineNumber, Format format) throws ParseException
    {
        this.setStringValue(data, value);
    }

    public void setValueUntil(Owner o, String newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    protected void setUntil(Object o, String s, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int offHeapValueOf(Owner o)
    {
        return StringPool.getInstance().getOffHeapAddressWithoutAdding(stringValueOf(o));
    }

    public String valueOfAsString(Owner object, Formatter formatter)
    {
        return formatter.format(this.stringValueOf(object));
    }

    public int valueHashCode(Owner o)
    {
        String val = this.stringValueOf(o);
        if (val == null) return HashUtil.NULL_HASH;
        return val.hashCode();
    }

    public boolean valueEquals(Owner first, Owner second)
    {
        if (first == second) return true;
        String firstValue = this.stringValueOf(first);
        String secondValue = this.stringValueOf(second);
        return firstValue == secondValue || (firstValue != null) && firstValue.equals(secondValue);
    }

    public <O> boolean valueEquals(Owner first, O second, Extractor<O, String> secondExtractor)
    {
        String firstValue = this.stringValueOf(first);
        Object secondValue = secondExtractor.valueOf(second);
        if (firstValue == secondValue) return true; // takes care of both null

        return (firstValue != null) && firstValue.equals(secondValue);
    }

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
            throws SQLException
    {
        //todo: trim and pool only if attribute is configured to be trimmed or pooled
        String s = rs.getString(resultSetPosition);
        if (s != null)
        {
            s = StringPool.getInstance().getOrAddToCache(s.trim(), this.getOwnerPortal().isFullyCached());
        }
        data.setValueAt(dataPosition, new MutableComparableReference<String>(s));
    }

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        String s = rs.getString(resultSetPosition);
        if (s != null) s = s.trim();
        tempArray[0] = s;
        try
        {
            method.invoke(object, tempArray);
        }
        catch (IllegalArgumentException e)
        {
            throw new MithraBusinessException("Invalid argument " + tempArray[0] + " passed in invoking method " + method.getName() + " of class " + object.getClass().getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("No valid access to invoke method " + method.getName() + " of class " + object.getClass().getName(), e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("Error invoking method " + method.getName() + " of class " + object.getClass().getName(), e);
        }
    }

    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableComparableReference<String>((String) value));
    }

    public StringAggregateAttribute min()
    {
        return new StringAggregateAttribute(new MinCalculatorString(this));
    }

    public StringAggregateAttribute max()
    {
        return new StringAggregateAttribute(new MaxCalculatorString(this));
    }

    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForString() + (this.maxLength > 0 ? "("+maxLength+")" : "(255)");
    }

    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new StringUpdateWrapper(this, data, null);
    }

    public IntegerAttribute<Owner> convertToIntegerAttribute()
    {
        return new CalculatedIntegerAttribute(new StringToIntegerNumericAttributeCalculator(this));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq("");
    }

    public abstract void forEach(final StringProcedure proc, Owner o, Object context);

    private class OperationConstructionProcedure implements IntObjectProcedure<Set<String>>
    {
        private final Operation op;
        Operation localOp;

        public OperationConstructionProcedure(Operation op)
        {
            this.op = op;
            localOp = op;
        }

        @Override
        public void value(int len, Set<String> set)
        {
            localOp = localOp.or(substring(0, len).in(set));
        }
    }

    @Override
    protected void zWriteNonNullSerial(ReladomoSerializationContext context, SerialWriter writer, Owner reladomoObject) throws IOException
    {
        writer.writeString(context, this.getAttributeName(), this.stringValueOf(reladomoObject));
    }
}
