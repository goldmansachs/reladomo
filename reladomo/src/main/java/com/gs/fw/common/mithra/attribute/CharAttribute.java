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

import com.gs.collections.api.set.primitive.CharSet;
import com.gs.collections.api.set.primitive.MutableCharSet;
import com.gs.collections.impl.set.mutable.primitive.CharHashSet;
import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraNullPrimitiveException;
import com.gs.fw.common.mithra.aggregate.attribute.CharAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorCharacter;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorCharacter;
import com.gs.fw.common.mithra.attribute.calculator.procedure.CharacterProcedure;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.CharNullUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.CharExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.CharOrderBy;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


public abstract class CharAttribute<T> extends Attribute<T, Character> implements com.gs.fw.finder.attribute.CharacterAttribute<T>, CharExtractor<T>
{
    private transient OrderBy ascendingOrderBy;
    private transient OrderBy descendingOrderBy;

    private static final long serialVersionUID = 5431632941496906065L;

    @Override
    public Class valueType()
    {
        return Character.class;
    }

    @Override
    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        out.writeChar(this.charValueOf(o));
    }

    @Override
    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException
    {
        this.setCharValue(o, in.readChar());
    }

    @Override
    public Operation nonPrimitiveEq(Object other)
    {
        if (other == null) return this.isNull();
        return this.eq(((Character) other).charValue());
    }

    public abstract Operation eq(char other);

    public abstract Operation notEq(char other);

    @Override
    public abstract Operation in(CharSet charSet);

    @Override
    public abstract Operation notIn(CharSet charSet);

    public abstract Operation greaterThan(char target);

    public abstract Operation greaterThanEquals(char target);

    public abstract Operation lessThan(char target);

    public abstract Operation lessThanEquals(char target);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    @Deprecated
    public abstract Operation eq(CharAttribute other);

    public abstract Operation joinEq(CharAttribute other);

    public abstract Operation filterEq(CharAttribute other);

    public abstract Operation notEq(CharAttribute other);

    public Character valueOf(T o)
    {
        if (this.isAttributeNull(o)) return null;
        return new Character(this.charValueOf(o));
    }

    public void setValue(T o, Character newValue)
    {
        this.setCharValue(o, newValue.charValue());
    }

    public int valueHashCode(T o)
    {
        if (this.isAttributeNull(o)) return HashUtil.NULL_HASH;
        return HashUtil.hash(this.charValueOf(o));
    }

    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.charValueOf(first) == this.charValueOf(second);
    }

    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Character> secondExtractor)
    {
        return this.charValueOf(first) == ((CharExtractor) secondExtractor).charValueOf(second);

    }

    @Override
    public OrderBy ascendingOrderBy()
    {
        if (this.ascendingOrderBy == null)
        {
            this.ascendingOrderBy = new CharOrderBy(this, true);
        }
        return this.ascendingOrderBy;
    }

    @Override
    public OrderBy descendingOrderBy()
    {
        if (this.descendingOrderBy == null)
        {
            this.descendingOrderBy = new CharOrderBy(this, false);
        }
        return this.descendingOrderBy;
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        final CharExtractor charExtractor = (CharExtractor) extractor;
        final MutableCharSet set = new CharHashSet();
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            final Object o = objects.get(i);
            if (!charExtractor.isAttributeNull(o))
            {
                set.add(charExtractor.charValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final CharExtractor charExtractor = (CharExtractor) extractor;
        final MutableCharSet set = new CharHashSet();
        for (Object o : objects)
        {
            if (!charExtractor.isAttributeNull(o))
            {
                set.add(charExtractor.charValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        CharExtractor charExtractor = (CharExtractor) extractor;
        MutableCharSet set = new CharHashSet();
        for (int i = 0; i < objects.size(); i++)
        {
            Object o = objects.get(i);
            if (!charExtractor.isAttributeNull(o))
            {
                set.add(charExtractor.charValueOf(o));
                if (set.size() > maxInClause)
                {
                    return new None(this);
                }
            }
        }
        return this.in(set);
    }

    @Override
    public void parseStringAndSet(String value, T data, int lineNumber, Format format) throws ParseException
    {
        if (value.length() != 1)
        {
            throw new ParseException("char value too long or too short '" + value + "' on line " +
                    lineNumber + " for attribute " + this.getAttributeName(), lineNumber);
        }
        this.setCharValue(data, value.charAt(0));
    }

    public void setValueUntil(T o, Character newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue.charValue(), exclusiveUntil);
    }

    protected void setUntil(T o, char c, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public String valueOfAsString(T object, Formatter formatter)
    {
        return formatter.format(this.charValueOf(object));
    }

    @Override
    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        if (this.isAttributeNull((T) dataObjects[0]))
        {
            return 1;
        }
        char firstValue = this.charValueOf((T) dataObjects[0]);
        MutableCharSet set = null;
        for (int i = 1; i < dataObjects.length; i++)
        {
            char nextValue = this.charValueOf((T) dataObjects[i]);
            if (set != null)
            {
                set.add(nextValue);
            }
            else if (nextValue != firstValue)
            {
                set = new CharHashSet();
                set.add(firstValue);
                set.add(nextValue);
            }
        }
        if (set != null)
        {
            return set.size();
        }
        return 1;
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        String charAttrCharValue = rs.getString(resultSetPosition);
        boolean charAttrCharValueisNull = charAttrCharValue == null || charAttrCharValue.length() < 1;
        if (!charAttrCharValueisNull)
        {
            if (charAttrCharValue.length() > 1)
            {
                throw new SQLException("attribute defined as char, but is more than one character long in database");
            }
            else if (charAttrCharValue.length() == 1)
            {
                tempArray[0] = charAttrCharValue.charAt(0);
            }
        }
        else
        {
           tempArray[0] = charAttrCharValue;
        }

        try
        {
            method.invoke(object, tempArray);
        }
        catch (IllegalArgumentException e)
        {
            if (tempArray[0] == null && method.getParameterTypes()[0].isPrimitive())
            {
                throw new MithraNullPrimitiveException("Aggregate result returned null for " + method.getName() + " of class " + object.getClass().getName() + " which cannot be set as primitive", e);
            }
            throw new MithraBusinessException("Invalid argument " + tempArray[0] + " passed in invoking method " + method.getName() + "  of class " + object.getClass().getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("No valid access to invoke method " + method.getName() + " of class " + object.getClass().getName(), e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("Error invoking method " + method.getName() + "of class " + object.getClass().getName(), e);
        }
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
            throws SQLException
    {
        MutableCharacter obj = new MutableCharacter();
        String charAttrCharValue = rs.getString(resultSetPosition);
        boolean charAttrCharValueisNull = charAttrCharValue == null || charAttrCharValue.length() < 1;
        if (!charAttrCharValueisNull)
        {
            if (charAttrCharValue.length() > 1)
            {
                throw new SQLException("attribute defined as char, but is more than one character long in database");
            }
            else if (charAttrCharValue.length() == 1)
            {
                obj.replace(charAttrCharValue.charAt(0));
            }
        }
        data.setValueAt(dataPosition, obj);
    }

    @Override
    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableCharacter((Character) value));
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        out.writeChar(((MutableCharacter)valueWrappedInNullable).getValue());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableCharacter(in.readChar());
    }

    @Override
    public CharAggregateAttribute min()
    {
        return new CharAggregateAttribute(new MinCalculatorCharacter(this));
    }

    @Override
    public CharAggregateAttribute max()
    {
        return new CharAggregateAttribute(new MaxCalculatorCharacter(this));
    }

    public abstract void forEach(final CharacterProcedure proc, T o, Object context);


    public boolean valueEquals(T first, T second)
    {
        if (first == second) return true;
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = this.isAttributeNull(second);
        if (firstNull != secondNull) return false;
        if (!firstNull) return this.primitiveValueEquals(first, second);
        return true;
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O, Character> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull) return false;
        if (!firstNull) return this.primitiveValueEquals(first, second, secondExtractor);
        return true;
    }

    @Override
    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForChar();
    }

    @Override
    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new CharNullUpdateWrapper(this, data);
    }

    @Override
    public Operation zGetOperationFromOriginal(Object original, Attribute left, Map tempOperationPool)
    {
        if (left.isAttributeNull(original))
        {
            return this.isNull();
        }
        return this.eq(((CharAttribute)left).charValueOf(original));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq((char)0);
    }

    @Override
    public Operation zGetOperationFromResult(T result, Map<Attribute, Object> tempOperationPool)
    {
        if (this.isAttributeNull(result))
        {
            return this.isNull();
        }
        return this.eq(this.charValueOf(result));
    }
}
