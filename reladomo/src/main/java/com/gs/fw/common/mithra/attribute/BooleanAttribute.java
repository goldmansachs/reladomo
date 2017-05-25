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

import com.gs.collections.api.set.primitive.BooleanSet;
import com.gs.collections.api.set.primitive.MutableBooleanSet;
import com.gs.collections.impl.set.mutable.primitive.BooleanHashSet;
import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraNullPrimitiveException;
import com.gs.fw.common.mithra.aggregate.attribute.BooleanAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorBoolean;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorBoolean;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BooleanProcedure;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.BooleanNullUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.BooleanExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.BooleanOrderBy;
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


public abstract class BooleanAttribute<T> extends Attribute<T, Boolean> implements com.gs.fw.finder.attribute.BooleanAttribute<T>, BooleanExtractor<T>
{
    private transient OrderBy ascendingOrderBy;
    private transient OrderBy descendingOrderBy;

    private static final byte NULL_BOOLEAN_VALUE = 10;
    private static final byte TRUE_BOOLEAN_VALUE = 20;
    private static final byte FALSE_BOOLEAN_VALUE = 30;

    private static final long serialVersionUID = -4666537384464968363L;

    public Class valueType()
    {
        return Boolean.class;
    }

    public void serializeValue(T o, ObjectOutput out) throws IOException
    {
        if (this.isAttributeNull(o))
        {
            out.writeByte(NULL_BOOLEAN_VALUE);
        }
        else
        {
            if (this.booleanValueOf(o))
            {
                out.writeByte(TRUE_BOOLEAN_VALUE);
            }
            else
            {
                out.writeByte(FALSE_BOOLEAN_VALUE);
            }
        }
    }

    public void deserializeValue(T o, ObjectInput in) throws IOException
    {
        byte result = in.readByte();
        switch(result)
        {
            case NULL_BOOLEAN_VALUE:
                this.setValueNull(o);
                break;
            case TRUE_BOOLEAN_VALUE:
                this.setBooleanValue(o, true);
                break;
            case FALSE_BOOLEAN_VALUE:
                this.setBooleanValue(o, false);
                break;
            default:
                throw new IOException("unexpected byte in stream "+result);
        }
    }

    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        throw new RuntimeException("should never get here");
    }

    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException, ClassNotFoundException
    {
        throw new RuntimeException("should never get here");
    }

    public Operation nonPrimitiveEq(Object other)
    {
        if (other == null) return this.isNull();
        return this.eq(((Boolean)other).booleanValue());
    }

    public abstract Operation eq(boolean other);

    public abstract Operation notEq(boolean other);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public abstract Operation in(BooleanSet booleanSet);

    @Override
    public abstract Operation in(org.eclipse.collections.api.set.primitive.BooleanSet booleanSet);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public abstract Operation notIn(BooleanSet booleanSet);

    @Override
    public abstract Operation notIn(org.eclipse.collections.api.set.primitive.BooleanSet booleanSet);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    public abstract Operation eq(BooleanAttribute other);

    public abstract Operation joinEq(BooleanAttribute other);

    public abstract Operation filterEq(BooleanAttribute other);

    public abstract Operation notEq(BooleanAttribute other);

    public Boolean valueOf(T o)
    {
        if (this.isAttributeNull(o))
        {
            return null;
        }
        return Boolean.valueOf(this.booleanValueOf(o));
    }

    public void setValue(T o, Boolean newValue)
    {
        this.setBooleanValue(o, newValue.booleanValue());
    }

    public int valueHashCode(T o)
    {
        if (this.isAttributeNull(o))
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(this.booleanValueOf(o));
    }

    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.booleanValueOf(first) == this.booleanValueOf(second);
    }

    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Boolean> secondExtractor)
    {
        return this.booleanValueOf(first) == ((BooleanExtractor)secondExtractor).booleanValueOf(second);

    }

    public OrderBy ascendingOrderBy()
    {
        if (this.ascendingOrderBy == null)
        {
            this.ascendingOrderBy = new BooleanOrderBy(this, true);
        }
        return this.ascendingOrderBy;
    }

    public OrderBy descendingOrderBy()
    {
        if (this.descendingOrderBy == null)
        {
            this.descendingOrderBy = new BooleanOrderBy(this, false);
        }
        return this.descendingOrderBy;
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        final BooleanExtractor booleanExtractor = (BooleanExtractor) extractor;
        final MutableBooleanSet set = new BooleanHashSet();
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            Object o = objects.get(i);
            if (!booleanExtractor.isAttributeNull(o))
            {
                set.add(booleanExtractor.booleanValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final BooleanExtractor booleanExtractor = (BooleanExtractor) extractor;
        final MutableBooleanSet set = new BooleanHashSet();
        for (Object o : objects)
        {
            if (!booleanExtractor.isAttributeNull(o))
            {
                set.add(booleanExtractor.booleanValueOf(o));
            }
        }
        return this.in(set);
    }

    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        return this.in(objects, extractor);
    }

    public void parseStringAndSet(String value, T data, int lineNumber, Format format) throws ParseException
    {
        if (value.equals("0"))
        {
            this.setBooleanValue(data, false);
        }
        else if (value.equals("1"))
        {
            this.setBooleanValue(data, true);
        }
        else
        {
            this.parseWordAndSet(value, data,lineNumber);
        }
    }

    public void parseWordAndSet(String word, T data, int lineNumber) throws ParseException
    {
        if (word.equals("null"))
        {
            this.setValueNull(data);
        }
        else if (word.equals("true"))
        {
            this.setBooleanValue(data, true);
        }
        else if (word.equals("false"))
        {
            this.setBooleanValue(data, false);
        }
        else
        {
            throw new ParseException(
                "Did not expect word '" + word + "' on line " + lineNumber + " for attribute '"
                + this.getClass().getName() + "'. Valid values are true and false.", lineNumber
            );
        }
    }

    public void setValueUntil(T o, Boolean newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue.booleanValue(), exclusiveUntil);
    }

    protected void setUntil(T o, boolean b, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public String valueOfAsString(T object, Formatter formatter)
    {
        return formatter.format(this.booleanValueOf(object));
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        if (isAttributeNull((T) dataObjects[0]))
        {
            return 1;
        }
        boolean firstValue = this.booleanValueOf((T) dataObjects[0]);
        for(int i=1;i<dataObjects.length;i++)
        {
            boolean nextValue = this.booleanValueOf((T) dataObjects[i]);
            if (nextValue != firstValue)
            {
                return 2;
            }
        }
        return 1;
    }

     @Override
     public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
     {
         boolean b = rs.getBoolean(resultSetPosition);
         if (rs.wasNull())
         {
             tempArray[0] = null;
         }
         else
         {
             tempArray[0] = b;
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

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
            throws SQLException
    {
        boolean b = rs.getBoolean(resultSetPosition);
        MutableBoolean obj;
        if(rs.wasNull())
        {
            obj = new MutableBoolean();
        }
        else
        {
            obj = new MutableBoolean(b);
        }
        data.setValueAt(dataPosition, obj);
    }

    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableBoolean((Boolean)value));
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        MutableBoolean b = (MutableBoolean) valueWrappedInNullable;
        if (b.booleanValue())
        {
            out.writeByte(TRUE_BOOLEAN_VALUE);
        }
        else
        {
            out.writeByte(FALSE_BOOLEAN_VALUE);
        }
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        byte result = in.readByte();
        MutableBoolean booleanResult;
        switch(result)
        {
            case TRUE_BOOLEAN_VALUE:
                booleanResult = new MutableBoolean(true);
                break;
            case FALSE_BOOLEAN_VALUE:
                booleanResult = new MutableBoolean(false);
                break;
            default:
                throw new IOException("unexpected byte in stream "+result);
        }
        return booleanResult;
    }

    public BooleanAggregateAttribute min()
   {
       return new BooleanAggregateAttribute(new MinCalculatorBoolean(this));
   }

   public BooleanAggregateAttribute max()
   {
      return new BooleanAggregateAttribute(new MaxCalculatorBoolean(this));
   }

   public abstract void forEach(final BooleanProcedure proc, T o, Object context);

    public boolean valueEquals(T first, T second)
    {
        if (first == second) return true;
        if (this.isNullable())
        {
            boolean firstNull = this.isAttributeNull(first);
            return firstNull == this.isAttributeNull(second) && (firstNull || this.primitiveValueEquals(first, second));
        }
        else
        {
            return this.primitiveValueEquals(first, second);
        }
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O, Boolean> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull) return false;
        if (!firstNull) return this.primitiveValueEquals(first, second, secondExtractor);
        return true;
    }

    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return databaseType.getIndexableSqlDataTypeForBoolean();
    }

    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new BooleanNullUpdateWrapper(this, data);
    }

    public Operation zGetOperationFromOriginal(Object original, Attribute left, Map tempOperationPool)
    {
        if (left.isAttributeNull(original))
        {
            return this.isNull();
        }
        return this.eq(((BooleanAttribute)left).booleanValueOf(original));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq(false);
    }

    public Operation zGetOperationFromResult(T result, Map<Attribute, Object> tempOperationPool)
    {
        if (this.isAttributeNull(result))
        {
            return this.isNull();
        }
        return this.eq(this.booleanValueOf(result));
    }
}
