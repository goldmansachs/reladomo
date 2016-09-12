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

package com.gs.fw.common.mithra.aggregate.attribute;

import com.gs.fw.common.mithra.AggregateAttribute;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraNullPrimitiveException;
import com.gs.fw.common.mithra.databasetype.DatabaseType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;



public class BeanAggregateAttribute extends AggregateAttribute
{
    private Method setterMethod;
    private AggregateAttribute aggregateAttribute;
    private boolean isMethodParameterShort;
    private boolean isMethodParameterByte;

    public BeanAggregateAttribute()
    {
        super();
    }

    public BeanAggregateAttribute(AggregateAttribute aggregateAttribute, Method method)
    {
        super(aggregateAttribute.getCalculator());
        this.setterMethod = method;
        this.aggregateAttribute = aggregateAttribute;
        checkMethodParamForShortAndByte();
    }

    private void checkMethodParamForShortAndByte()
    {
        Class className = setterMethod.getParameterTypes()[0];
        if (className.equals(Short.TYPE) || (className.equals(Short.class)))
            this.isMethodParameterShort = true;
        if (className.equals(Byte.TYPE) || (className.equals(Byte.class)))
            this.isMethodParameterByte = true;

    }

    public AggregateAttribute getAggregateAttribute()
    {
        return aggregateAttribute;
    }

    @Override
    public int populateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object data, TimeZone databaseTimezone, DatabaseType dt, Object[] scratchArray) throws SQLException
    {
        return this.getCalculator().populateValueFromResultSet(resultSetPosition, dataPosition, rs, data, setterMethod, databaseTimezone, dt, scratchArray);
    }

    public void setSqlParameter(PreparedStatement pstmt, int startIndex, Object value) throws SQLException
    {
        throw new RuntimeException("Not Implemented");
    }

    public void setValue(Object instance, Object[] valueArray)
    {
        valueArray[0] = getValueToSet(instance, valueArray[0]);
        try
        {
            setterMethod.invoke(instance, valueArray);
        }
        catch (IllegalArgumentException e)
        {
            if (setterMethod.getParameterTypes()[0].isPrimitive())
            {
                throw new MithraNullPrimitiveException("Aggregate result returned null for " + setterMethod.getName() + " of class " + instance.getClass().getName() + " which cannot be set as primitive", e);
            }
            throw new MithraBusinessException("Invalid argument " + valueArray[0] + " passed in invoking method " + setterMethod.getName() + "  of class " + instance.getClass().getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("No valid access to invoke method " + setterMethod.getName() + " of class " + instance.getClass().getName(), e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("Error invoking method " + setterMethod.getName() + "of class " + instance.getClass().getName(), e);
        }
    }


    private Object getValueToSet(Object instance, Object value)
    {
        if ((value != null))
        {
            if (isMethodParameterShort)
            {
                int intVal = ((Number) value).intValue();
                if (intVal < Short.MIN_VALUE || intVal > Short.MAX_VALUE)
                {
                    throw new MithraBusinessException("Aggregate result returned value " + value + " for method " + setterMethod.getName() + "of class " + instance.getClass() + "which cannot be set as short as it out is of short range");
                }
                return (short) intVal;
            }
            else if (isMethodParameterByte)
            {
                int intVal = ((Number) value).intValue();
                if (intVal < Byte.MIN_VALUE || intVal > Byte.MAX_VALUE)
                {
                    throw new MithraBusinessException("Aggregate result returned value " + value + " for method " + setterMethod.getName() + "of class " + instance.getClass() + " which cannot be set as byte as it is out of byte range");
                }
                return (byte) intVal;
            }
        }
        return value;
    }


}
