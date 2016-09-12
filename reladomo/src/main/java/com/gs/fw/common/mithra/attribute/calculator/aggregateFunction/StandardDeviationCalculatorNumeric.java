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

package com.gs.fw.common.mithra.attribute.calculator.aggregateFunction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraNullPrimitiveException;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.FloatProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.IntegerProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.LongProcedure;
import com.gs.fw.common.mithra.attribute.numericType.DoubleNumericType;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.MutableDouble;
import com.gs.fw.common.mithra.util.MutableStandardDeviation;
import com.gs.fw.common.mithra.util.Nullable;


public class StandardDeviationCalculatorNumeric extends AbstractAggregateAttributeCalculator
        implements DoubleProcedure, BigDecimalProcedure, FloatProcedure, LongProcedure, IntegerProcedure
{
    private static final long serialVersionUID = 3127653167581549558L;
    private NumericAttribute attribute;

    public StandardDeviationCalculatorNumeric(NumericAttribute attribute)
    {
        super(null);
        this.attribute = attribute;
    }

    public StandardDeviationCalculatorNumeric()
    {
    }

    protected StandardDeviationCalculatorNumeric(String sqlFunction)
    {
        super(sqlFunction);
    }

    @Override
    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        return "(" + query.getDatabaseType().getSqlExpressionForStandardDeviationSample(attribute.getFullyQualifiedLeftHandExpression(query)) + "), count(1)";
    }

    @Override
    public int populateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt) throws SQLException
    {
        MutableDouble obj;
        double d = rs.getDouble(resultSetPosition);
        if (rs.wasNull())
        {
            obj = new MutableDouble();
        }
        else
        {
            obj = new MutableDouble(d);
        }

        int count = rs.getInt(resultSetPosition + 1);
        if (rs.wasNull() || count == 0 || count == 1)
        {
            obj = new MutableDouble();
        }
        else if (!obj.isNull())
        {
            dt.fixSampleStandardDeviation(obj, count);
        }
        data.setValueAt(dataPosition, obj);
        return 2;
    }

    @Override
    public int populateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] scratchArray) throws SQLException
    {
        double d = rs.getDouble(resultSetPosition);
        if (rs.wasNull())
        {
            scratchArray[0] = null;
        }
        else
        {
            scratchArray[0] = d;
        }
        try
        {
            method.invoke(object, scratchArray);
        }
        catch (IllegalArgumentException e)
        {
            if (scratchArray[0] == null && method.getParameterTypes()[0].isPrimitive())
            {
                throw new MithraNullPrimitiveException("Aggregate result returned null for " + method.getName() + " of class " + object.getClass().getName() + " which cannot be set as primitive", e);
            }
            throw new MithraBusinessException("Invalid argument " + scratchArray[0] + " passed in invoking method " + method.getName() + "  of class " + object.getClass().getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("No valid access to invoke method " + method.getName() + " of class " + object.getClass().getName(), e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("Error invoking method " + method.getName() + "of class " + object.getClass().getName(), e);
        }
        return 2;
    }

    public Object aggregate(Object previousValue, Object newValue)
    {
        if (previousValue == null)
        {
            previousValue = new MutableStandardDeviation();
        }
        ((NumericAttribute)getAttribute()).getNumericType().executeForEach(this, (NumericAttribute)getAttribute(),previousValue, newValue );
        //((NumericAttribute)getAttribute()).forEach(this, newValue, previousValue);
        return previousValue;
    }

    @Override
    public Attribute getAttribute()
    {
        return (Attribute)this.attribute;
    }

    public boolean execute(int object, Object context)
    {
        MutableStandardDeviation result = (MutableStandardDeviation) context;
        result.add(object);
        return false;
    }

    public boolean execute(double object, Object context)
    {
        MutableStandardDeviation result = (MutableStandardDeviation) context;
        result.add(object);
        return false;
    }

    public boolean execute(float object, Object context)
    {
        MutableStandardDeviation result = (MutableStandardDeviation) context;
        result.add(object);
        return false;
    }

    public boolean execute(long object, Object context)
    {
        MutableStandardDeviation result = (MutableStandardDeviation) context;
        result.add(object);
        return false;
    }

    public boolean execute(BigDecimal object, Object context)
    {
        MutableStandardDeviation result = (MutableStandardDeviation) context;
        result.add(object.doubleValue());
        return false;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(attribute);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        attribute = (NumericAttribute)in.readObject();
    }

    public Nullable getDefaultValueForEmptyGroup()
    {
        return DoubleNumericType.getInstance().createMutableNumber();
    }

    @Override
    public int hashCode()
    {
        return this.attribute.hashCode() ^ 0x22E602DA;
    }

    @Override
    public Class valueType()
    {
        return Double.class;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof StandardDeviationCalculatorNumeric)) return false;

        StandardDeviationCalculatorNumeric that = (StandardDeviationCalculatorNumeric) o;

        if (!attribute.equals(that.attribute)) return false;

        return true;
    }
}
