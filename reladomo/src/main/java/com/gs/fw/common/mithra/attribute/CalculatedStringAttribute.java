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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.StringAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.StringProcedure;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.StringExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.slf4j.Logger;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;


public class CalculatedStringAttribute<Owner> extends StringAttribute<Owner> implements StringExtractor<Owner>, SingleColumnAttribute<Owner>
{
    private static final long serialVersionUID = -3615678815516468830L;
    
    private StringAttributeCalculator calculator;

    public CalculatedStringAttribute(StringAttributeCalculator calculator)
    {
        this.calculator = calculator;
    }

    public SourceAttributeType getSourceAttributeType()
    {
        return this.getOwnerPortal().getFinder().getSourceAttributeType();
    }

    public Attribute getSourceAttribute()
    {
        return this.getOwnerPortal().getFinder().getSourceAttribute();
    }

    public String zGetTopOwnerClassName()
    {
        return this.calculator.getTopOwnerClassName();
    }

    public MithraObjectPortal getOwnerPortal()
    {
        return this.calculator.getOwnerPortal();
    }

    public Object readResolve()
    {
        return this;
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return this.calculator.getAsOfAttributes();
    }

    protected void serializedNonNullValue(Owner o, ObjectOutput out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    protected void deserializedNonNullValue(Owner o, ObjectInput in) throws IOException, ClassNotFoundException
    {
        throw new RuntimeException("not implemented");
    }

    public void forEach(final StringProcedure proc, Owner o, Object context)
    {
        throw new RuntimeException("not implemented");
    }

    public StringAttributeCalculator getCalculator()
    {
        return this.calculator;
    }

    public boolean isAttributeNull(Object o)
    {
        return this.calculator.isAttributeNull(o);
    }

    public String stringValueOf(Object o)
    {
        return this.calculator.stringValueOf(o);
    }

    public void setStringValue(Object o, String newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueNull(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueUntil(Owner o, String newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public String getFullyQualifiedLeftHandExpression(SqlQuery query)
    {
        return this.calculator.getFullyQualifiedCalculatedExpression(query);
    }

    @Override
    public void zAppendToString(ToStringContext toStringContext)
    {
        this.calculator.appendToString(toStringContext);
    }

    public String getColumnName()
    {
        throw new RuntimeException("method getColumName() can not be called on a calculated attribute");        
    }

    public Class valueType()
    {
        return String.class;
    }

    public boolean isSourceAttribute()
    {
        return false;
    }

    public void setValue(Owner o, String newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public int valueHashCode(Object o)
    {
        if (this.isAttributeNull(o))
        {
            return HashUtil.NULL_HASH;
        }
        return this.stringValueOf(o).hashCode();
    }

    public boolean valueEquals(Object first, Object second)
    {
        if (first == second)
        {
            return true;
        }
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = this.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.stringValueOf(first).equals(this.stringValueOf(second));
        }
        return true;
    }

    public <O> boolean valueEquals(Owner first, O second, Extractor<O, String> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.stringValueOf(first).equals(((StringExtractor) secondExtractor).stringValueOf(second));
        }
        return true;
    }

    public String valueOf(Owner anObject)
    {
        return this.stringValueOf(anObject);
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        this.calculator.addDepenedentAttributesToSet(set);
    }

    // join operation:
    public Operation eq(StringAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation joinEq(StringAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation filterEq(StringAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation notEq(StringAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public int getUpdateCount()
    {
        return this.calculator.getUpdateCount();
    }

    public int getNonTxUpdateCount()
    {
        return this.calculator.getNonTxUpdateCount();
    }

    public void incrementUpdateCount()
    {
        this.calculator.incrementUpdateCount();
    }

    public void commitUpdateCount()
    {
        this.calculator.commitUpdateCount();
    }

    public void rollbackUpdateCount()
    {
        this.calculator.rollbackUpdateCount();
    }

    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        return other instanceof CalculatedStringAttribute && ((CalculatedStringAttribute)other).calculator.equals(this.calculator);
    }

    public int hashCode()
    {
        return super.hashCode() ^ this.calculator.hashCode();
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        throw new RuntimeException("should never get here");
    }

    public void zAddDependentPortalsToSet(Set set)
    {
        this.calculator.addDependentPortalsToSet(set);
    }

    public void zAddDepenedentAttributesToSet(Set set)
    {
        this.calculator.addDepenedentAttributesToSet(set);
    }


    // SingleColumnAttribute - todo - try to move/remove the unused methods
    @Override
    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setColumnName(String columnName)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeValueToStream(Owner object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean verifyColumn(ColumnInfo info)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setSqlParameters(PreparedStatement ps, Object dataObject, int position, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return this.calculator.createTupleAttribute(pos, tupleTempContext);
    }

    @Override
    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone timeZone) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Object zDecodeColumnarData(ColumnarInStream in, int count) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void zWritePlainTextFromColumnar(Object columnData, int row, ColumnarOutStream out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }
}
