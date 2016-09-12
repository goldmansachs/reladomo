
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

import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.enumeration.*;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class SingleColumnEnumAttribute<Owner, E extends Enum<E>> extends EnumAttribute<Owner, E> implements SingleColumnAttribute<Owner>
{

    private transient String columnName;
    private transient String uniqueAlias;

    public SingleColumnEnumAttribute(Attribute delegate, String columnName)
    {
        super(delegate);
        this.setColumnName(columnName);
    }

    protected SingleColumnAttribute getSingleColumnDelegate()
    {
        return (SingleColumnAttribute) super.getDelegate();
    }

    public void setSqlParameters(PreparedStatement ps, Object dataObject, int position, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        ((SingleColumnAttribute)this.getDelegate()).setSqlParameters(ps, dataObject, position, databaseTimeZone, databaseType);
    }

    public String getColumnName()
    {
        return this.columnName;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public void setUniqueAlias(String uniqueAlias)
    {
        this.uniqueAlias = uniqueAlias;
    }

    public String valueOfAsString(Owner object, Formatter formatter)
    {
        return formatter.format(this.enumValueOf(object));
    }

    public boolean isSourceAttribute()
    {
        return this.getColumnName() == null;
    }

    public Operation eq(E other)
    {
        return (other == null) ? this.isNull() : new EnumEqOperation<E>(this, other);
    }

    public Operation notEq(E other)
    {
        return (other == null) ? this.isNotNull() : new EnumNotEqOperation<E>(this, other);
    }

    public Operation in(Set<E> enumSet)
    {
        Operation op;
        switch (enumSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(enumSet.iterator().next());
                break;
            default:
                op = new EnumInOperation<E>(this, enumSet);
                break;
        }
        return op;
    }

    public Operation notIn(Set<E> enumSet)
    {
        Operation op;
        switch (enumSet.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(enumSet.iterator().next());
                break;
            default:
                op = new EnumNotInOperation<E>(this, enumSet);
                break;
        }
        return op;
    }

    public <Owner2> Operation eq(EnumAttribute<Owner2, E> other)
    {
        return new MappedOperation(new EnumEqualityMapper<E>(this, other, true), new All(other));
    }

    public <Owner2> Operation notEq(EnumAttribute<Owner2, E> other)
    {
        if (this.getOwnerPortal().equals(other.getOwnerPortal()))
        {
            return new AtomicSelfNotEqualityOperation(this, other);
        }
        throw new RuntimeException("Non-equality join is not yet supported");
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public String getFullyQualifiedLeftHandExpression(SqlQuery query)
    {
        String result = this.getColumnName();
        String databaseAlias = query.getDatabaseAlias(this.getOwnerPortal());
        if (databaseAlias != null)
        {
            if (this.uniqueAlias != null)
            {
                result = databaseAlias + this.uniqueAlias + "." + result;
            }
            else
            {
                result = databaseAlias + "." + result;
            }
        }
        return result;
    }

    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone timeZone) throws SQLException
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void writeValueToStream(Owner object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Object zDecodeColumnarData(ColumnarInStream in, int count) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void zWritePlainTextFromColumnar(Object columnData, int row, ColumnarOutStream out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }
}
