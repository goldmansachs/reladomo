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

package com.gs.fw.common.mithra.connectionmanager;


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

public class DelegatingPreparedStatement extends DelegatingStatement implements PreparedStatement
{
    public DelegatingPreparedStatement(PreparedStatement delegate)
    {
        super(delegate);
    }

    @Override
    public void addBatch() throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).addBatch();
    }

    @Override
    public void clearParameters() throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).clearParameters();
    }

    @Override
    public boolean execute() throws SQLException
    {
        return ((PreparedStatement) this.getDelegate()).execute();
    }

    @Override
    public ResultSet executeQuery() throws SQLException
    {
        return ((PreparedStatement) this.getDelegate()).executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException
    {
        return ((PreparedStatement) this.getDelegate()).executeUpdate();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return ((PreparedStatement) this.getDelegate()).getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        return ((PreparedStatement) this.getDelegate()).getParameterMetaData();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setArray(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBlob(parameterIndex, x);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setByte(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBytes(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setClob(parameterIndex, reader);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setClob(parameterIndex, reader, length);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setClob(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setDate(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setDate(parameterIndex, x, cal);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setDouble(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setFloat(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setLong(parameterIndex, x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNClob(parameterIndex, reader);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNClob(parameterIndex, value);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNString(parameterIndex, value);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNull(parameterIndex, sqlType);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setRef(parameterIndex, x);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setRowId(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setShort(parameterIndex, x);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setString(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setTime(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setTimestamp(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setURL(parameterIndex, x);
    }

    @Override
    public void closeOnCompletion () throws SQLException
    {
        this.getDelegate().closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion () throws SQLException
    {
        return this.getDelegate().isCloseOnCompletion ();
    }
}
