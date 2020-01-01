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

    public void addBatch() throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).addBatch();
    }

    public void clearParameters() throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).clearParameters();
    }

    public boolean execute() throws SQLException
    {
        return ((PreparedStatement) this.getDelegate()).execute();
    }

    public ResultSet executeQuery() throws SQLException
    {
        return ((PreparedStatement) this.getDelegate()).executeQuery();
    }

    public int executeUpdate() throws SQLException
    {
        return ((PreparedStatement) this.getDelegate()).executeUpdate();
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        return ((PreparedStatement) this.getDelegate()).getMetaData();
    }

    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        return ((PreparedStatement) this.getDelegate()).getParameterMetaData();
    }

    public void setArray(int parameterIndex, Array x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setArray(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setAsciiStream(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setAsciiStream(parameterIndex, x, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setAsciiStream(parameterIndex, x, length);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBigDecimal(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBinaryStream(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBinaryStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBinaryStream(parameterIndex, x, length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBlob(parameterIndex, inputStream);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBlob(parameterIndex, inputStream, length);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBlob(parameterIndex, x);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBoolean(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setByte(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setBytes(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setCharacterStream(parameterIndex, reader);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setCharacterStream(parameterIndex, reader, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setCharacterStream(parameterIndex, reader, length);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setClob(parameterIndex, reader);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setClob(parameterIndex, reader, length);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setClob(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setDate(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setDate(parameterIndex, x, cal);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setDouble(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setFloat(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setInt(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setLong(parameterIndex, x);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNCharacterStream(parameterIndex, value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNCharacterStream(parameterIndex, value, length);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNClob(parameterIndex, reader);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNClob(parameterIndex, reader, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNClob(parameterIndex, value);
    }

    public void setNString(int parameterIndex, String value) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNString(parameterIndex, value);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNull(parameterIndex, sqlType);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setNull(parameterIndex, sqlType, typeName);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setObject(parameterIndex, x);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setObject(parameterIndex, x, targetSqlType);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setRef(parameterIndex, x);
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setRowId(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setShort(parameterIndex, x);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setSQLXML(parameterIndex, xmlObject);
    }

    public void setString(int parameterIndex, String x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setString(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setTime(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setTime(parameterIndex, x, cal);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setTimestamp(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setTimestamp(parameterIndex, x, cal);
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setUnicodeStream(parameterIndex, x, length);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException
    {
        ((PreparedStatement) this.getDelegate()).setURL(parameterIndex, x);
    }

    public boolean isCloseOnCompletion() throws SQLException
    {
        return this.getDelegate().isCloseOnCompletion();
    }
}
