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
import java.util.Map;

public class DelegatingCallableStatement extends DelegatingPreparedStatement implements CallableStatement
{
    public DelegatingCallableStatement(PreparedStatement delegate)
    {
        super(delegate);
    }

    protected CallableStatement getDelegate()
    {
        return (CallableStatement) super.getDelegate();
    }

    public Array getArray(int parameterIndex) throws SQLException
    {
        return getDelegate().getArray(parameterIndex);
    }

    public Array getArray(String parameterName) throws SQLException
    {
        return getDelegate().getArray(parameterName);
    }

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException
    {
        return getDelegate().getBigDecimal(parameterIndex);
    }

    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException
    {
        return getDelegate().getBigDecimal(parameterIndex, scale);
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException
    {
        return getDelegate().getBigDecimal(parameterName);
    }

    public Blob getBlob(int parameterIndex) throws SQLException
    {
        return getDelegate().getBlob(parameterIndex);
    }

    public Blob getBlob(String parameterName) throws SQLException
    {
        return getDelegate().getBlob(parameterName);
    }

    public boolean getBoolean(int parameterIndex) throws SQLException
    {
        return getDelegate().getBoolean(parameterIndex);
    }

    public boolean getBoolean(String parameterName) throws SQLException
    {
        return getDelegate().getBoolean(parameterName);
    }

    public byte getByte(int parameterIndex) throws SQLException
    {
        return getDelegate().getByte(parameterIndex);
    }

    public byte getByte(String parameterName) throws SQLException
    {
        return getDelegate().getByte(parameterName);
    }

    public byte[] getBytes(int parameterIndex) throws SQLException
    {
        return getDelegate().getBytes(parameterIndex);
    }

    public byte[] getBytes(String parameterName) throws SQLException
    {
        return getDelegate().getBytes(parameterName);
    }

    public Reader getCharacterStream(int parameterIndex) throws SQLException
    {
        return getDelegate().getCharacterStream(parameterIndex);
    }

    public Reader getCharacterStream(String parameterName) throws SQLException
    {
        return getDelegate().getCharacterStream(parameterName);
    }

    public Clob getClob(int parameterIndex) throws SQLException
    {
        return getDelegate().getClob(parameterIndex);
    }

    public Clob getClob(String parameterName) throws SQLException
    {
        return getDelegate().getClob(parameterName);
    }

    public Date getDate(int parameterIndex) throws SQLException
    {
        return getDelegate().getDate(parameterIndex);
    }

    public Date getDate(int parameterIndex, Calendar cal) throws SQLException
    {
        return getDelegate().getDate(parameterIndex, cal);
    }

    public Date getDate(String parameterName) throws SQLException
    {
        return getDelegate().getDate(parameterName);
    }

    public Date getDate(String parameterName, Calendar cal) throws SQLException
    {
        return getDelegate().getDate(parameterName, cal);
    }

    public double getDouble(int parameterIndex) throws SQLException
    {
        return getDelegate().getDouble(parameterIndex);
    }

    public double getDouble(String parameterName) throws SQLException
    {
        return getDelegate().getDouble(parameterName);
    }

    public float getFloat(int parameterIndex) throws SQLException
    {
        return getDelegate().getFloat(parameterIndex);
    }

    public float getFloat(String parameterName) throws SQLException
    {
        return getDelegate().getFloat(parameterName);
    }

    public int getInt(int parameterIndex) throws SQLException
    {
        return getDelegate().getInt(parameterIndex);
    }

    public int getInt(String parameterName) throws SQLException
    {
        return getDelegate().getInt(parameterName);
    }

    public long getLong(int parameterIndex) throws SQLException
    {
        return getDelegate().getLong(parameterIndex);
    }

    public long getLong(String parameterName) throws SQLException
    {
        return getDelegate().getLong(parameterName);
    }

    public Reader getNCharacterStream(int parameterIndex) throws SQLException
    {
        return getDelegate().getNCharacterStream(parameterIndex);
    }

    public Reader getNCharacterStream(String parameterName) throws SQLException
    {
        return getDelegate().getNCharacterStream(parameterName);
    }

    public NClob getNClob(int parameterIndex) throws SQLException
    {
        return getDelegate().getNClob(parameterIndex);
    }

    public NClob getNClob(String parameterName) throws SQLException
    {
        return getDelegate().getNClob(parameterName);
    }

    public String getNString(int parameterIndex) throws SQLException
    {
        return getDelegate().getNString(parameterIndex);
    }

    public String getNString(String parameterName) throws SQLException
    {
        return getDelegate().getNString(parameterName);
    }

    public Object getObject(int parameterIndex) throws SQLException
    {
        return getDelegate().getObject(parameterIndex);
    }

    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException
    {
        return getDelegate().getObject(parameterIndex, map);
    }

    public Object getObject(String parameterName) throws SQLException
    {
        return getDelegate().getObject(parameterName);
    }

    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException
    {
        return getDelegate().getObject(parameterName, map);
    }

    public Ref getRef(int parameterIndex) throws SQLException
    {
        return getDelegate().getRef(parameterIndex);
    }

    public Ref getRef(String parameterName) throws SQLException
    {
        return getDelegate().getRef(parameterName);
    }

    public RowId getRowId(int parameterIndex) throws SQLException
    {
        return getDelegate().getRowId(parameterIndex);
    }

    public RowId getRowId(String parameterName) throws SQLException
    {
        return getDelegate().getRowId(parameterName);
    }

    public short getShort(int parameterIndex) throws SQLException
    {
        return getDelegate().getShort(parameterIndex);
    }

    public short getShort(String parameterName) throws SQLException
    {
        return getDelegate().getShort(parameterName);
    }

    public SQLXML getSQLXML(int parameterIndex) throws SQLException
    {
        return getDelegate().getSQLXML(parameterIndex);
    }

    public SQLXML getSQLXML(String parameterName) throws SQLException
    {
        return getDelegate().getSQLXML(parameterName);
    }

    public String getString(int parameterIndex) throws SQLException
    {
        return getDelegate().getString(parameterIndex);
    }

    public String getString(String parameterName) throws SQLException
    {
        return getDelegate().getString(parameterName);
    }

    public Time getTime(int parameterIndex) throws SQLException
    {
        return getDelegate().getTime(parameterIndex);
    }

    public Time getTime(int parameterIndex, Calendar cal) throws SQLException
    {
        return getDelegate().getTime(parameterIndex, cal);
    }

    public Time getTime(String parameterName) throws SQLException
    {
        return getDelegate().getTime(parameterName);
    }

    public Time getTime(String parameterName, Calendar cal) throws SQLException
    {
        return getDelegate().getTime(parameterName, cal);
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException
    {
        return getDelegate().getTimestamp(parameterIndex);
    }

    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException
    {
        return getDelegate().getTimestamp(parameterIndex, cal);
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException
    {
        return getDelegate().getTimestamp(parameterName);
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException
    {
        return getDelegate().getTimestamp(parameterName, cal);
    }

    public URL getURL(int parameterIndex) throws SQLException
    {
        return getDelegate().getURL(parameterIndex);
    }

    public URL getURL(String parameterName) throws SQLException
    {
        return getDelegate().getURL(parameterName);
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException
    {
        getDelegate().registerOutParameter(parameterIndex, sqlType);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException
    {
        getDelegate().registerOutParameter(parameterIndex, sqlType, scale);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        getDelegate().registerOutParameter(parameterIndex, sqlType, typeName);
    }

    public void registerOutParameter(String parameterName, int sqlType) throws SQLException
    {
        getDelegate().registerOutParameter(parameterName, sqlType);
    }

    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException
    {
        getDelegate().registerOutParameter(parameterName, sqlType, scale);
    }

    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException
    {
        getDelegate().registerOutParameter(parameterName, sqlType, typeName);
    }

    public void setAsciiStream(String parameterName, InputStream x) throws SQLException
    {
        getDelegate().setAsciiStream(parameterName, x);
    }

    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException
    {
        getDelegate().setAsciiStream(parameterName, x, length);
    }

    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException
    {
        getDelegate().setAsciiStream(parameterName, x, length);
    }

    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException
    {
        getDelegate().setBigDecimal(parameterName, x);
    }

    public void setBinaryStream(String parameterName, InputStream x) throws SQLException
    {
        getDelegate().setBinaryStream(parameterName, x);
    }

    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException
    {
        getDelegate().setBinaryStream(parameterName, x, length);
    }

    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException
    {
        getDelegate().setBinaryStream(parameterName, x, length);
    }

    public void setBlob(String parameterName, InputStream inputStream) throws SQLException
    {
        getDelegate().setBlob(parameterName, inputStream);
    }

    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException
    {
        getDelegate().setBlob(parameterName, inputStream, length);
    }

    public void setBlob(String parameterName, Blob x) throws SQLException
    {
        getDelegate().setBlob(parameterName, x);
    }

    public void setBoolean(String parameterName, boolean x) throws SQLException
    {
        getDelegate().setBoolean(parameterName, x);
    }

    public void setByte(String parameterName, byte x) throws SQLException
    {
        getDelegate().setByte(parameterName, x);
    }

    public void setBytes(String parameterName, byte[] x) throws SQLException
    {
        getDelegate().setBytes(parameterName, x);
    }

    public void setCharacterStream(String parameterName, Reader reader) throws SQLException
    {
        getDelegate().setCharacterStream(parameterName, reader);
    }

    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException
    {
        getDelegate().setCharacterStream(parameterName, reader, length);
    }

    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException
    {
        getDelegate().setCharacterStream(parameterName, reader, length);
    }

    public void setClob(String parameterName, Reader reader) throws SQLException
    {
        getDelegate().setClob(parameterName, reader);
    }

    public void setClob(String parameterName, Reader reader, long length) throws SQLException
    {
        getDelegate().setClob(parameterName, reader, length);
    }

    public void setClob(String parameterName, Clob x) throws SQLException
    {
        getDelegate().setClob(parameterName, x);
    }

    public void setDate(String parameterName, Date x) throws SQLException
    {
        getDelegate().setDate(parameterName, x);
    }

    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException
    {
        getDelegate().setDate(parameterName, x, cal);
    }

    public void setDouble(String parameterName, double x) throws SQLException
    {
        getDelegate().setDouble(parameterName, x);
    }

    public void setFloat(String parameterName, float x) throws SQLException
    {
        getDelegate().setFloat(parameterName, x);
    }

    public void setInt(String parameterName, int x) throws SQLException
    {
        getDelegate().setInt(parameterName, x);
    }

    public void setLong(String parameterName, long x) throws SQLException
    {
        getDelegate().setLong(parameterName, x);
    }

    public void setNCharacterStream(String parameterName, Reader value) throws SQLException
    {
        getDelegate().setNCharacterStream(parameterName, value);
    }

    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException
    {
        getDelegate().setNCharacterStream(parameterName, value, length);
    }

    public void setNClob(String parameterName, Reader reader) throws SQLException
    {
        getDelegate().setNClob(parameterName, reader);
    }

    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException
    {
        return getDelegate().getObject (parameterIndex,type);
    }

    public <T> T getObject(String parameterName, Class<T> type) throws SQLException
    {
        return getDelegate().getObject (parameterName, type);
    }

    public void setNClob(String parameterName, Reader reader, long length) throws SQLException
    {
        getDelegate().setNClob(parameterName, reader, length);
    }

    public void setNClob(String parameterName, NClob value) throws SQLException
    {
        getDelegate().setNClob(parameterName, value);
    }

    public void setNString(String parameterName, String value) throws SQLException
    {
        getDelegate().setNString(parameterName, value);
    }

    public void setNull(String parameterName, int sqlType) throws SQLException
    {
        getDelegate().setNull(parameterName, sqlType);
    }

    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException
    {
        getDelegate().setNull(parameterName, sqlType, typeName);
    }

    public void setObject(String parameterName, Object x) throws SQLException
    {
        getDelegate().setObject(parameterName, x);
    }

    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException
    {
        getDelegate().setObject(parameterName, x, targetSqlType);
    }

    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException
    {
        getDelegate().setObject(parameterName, x, targetSqlType, scale);
    }

    public void setRowId(String parameterName, RowId x) throws SQLException
    {
        getDelegate().setRowId(parameterName, x);
    }

    public void setShort(String parameterName, short x) throws SQLException
    {
        getDelegate().setShort(parameterName, x);
    }

    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException
    {
        getDelegate().setSQLXML(parameterName, xmlObject);
    }

    public void setString(String parameterName, String x) throws SQLException
    {
        getDelegate().setString(parameterName, x);
    }

    public void setTime(String parameterName, Time x) throws SQLException
    {
        getDelegate().setTime(parameterName, x);
    }

    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException
    {
        getDelegate().setTime(parameterName, x, cal);
    }

    public void setTimestamp(String parameterName, Timestamp x) throws SQLException
    {
        getDelegate().setTimestamp(parameterName, x);
    }

    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException
    {
        getDelegate().setTimestamp(parameterName, x, cal);
    }

    public void setURL(String parameterName, URL val) throws SQLException
    {
        getDelegate().setURL(parameterName, val);
    }

    public boolean wasNull() throws SQLException
    {
        return getDelegate().wasNull();
    }
}
