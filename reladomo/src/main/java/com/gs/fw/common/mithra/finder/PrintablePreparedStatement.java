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

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.util.FastStringBuffer;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;



public class PrintablePreparedStatement implements PreparedStatement
{

    private String initialStatement;
    private String[] parameters;
    private boolean[] quoted;
    private int paramLength = 0;
    private int firstQuestionMark = 0;

    public static final DateTimeFormatter dateFormat = DateTimeFormat.forPattern("''yyyy-MM-dd''");
    public static final DateTimeFormatter timestampFormat = DateTimeFormat.forPattern("''yyyy-MM-dd HH:mm:ss.SSS''");
    public static final DateTimeFormatter timestampFormatUtc = DateTimeFormat.forPattern("''yyyy-MM-dd HH:mm:ss.SSS''").withZoneUTC();

    public PrintablePreparedStatement(String initialStatement)
    {
        this.initialStatement = initialStatement;
    }

    private void initializeParameters()
    {
        if (parameters == null)
        {
            int length = initialStatement.length();
            int count = 0;
            for (int i = 0; i < length; i++)
            {
                if (initialStatement.charAt(i) == '?')
                {
                    if (count == 0) firstQuestionMark = i;
                    count++;
                }
            }
            parameters = new String[count];
        }
    }

    protected void setQuoted(int parameterIndex)
    {
        if (this.quoted == null)
        {
            this.quoted = new boolean[this.parameters.length];
        }
        this.quoted[parameterIndex - 1] = true;
        this.paramLength += 2;
    }

    protected void setPrintableParameter(int parameterIndex, String value)
    {
        this.initializeParameters();
        if (parameterIndex > parameters.length)
        {
            throw new RuntimeException("parameter " + parameterIndex + " not found in statement: "+initialStatement);
        }
        if (value == null) value = "null";
        this.parameters[parameterIndex - 1] = value;
        this.paramLength += value.length();
    }

    public String getPrintableStatement()
    {
        if (parameters != null)
        {
            FastStringBuffer buffer = new FastStringBuffer(this.initialStatement.length() + paramLength);
            int length = initialStatement.length();
            int count = 0;
            buffer.append(initialStatement, firstQuestionMark);
            for (int i = firstQuestionMark; i < length; i++)
            {
                char c = initialStatement.charAt(i);
                if (c == '?' && parameters[count] != null)
                {
                    if (quoted != null && quoted[count])
                    {
                        buffer.append('\'');
                    }
                    buffer.append(parameters[count]);
                    if (quoted != null && quoted[count])
                    {
                        buffer.append('\'');
                    }
                    count++;
                }
                else
                {
                    buffer.append(c);
                }
            }
            return buffer.toString();
        }
        else return this.initialStatement;
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException
    {
        this.setPrintableParameter(parameterIndex, "" + x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        this.setPrintableParameter(parameterIndex, Double.toString(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        this.setPrintableParameter(parameterIndex, "" + x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException
    {
        this.setPrintableParameter(parameterIndex, Integer.toString(x));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        this.setPrintableParameter(parameterIndex, "null");
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException
    {
        this.setPrintableParameter(parameterIndex, "" + x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException
    {
        this.setPrintableParameter(parameterIndex, "" + x);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException
    {
        this.setPrintableParameter(parameterIndex, x ? "1" : "0");
    }

    @Override
    public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException
    {
        this.setPrintableParameter(paramIndex, "null");
    }

    protected boolean handleNull(int paramIndex, Object x)
    {
        if (x == null)
        {
            this.setPrintableParameter(paramIndex, "null");
            return true;
        }
        return false;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException
    {
        if (!handleNull(parameterIndex, x))
        {
            this.setPrintableParameter(parameterIndex, x);
            this.setQuoted(parameterIndex);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException
    {
        if (!handleNull(parameterIndex, x))
        {
            this.setPrintableParameter(parameterIndex, dateFormat.print(x.getTime()));
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
    {
        if (!handleNull(parameterIndex, x))
        {
            this.setPrintableParameter(parameterIndex, timestampFormat.print(x.getTime()));
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
    {
        if (!handleNull(parameterIndex, x))
        {
            this.setPrintableParameter(parameterIndex, x.toString());
        }
    }

    @Override
    public int getFetchDirection() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getFetchSize() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getMaxFieldSize() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getMaxRows() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getQueryTimeout() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getResultSetConcurrency() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getResultSetHoldability() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getResultSetType() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getUpdateCount() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void cancel() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void clearBatch() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void clearWarnings() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void close() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean getMoreResults() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int[] executeBatch() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setMaxRows(int max) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void addBatch(String sql) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setCursorName(String name) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean execute(String sql) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int executeUpdate(String sql, int columnIndexes[]) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean execute(String sql, int columnIndexes[]) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Connection getConnection() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ResultSet getResultSet() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int executeUpdate(String sql, String columnNames[]) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean execute(String sql, String columnNames[]) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int executeUpdate() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void addBatch() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void clearParameters() throws SQLException
    {
        if (this.parameters != null)
        {
            for(int i=0;i<parameters.length;i++)
            {
                this.parameters[i] = null;
            }
        }
        if (this.quoted != null)
        {
            for(int i=0;i<quoted.length;i++)
            {
                this.quoted[i] = false;
            }
        }
    }

    @Override
    public boolean execute() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    private String convertBytesToString(byte[] data)
    {
        String asString = "";
        for(int i=0;i<data.length;i++)
        {
            String s = Integer.toHexString(((int) data[i]) & 0xFF);
            if (s.length() == 1) asString += "0";
            asString += s;
        }
        return "0x"+asString.toUpperCase();
    }

    @Override
    public void setBytes(int parameterIndex, byte x[]) throws SQLException
    {
        this.setPrintableParameter(parameterIndex, convertBytesToString(x));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setArray(int i, Array x) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setBlob(int i, Blob x) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setClob(int i, Clob x) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setRef(int i, Ref x) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ResultSet executeQuery() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException
    {
        if (x == null)
        {
            this.setPrintableParameter(parameterIndex, "null");
        }
        else
        {
            this.setPrintableParameter(parameterIndex, "'" + x.toString() + "'");
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
    {
        if (x == null)
        {
            this.setPrintableParameter(parameterIndex, "null");
        }
        else
        {
            if (cal.getTimeZone() != MithraTimestamp.UtcTimeZone)
            {
                throw new RuntimeException("not implemented");
            }
            this.setPrintableParameter(parameterIndex, timestampFormatUtc.print(x.getTime()));
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isClosed() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isPoolable() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void closeOnCompletion() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLException("not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return false;
    }
}
