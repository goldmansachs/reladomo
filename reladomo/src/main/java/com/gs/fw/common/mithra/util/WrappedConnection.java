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

package com.gs.fw.common.mithra.util;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;


/**
 * this is a trivial class. It delegates all the calls to the Underlying connection.
 * Interesting behavior is in the subclasses.
 */
public class WrappedConnection implements Connection
{
    private Connection underlyingConnection;

    public WrappedConnection(Connection c)
    {
        this.underlyingConnection = c;
    }

    public Connection getUnderlyingConnection()
    {
        return underlyingConnection;
    }

    public int getHoldability() throws SQLException
    {
        return getUnderlyingConnection().getHoldability();
    }

    public int getTransactionIsolation() throws SQLException
    {
        return getUnderlyingConnection().getTransactionIsolation();
    }

    public void clearWarnings() throws SQLException
    {
        getUnderlyingConnection().clearWarnings();
    }

    public boolean getAutoCommit() throws SQLException
    {
        return getUnderlyingConnection().getAutoCommit();
    }

    public boolean isClosed() throws SQLException
    {
        return getUnderlyingConnection().isClosed();
    }

    public boolean isReadOnly() throws SQLException
    {
        return getUnderlyingConnection().isReadOnly();
    }

    public void setHoldability(int holdability) throws SQLException
    {
        getUnderlyingConnection().setHoldability(holdability);
    }

    public void setTransactionIsolation(int level) throws SQLException
    {
        getUnderlyingConnection().setTransactionIsolation(level);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        getUnderlyingConnection().setAutoCommit(autoCommit);
    }

    public void setReadOnly(boolean readOnly) throws SQLException
    {
        getUnderlyingConnection().setReadOnly(readOnly);
    }

    public String getCatalog() throws SQLException
    {
        return getUnderlyingConnection().getCatalog();
    }

    public void setCatalog(String catalog) throws SQLException
    {
        getUnderlyingConnection().setCatalog(catalog);
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        return getUnderlyingConnection().getMetaData();
    }

    public SQLWarning getWarnings() throws SQLException
    {
        return getUnderlyingConnection().getWarnings();
    }

    public Savepoint setSavepoint() throws SQLException
    {
        return getUnderlyingConnection().setSavepoint();
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        getUnderlyingConnection().releaseSavepoint(savepoint);
    }

    public void rollback(Savepoint savepoint) throws SQLException
    {
        getUnderlyingConnection().rollback(savepoint);
    }

    public Statement createStatement() throws SQLException
    {
        return getUnderlyingConnection().createStatement();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
    throws SQLException
    {
        return getUnderlyingConnection().createStatement(resultSetType, resultSetConcurrency);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                  int resultSetHoldability) throws SQLException
    {
        return getUnderlyingConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public Map getTypeMap() throws SQLException
    {
        return getUnderlyingConnection().getTypeMap();
    }

    public void setTypeMap(Map map) throws SQLException
    {
        getUnderlyingConnection().setTypeMap(map);
    }

    public String nativeSQL(String sql) throws SQLException
    {
        return getUnderlyingConnection().nativeSQL(sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException
    {
        return getUnderlyingConnection().prepareCall(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                  int resultSetConcurrency) throws SQLException
    {
        return getUnderlyingConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                  int resultSetConcurrency,
                  int resultSetHoldability) throws SQLException
    {
        return getUnderlyingConnection().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql)
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql, autoGeneratedKeys);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                       int resultSetConcurrency)
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                       int resultSetConcurrency, int resultSetHoldability)
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int columnIndexes[])
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql, columnIndexes);
    }

    public Savepoint setSavepoint(String name) throws SQLException
    {
        return getUnderlyingConnection().setSavepoint(name);
    }

    public PreparedStatement prepareStatement(String sql, String columnNames[])
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql, columnNames);
    }

    public void close() throws SQLException
    {
        getUnderlyingConnection().close();
    }

    public void commit() throws SQLException
    {
        this.getUnderlyingConnection().commit();
    }

    public void rollback() throws SQLException
    {
        this.getUnderlyingConnection().rollback();
    }

    public boolean equals(Object o)
    {
        if(this == o) return true;
        if (o instanceof WrappedConnection)
        {
            WrappedConnection other = (WrappedConnection) o;
            return this.getUnderlyingConnection().equals(other.getUnderlyingConnection());
        }
        else return this.getUnderlyingConnection().equals(o);
    }

    public int hashCode()
    {
        return this.getUnderlyingConnection().hashCode();
    }

    public Clob createClob()
            throws SQLException
    {
        return getUnderlyingConnection().createClob();
    }

    public NClob createNClob()
            throws SQLException
    {
        return getUnderlyingConnection().createNClob();
    }

    public boolean isValid(int timeout)
            throws SQLException
    {
        return getUnderlyingConnection().isValid(timeout);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface.equals(Connection.class);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (!iface.equals(Connection.class))
        {
            throw new SQLException("Expecting java.sql.Connection interface");
        }
        return this.getUnderlyingConnection().unwrap(iface);

    }

    public void setClientInfo(String name, String value)
            throws SQLClientInfoException
    {
        getUnderlyingConnection().setClientInfo(name, value);
    }

    public void setClientInfo(Properties properties)
            throws SQLClientInfoException
    {
        getUnderlyingConnection().setClientInfo(properties);
    }

    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException
    {
        return getUnderlyingConnection().createStruct(typeName, attributes);
    }

    public void setSchema(String schema) throws SQLException
    {
        getUnderlyingConnection().setSchema(schema);
    }

    public String getSchema() throws SQLException
    {
        return getUnderlyingConnection().getSchema();
    }

    public void abort(Executor executor) throws SQLException
    {
        getUnderlyingConnection().abort (executor);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
    {
        getUnderlyingConnection().setNetworkTimeout(executor, milliseconds);
    }

    public int getNetworkTimeout() throws SQLException
    {
        return getUnderlyingConnection().getNetworkTimeout();
    }

    public Properties getClientInfo()
            throws SQLException
    {
        return getUnderlyingConnection().getClientInfo();
    }

    public String getClientInfo(String name)
            throws SQLException
    {
        return getUnderlyingConnection().getClientInfo(name);
    }

    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException
    {
        return getUnderlyingConnection().createArrayOf(typeName, elements);
    }

    public Blob createBlob()
            throws SQLException
    {
        return getUnderlyingConnection().createBlob();
    }

    public SQLXML createSQLXML()
            throws SQLException
    {
        return getUnderlyingConnection().createSQLXML();
    }
}
