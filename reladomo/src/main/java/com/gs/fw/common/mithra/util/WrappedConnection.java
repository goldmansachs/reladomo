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

    @Override
    public int getHoldability() throws SQLException
    {
        return getUnderlyingConnection().getHoldability();
    }

    @Override
    public int getTransactionIsolation() throws SQLException
    {
        return getUnderlyingConnection().getTransactionIsolation();
    }

    @Override
    public void clearWarnings() throws SQLException
    {
        getUnderlyingConnection().clearWarnings();
    }

    @Override
    public boolean getAutoCommit() throws SQLException
    {
        return getUnderlyingConnection().getAutoCommit();
    }

    @Override
    public boolean isClosed() throws SQLException
    {
        return getUnderlyingConnection().isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException
    {
        return getUnderlyingConnection().isReadOnly();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException
    {
        getUnderlyingConnection().setHoldability(holdability);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException
    {
        getUnderlyingConnection().setTransactionIsolation(level);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        getUnderlyingConnection().setAutoCommit(autoCommit);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException
    {
        getUnderlyingConnection().setReadOnly(readOnly);
    }

    @Override
    public String getCatalog() throws SQLException
    {
        return getUnderlyingConnection().getCatalog();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException
    {
        getUnderlyingConnection().setCatalog(catalog);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException
    {
        return getUnderlyingConnection().getMetaData();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        return getUnderlyingConnection().getWarnings();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException
    {
        return getUnderlyingConnection().setSavepoint();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        getUnderlyingConnection().releaseSavepoint(savepoint);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException
    {
        getUnderlyingConnection().rollback(savepoint);
    }

    @Override
    public Statement createStatement() throws SQLException
    {
        return getUnderlyingConnection().createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
    throws SQLException
    {
        return getUnderlyingConnection().createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException
    {
        return getUnderlyingConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public Map getTypeMap() throws SQLException
    {
        return getUnderlyingConnection().getTypeMap();
    }

    @Override
    public void setTypeMap(Map map) throws SQLException
    {
        getUnderlyingConnection().setTypeMap(map);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException
    {
        return getUnderlyingConnection().nativeSQL(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        return getUnderlyingConnection().prepareCall(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency) throws SQLException
    {
        return getUnderlyingConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException
    {
        return getUnderlyingConnection().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql)
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency)
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int columnIndexes[])
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql, columnIndexes);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException
    {
        return getUnderlyingConnection().setSavepoint(name);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String columnNames[])
    throws SQLException
    {
        return getUnderlyingConnection().prepareStatement(sql, columnNames);
    }

    @Override
    public void close() throws SQLException
    {
        getUnderlyingConnection().close();
    }

    @Override
    public void commit() throws SQLException
    {
        this.getUnderlyingConnection().commit();
    }

    @Override
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

    @Override
    public Clob createClob()
            throws SQLException
    {
        return getUnderlyingConnection().createClob();
    }

    @Override
    public NClob createNClob()
            throws SQLException
    {
        return getUnderlyingConnection().createNClob();
    }

    @Override
    public boolean isValid(int timeout)
            throws SQLException
    {
        return getUnderlyingConnection().isValid(timeout);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface.equals(Connection.class);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (!iface.equals(Connection.class))
        {
            throw new SQLException("Expecting java.sql.Connection interface");
        }
        return this.getUnderlyingConnection().unwrap(iface);

    }

    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException
    {
        getUnderlyingConnection().setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException
    {
        getUnderlyingConnection().setClientInfo(properties);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException
    {
        return getUnderlyingConnection().createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException
    {
        getUnderlyingConnection().setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException
    {
        return getUnderlyingConnection().getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException
    {
        getUnderlyingConnection().abort (executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
    {
        getUnderlyingConnection().setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException
    {
        return getUnderlyingConnection().getNetworkTimeout();
    }

    @Override
    public Properties getClientInfo()
            throws SQLException
    {
        return getUnderlyingConnection().getClientInfo();
    }

    @Override
    public String getClientInfo(String name)
            throws SQLException
    {
        return getUnderlyingConnection().getClientInfo(name);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException
    {
        return getUnderlyingConnection().createArrayOf(typeName, elements);
    }

    @Override
    public Blob createBlob()
            throws SQLException
    {
        return getUnderlyingConnection().createBlob();
    }

    @Override
    public SQLXML createSQLXML()
            throws SQLException
    {
        return getUnderlyingConnection().createSQLXML();
    }
}
