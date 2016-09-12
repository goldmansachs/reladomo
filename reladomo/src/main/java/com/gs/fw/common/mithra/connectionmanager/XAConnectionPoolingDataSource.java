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

import com.gs.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.transaction.LocalTm;
import com.gs.fw.common.mithra.transaction.TransactionLocal;
import com.gs.fw.common.mithra.util.WrappedConnection;

import javax.sql.DataSource;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class XAConnectionPoolingDataSource implements DataSource
{

    private static final Logger logger = LoggerFactory.getLogger(XAConnectionPoolingDataSource.class.getName());
    private TransactionLocal txLocalXaResource = new TransactionLocal();
    private DatabaseType databaseType;
    private ObjectPoolWithThreadAffinity objectPool;

    public XAConnectionPoolingDataSource(ObjectPoolWithThreadAffinity objectPool, DatabaseType databaseType)
    {
        this.objectPool = objectPool;
        this.databaseType = databaseType;
    }

    public DatabaseType getDatabaseType()
    {
        return databaseType;
    }

    protected ObjectPoolWithThreadAffinity getPool()
    {
        return objectPool;
    }

    private Connection getConnectionForTransaction(MithraTransaction transaction) throws SQLException
    {
        boolean mustParticipate = transaction.isWriteOperationMode();
        JDBCConnectionXAResource resource = null;
        resource = (JDBCConnectionXAResource) txLocalXaResource.get(transaction);
        if(resource == null)
        {
            if (mustParticipate)
            {
                resource = new JDBCConnectionXAResource(transaction);
                try
                {
                    transaction.enlistResource(resource);
                }
                catch (SystemException e)
                {
                    throw new RuntimeException("unable to enlist resource in transaction", e);
                }
                catch (RollbackException e)
                {
                    throw new RuntimeException("unable to enlist resource in transaction", e);
                }
                txLocalXaResource.set(transaction, resource);
                if(logger.isDebugEnabled())
                {
                    logger.debug("Thread: "+Thread.currentThread().getName()+":  Transaction "+transaction.getTransactionName()+" has enlisted resource "+resource.toString());
                }
            }
            else
            {
                return this.getConnectionForNoTransaction();
            }
        }

        if(logger.isDebugEnabled())
        {
            logger.debug("Thread: "+Thread.currentThread().getName()+": is getting Connection ");
        }
        return resource.getConnection();
    }

    public Connection getConnection() throws SQLException
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if(currentTransaction != null)
        {
            return getConnectionForTransaction(currentTransaction);
        }
        else
        {
            return getConnectionForNoTransaction();
        }
    }

    private Connection getConnectionForNoTransaction()
            throws SQLException
    {
        while (true)
        {
            Connection connection = this.getConnectionFromPool();

            try
            {
                connection.setAutoCommit(true);
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                return connection;
            }
            catch (SQLException e)
            {
                boolean isDead = false;
                if (databaseType != null && databaseType.isConnectionDead(e))
                {
                    logger.warn("detected dead connection, closing and retrying");
                    try
                    {
                        getPool().invalidateObject(connection);
                        isDead = true;
                    }
                    catch (Exception e1)
                    {
                        SQLException sqlException = new SQLException("could not invalidate bad connection ");
                        sqlException.initCause(e1);
                        throw sqlException;
                    }
                }
                else
                {
                    connection.close();
                }
                if (isDead) continue;
                throw e;
            }
        }
    }

    private Connection getConnectionFromPool() throws SQLException
    {
        try
        {
            return (Connection)(this.objectPool.borrowObject());
        }
        catch (Exception e)
        {
            if (e instanceof SQLException)
            {
                throw (SQLException) e;
            }
            throw new SQLException("Could not borrow object from pool", e);
        }
    }

    // unused methods

    @Override
    public Connection getConnection(String username, String password) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getLoginTimeout() throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    // end of unused methods

    private static class XAConnectionWrapper extends WrappedConnection implements PostTransactionExecutor
    {
        private JDBCConnectionXAResource resource;
        private List<PostTransactionAction> postTransactionActions;

        public XAConnectionWrapper(Connection c)
        {
            super(c);
        }

        @Override
        public void addPostTransactionAction(PostTransactionAction action)
        {
            if (postTransactionActions == null)
            {
                postTransactionActions = FastList.newList();
            }
            postTransactionActions.add(action);
        }

        private void setResource(JDBCConnectionXAResource resource)
        {
            this.resource = resource;
        }

        public void close() throws SQLException
        {
            resource.makeConnectionAvailable(this);
        }

        private void protectedCommit() throws SQLException
        {
            this.getUnderlyingConnection().commit();
            safeClose(this.getUnderlyingConnection());
        }

        private void protectedRollback() throws SQLException
        {
            SQLException failedRollback = null;
            Connection underlyingCon = this.getUnderlyingConnection();
            if (!underlyingCon.isClosed())
            {
                try
                {
                    underlyingCon.rollback();
                }
                catch (SQLException e)
                {
                    failedRollback = e;
                }
            }
            safeClose(underlyingCon);
            if (failedRollback != null) throw failedRollback;
        }

        private void safeClose(Connection underlyingCon)
                throws SQLException
        {
            if (postTransactionActions != null)
            {
                for(int i=0;i<postTransactionActions.size();i++)
                {
                    try
                    {
                        postTransactionActions.get(i).execute(this);
                    }
                    catch (Exception e)
                    {
                        logger.error("Ignoring post transaction error", e);
                    }
                }
            }
            try
            {
                underlyingCon.close();
            }
            catch(SQLException e)
            {
                if (!"Already closed.".equals(e.getMessage())) throw e;
            }
        }

        public void commit() throws SQLException
        {
            throw new SQLException("Commit on connection not allowed. Commit the transaction instead");
        }

        public void rollback() throws SQLException
        {
            throw new SQLException("Rollback on connection not allowed. Rollback the transaction instead");
        }
    }

    private class JDBCConnectionXAResource implements XAResource, LocalTm.SinglePhaseResource
    {
        private List availableConnections = new ArrayList();
        private List connectionsInUse = new ArrayList();

        private boolean commited;
        private boolean rolledback = false;
        private final MithraTransaction ownerTransaction;

        public JDBCConnectionXAResource(MithraTransaction tx)
        {
            this.ownerTransaction = tx;
        }


        private void makeConnectionAvailable(XAConnectionWrapper connection)
        {
            if(!availableConnections.contains(connection))
                availableConnections.add(connection);
            connectionsInUse.remove(connection);
            logConnectionStats();
        }

        private void logConnectionStats()
        {
            if(logger.isDebugEnabled())
            {
                logger.debug("available : " + availableConnections.size() + " in use: " + connectionsInUse.size());
            }
        }

        public Connection getConnection() throws SQLException
        {
            XAConnectionWrapper result = null;
            if(availableConnections.size() > 0)
            {
                result = (XAConnectionWrapper)availableConnections.remove(availableConnections.size() - 1);
            }
            else
            {
                result = getFreshConnectionFromPool();
            }
            connectionsInUse.add(result);
            logConnectionStats();
            return result;
        }

        private XAConnectionWrapper getFreshConnectionFromPool()
                throws SQLException
        {
            XAConnectionWrapper result;
            Connection freshConnectionFromPool = null;
            while(true)
            {
                DatabaseType databaseType = getDatabaseType();
                try
                {
                    freshConnectionFromPool = XAConnectionPoolingDataSource.this.getConnectionFromPool();
                    result = new XAConnectionWrapper(freshConnectionFromPool);
                    result.setAutoCommit(false);
                    result.setTransactionIsolation(databaseType.zGetTxLevel());
                    result.setResource(this);
                    return result;
                }
                catch (SQLException e)
                {
                    if (freshConnectionFromPool != null)
                    {
                        boolean isDead = false;
                        if (databaseType != null && databaseType.isConnectionDead(e))
                        {
                            logger.warn("detected dead connection, closing and retrying");
                            try
                            {
                                getPool().invalidateObject(freshConnectionFromPool);
                                isDead = true;
                            }
                            catch (Exception e1)
                            {
                                SQLException sqlException = new SQLException("could not invalidate bad connection ");
                                sqlException.initCause(e1);
                                throw sqlException;
                            }
                        }
                        else
                        {
                            freshConnectionFromPool.close();
                        }
                        if (isDead) continue;
                    }
                    throw e;
                }
            }
        }

        public int getTransactionTimeout() throws XAException
        {
            return -1;
        }

        public boolean setTransactionTimeout(int i) throws XAException
        {
            return false;
        }

        public boolean isSameRM(XAResource xaResource) throws XAException
        {
            return this.equals(xaResource);
        }

        public Xid[] recover(int i) throws XAException
        {
            return new Xid[0];
        }

        public int prepare(Xid xid) throws XAException
        {
            return 0;
        }

        public void forget(Xid xid) throws XAException
        {
        }

        public void rollback(Xid xid) throws XAException
        {
            if(commited)
            {
                throw new XAException("Transaction is already committed");
            }
            if (rolledback) return;
            boolean success = rollbackConnections(availableConnections) && rollbackConnections(connectionsInUse);
            if(!success)
            {
                throw new XAException("Rollback failed");
            }
        }

        public void end(Xid xid, int i) throws XAException
        {
            txLocalXaResource.set(ownerTransaction, null);
        }

        public void start(Xid xid, int i) throws XAException
        {
        }

        public boolean rollbackConnections(List connectionList)
        {
            boolean rollbackSuccess = true;
            for (int i = 0; i < connectionList.size(); i++)
            {
                XAConnectionWrapper connection = (XAConnectionWrapper) connectionList.get(i);
                try
                {
                    connection.protectedRollback();
                }
                catch (SQLException e)
                {
                    logger.error("Rollback failed", e);
                    rollbackSuccess = false;
                }
            }
            rolledback = true;
            return rollbackSuccess;
        }

        public void commitConnections(List connectionList) throws SQLException
        {
            for (int i = 0; i < connectionList.size(); i++)
            {
                XAConnectionWrapper connection = (XAConnectionWrapper) connectionList.get(i);
                connection.protectedCommit();
            }
        }

        public void commit(Xid xid, boolean b) throws XAException
        {
            if(commited)
            {
                throw new XAException("Transaction is already committed");
            }
            try
            {
                if(connectionsInUse.size() != 0)
                {
                    throw new XAException("Detected open jdbc connections. Change code to close all connection borrowed from the connection manager");
                }
                commitConnections(availableConnections);
                commited = true;
            }
            catch (SQLException e)
            {
                logger.error("Commit failed", e);
                rollbackConnections(availableConnections); // if commit fails, we must ensure we've already rolledback
                XAException xaException = new XAException("Commit Failed - "+e.getMessage());
                xaException.initCause(e);
                throw xaException;
            }
        }
    }

}