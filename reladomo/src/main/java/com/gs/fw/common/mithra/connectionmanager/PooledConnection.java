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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.connectionmanager;


import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.SynchronizedLruMap;
import com.gs.fw.common.mithra.util.WrappedConnection;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class PooledConnection extends WrappedConnection
{
    private static final StatementDestroyAction DESTROY_ACTION = new StatementDestroyAction();

    private static final int STATE_ACTIVE = 1;
    private static final int STATE_IN_POOL = 2;
    private static final int STATE_DEAD = 3;

    private static Logger logger = LoggerFactory.getLogger(PooledConnection.class.getName());
    private final ObjectPoolWithThreadAffinity pool;
    private final SynchronizedLruMap<StatementKey, PooledPreparedStatement> statementPool;
    private final List<Statement> allStatements = FastList.newList();

    private int state = STATE_ACTIVE;

    public PooledConnection(Connection c, ObjectPoolWithThreadAffinity<? extends Connection> pool, int statementsToPool)
    {
        super(c);
        this.pool = pool;
        if (statementsToPool == 0)
        {
            statementPool = null;
        }
        else
        {
            statementPool = new SynchronizedLruMap(8, statementsToPool, DESTROY_ACTION);
        }
    }

    public synchronized void activate() throws SQLException
    {
        checkNotDead();
        this.state = STATE_ACTIVE;
    }

    private void checkNotDead() throws SQLException
    {
        if (this.state == STATE_DEAD)
        {
            throw new SQLException("Connection is closed");
        }
    }

    public synchronized void close() throws SQLException
    {
        closeStatements();
        boolean isClosed = false;
        try
        {
            isClosed = this.state == STATE_DEAD || isClosed();
        }
        catch (SQLException e)
        {
            invalidateIfNotDead();
            throw new SQLException("Cannot close connection (isClosed check failed)", e);
        }
        if (isClosed)
        {
            invalidateIfNotDead();
        }
        else if (this.state != STATE_IN_POOL)
        {
            try
            {
                pool.returnObject(this);
                this.state = STATE_IN_POOL;
            }
            catch (SQLException e)
            {
                throw e;
            }
            catch (RuntimeException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                throw new SQLException("Cannot close connection (return to pool failed)", e);
            }
        }
        else
        {
            logger.info("connection not returned to pool as it was already closed " + this.state);
        }
    }

    private void invalidateIfNotDead()
    {
        if (this.state != STATE_DEAD)
        {
            this.state = STATE_DEAD;
            try
            {
                pool.invalidateObject(this);
            }
            catch (Exception e2)
            {
                logger.warn("invalidation failed", e2);
            }
        }
    }

    private synchronized void closeStatements()
    {
        for(int i=0;i<allStatements.size();i++)
        {
            try
            {
                allStatements.get(i).close();
            }
            catch (SQLException e)
            {
                logger.warn("could not close statement", e);
            }
        }
        allStatements.clear();
    }

    /**
     * Actually close my underlying {@link java.sql.Connection}.
     */
    public void reallyClose() throws SQLException
    {
        if (statementPool != null)
        {
            statementPool.forAll(new DoUntilProcedure<PooledPreparedStatement>()
            {
                @Override
                public boolean execute(PooledPreparedStatement obj)
                {
                    obj.reallyClose();
                    return false;
                }
            });
        }
        this.state = STATE_DEAD;
        super.close();
    }

    public Statement createStatement() throws SQLException
    {
        return new DelegatingStatement(getUnderlyingConnection().createStatement());
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return addStatement(this.getUnderlyingConnection().createStatement(resultSetType, resultSetConcurrency));
    }

    protected synchronized Statement addStatement(Statement statement)
    {
        Statement delegatingStatement = new DelegatingStatement(statement);
        this.allStatements.add(delegatingStatement);
        return delegatingStatement;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        return addStatement(this.getUnderlyingConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        if (statementPool == null)
        {
            return super.prepareStatement(sql);
        }
        StatementKey key = new StatementKey(sql, getCatalog());
        return getOrCreatePreparedStatement(key);
    }

    private PreparedStatement getOrCreatePreparedStatement(StatementKey key) throws SQLException
    {
        PooledPreparedStatement preparedStatement = this.statementPool.remove(key);
        if (preparedStatement == null)
        {
            preparedStatement = new PooledPreparedStatement(key, super.prepareStatement(key.getSql()), this.statementPool);
        }
        else
        {
            preparedStatement.activate();
        }
        return preparedStatement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return addPreparedStatement(super.prepareStatement(sql, resultSetType, resultSetConcurrency));
    }

    private synchronized PreparedStatement addPreparedStatement(PreparedStatement preparedStatement)
    {
        DelegatingPreparedStatement delegatingPreparedStatement = new DelegatingPreparedStatement(preparedStatement);
        this.allStatements.add(delegatingPreparedStatement);
        return delegatingPreparedStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        return addCallableStatemnet(super.prepareCall(sql));
    }

    private synchronized CallableStatement addCallableStatemnet(CallableStatement callableStatement)
    {
        DelegatingCallableStatement delegatingCallableStatement = new DelegatingCallableStatement(callableStatement);
        this.allStatements.add(delegatingCallableStatement);
        return delegatingCallableStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return this.addCallableStatemnet(super.prepareCall(sql, resultSetType, resultSetConcurrency));
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        return this.addCallableStatemnet(super.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        return this.addPreparedStatement(super.prepareStatement(sql, autoGeneratedKeys));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        return this.addPreparedStatement(super.prepareStatement(sql, columnIndexes));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        return this.addPreparedStatement(super.prepareStatement(sql, columnNames));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        return this.addPreparedStatement(super.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    private static final class StatementDestroyAction implements SynchronizedLruMap.DestroyAction<PooledPreparedStatement>
    {
        @Override
        public void destroy(PooledPreparedStatement object)
        {
            object.reallyClose();
        }
    }

}
