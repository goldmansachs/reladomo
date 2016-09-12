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

import java.sql.Connection;
import java.sql.SQLException;

public class MithraPoolableConnectionFactory implements PoolableObjectFactory<Connection>
{
    protected final ConnectionFactory connectionFactory;
    protected final int statementsToPool;

    public MithraPoolableConnectionFactory(ConnectionFactory connFactory, int statementsToPool)
    {
        connectionFactory = connFactory;
        this.statementsToPool = statementsToPool;
    }

    // overriding to remove synchronized. We never mutate any state that affects this method
    @Override
    public Connection makeObject(ObjectPoolWithThreadAffinity<Connection> pool) throws Exception
    {
        Connection conn = connectionFactory.createConnection();
        return new PooledConnection(conn, pool, this.statementsToPool);
    }

    public void destroyObject(Connection obj) throws Exception
    {
        if (obj instanceof PooledConnection)
        {
            ((PooledConnection) obj).reallyClose();
        }
    }

    public boolean validateObject(Connection obj)
    {
        try
        {
            if (obj.isClosed())
            {
                throw new SQLException("validateConnection: connection closed");
            }
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    public void passivateObject(Connection conn) throws Exception
    {
        if (!conn.getAutoCommit() && !conn.isReadOnly())
        {
            conn.rollback();
        }
        conn.clearWarnings();
        if (!conn.getAutoCommit())
        {
            conn.setAutoCommit(true);
        }
    }

    public void activateObject(Connection conn) throws Exception
    {
        if (conn instanceof PooledConnection)
        {
            ((PooledConnection) conn).activate();
        }
        if (!conn.getAutoCommit())
        {
            conn.setAutoCommit(true);
        }
    }

}