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


import com.gs.fw.common.mithra.util.SynchronizedLruMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PooledPreparedStatement extends DelegatingPreparedStatement
{
    private static Logger logger = LoggerFactory.getLogger(PooledPreparedStatement.class.getName());
    private final StatementKey key;
    private final SynchronizedLruMap<StatementKey, PooledPreparedStatement> statementPool;
    private boolean isActive;

    public PooledPreparedStatement(StatementKey key, PreparedStatement delegate, SynchronizedLruMap<StatementKey, PooledPreparedStatement> statementPool)
    {
        super(delegate);
        this.key = key;
        this.statementPool = statementPool;
        this.isActive = true;
    }

    @Override
    public void close() throws SQLException
    {
        if (isActive)
        {
            this.clearParameters();
            statementPool.put(this.key, this);
        }
        this.isActive = false;
    }

    public void activate()
    {
        this.isActive = true;
    }

    public void reallyClose()
    {
        try
        {
            super.close();
        }
        catch (SQLException e)
        {
            logger.warn("could not close statement", e);
        }
    }
}
