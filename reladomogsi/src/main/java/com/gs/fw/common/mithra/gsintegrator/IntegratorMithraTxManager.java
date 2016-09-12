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

package com.gs.fw.common.mithra.gsintegrator;

import com.gs.fw.aig.intgr.IntgrException;
import com.gs.fw.aig.intgr.bus.ITxManager;
import com.gs.fw.aig.intgr.store.SafeStoreDatum;
import com.gs.fw.common.mithra.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class IntegratorMithraTxManager implements ITxManager
{
    static private Logger logger = LoggerFactory.getLogger(IntegratorMithraTxManager.class.getName());

    private Connection commitAgent;
    private IntegratorMithraPublisher publisher;
    private List messagesInTransaction = new ArrayList();
    private static final int MAX_RETRY_COUNT = 10;
    private int retryCount = MAX_RETRY_COUNT;
    private PublisherPlugin plugin;

    public IntegratorMithraTxManager(Connection commitAgent, IntegratorMithraPublisher publisher)
    {
        this.commitAgent = commitAgent;
        this.publisher = publisher;
    }

    public void begin() throws IntgrException
    {
        this.messagesInTransaction.clear();
        this.retryCount = MAX_RETRY_COUNT;
        try
        {
            commitAgent.setAutoCommit( false );
        }
        catch ( SQLException sqle )
        {
            throw new IntgrException( "failed disabling auto-commit policy on commit agent connection" );
        }
        startMithraTransaction();
        plugin.beginBatch();
    }

    private void startMithraTransaction()
            throws IntgrException
    {
        try
        {
            MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        }
        catch(MithraException e)
        {
            this.logger.error("could not start transaction", e);
            throw new IntgrException("could not start transaction", e);
        }
    }

    public void commit() throws IntgrException
    {
        plugin.endBatch();
        commitMithraTransaction();
        try
        {
            commitAgent.commit();
        }
        catch ( SQLException sqle )
        {
            throw new IntgrException( "commit failed on commit agent", sqle );
        }

        /*
            manage auto-commit policy
        */
        try
        {
            commitAgent.setAutoCommit( true );
        }
        catch ( SQLException sqle )
        {
            throw new IntgrException( "failed enabling auto-commit policy on commit agent connection" );
        }
    }

    private void commitMithraTransaction()
            throws IntgrException
    {
        try
        {
            MithraManagerProvider.getMithraManager().getCurrentTransaction().commit();
            this.messagesInTransaction.clear();
        }
        catch (MithraDatabaseException e)
        {
            this.retryIfWarranted(e, true);
        }
    }

    public void rollback() throws IntgrException
    {
        try
        {
            /*
                opposite order of commit
             */
            commitAgent.rollback();
        }
        catch ( SQLException sqle )
        {
            throw new IntgrException( "rollback failed on remote database", sqle );
        }
        finally
        {
            MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            if (currentTransaction != null)
            {
                currentTransaction.rollback();
            }
        }
    }

    public void addMessage(SafeStoreDatum datum)
    {
        this.messagesInTransaction.add(datum);
    }

    public void retryIfWarranted(MithraBusinessException e, boolean commit) throws IntgrException
    {
        boolean retried = false;
        if (e.isRetriable())
        {
            retryCount--;
            if (retryCount > 0)
            {
                this.logger.warn("retring due to exception ", e);
                MithraManagerProvider.getMithraManager().getCurrentTransaction().rollback();
                startMithraTransaction();
                this.plugin.beginBatch();
                List resend = this.messagesInTransaction;
                this.messagesInTransaction = new ArrayList(resend.size());
                for(int i=0;i<resend.size();i++)
                {
                    SafeStoreDatum datum = (SafeStoreDatum) resend.get(i);
                    this.publisher.send(datum);
                }
                if (commit)
                {
                    this.plugin.endBatch();
                    commitMithraTransaction();
                }
                retried = true;
            }
        }
        if (!retried)
        {
            String message = commit ? "Could not commit transaction" : "Could not send message";
            if (e.isRetriable()) message += " No more retries left";
            this.logger.error(message, e);
            throw new IntgrException(message, e);
        }
    }

    public void setPlugin(PublisherPlugin plugin)
    {
        this.plugin = plugin;
    }
}
