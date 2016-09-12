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

package com.gs.fw.common.mithra.transaction;

import com.gs.fw.common.mithra.list.DelegatingList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.notification.MithraNotificationEvent;

import javax.transaction.xa.XAResource;
import javax.transaction.SystemException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.Status;


public class MithraTempTransaction extends MithraLocalTransaction
{

    private static final Logger logger = LoggerFactory.getLogger(MithraTempTransaction.class.getName());

    private MithraTransactionalObject enrolledObject;

    public MithraTempTransaction(MithraTransactionalObject toEnroll)
    {
        super(MithraManagerProvider.getMithraManager().getTransactionTimeout());
        this.enrolledObject = toEnroll;
    }

    @Override
    protected Logger getLogger()
    {
        return logger;
    }

    @Override
    protected long getRequesterVmId()
    {
        return MithraManager.getInstance().getNotificationEventManager().getMithraVmId();
    }

    @Override
    protected void synchronizedHandleCacheRollback() throws MithraTransactionException
    {
        // if something hangs or dies, this will clear the object state
        enrolledObject.zHandleRollback(this);
    }

    @Override
    public void addLogicalDeleteForPortal(MithraObjectPortal portal)
    {
    }

    @Override
    public void addMithraNotificationEvent(String databaseIdentifier, MithraNotificationEvent notificationEvent)
    {
    }

    @Override
    public void commit() throws MithraDatabaseException
    {
    }

    @Override
    public void delete(MithraTransactionalObject obj) throws MithraDatabaseException
    {
    }

    @Override
    public int deleteBatchUsingOperation(Operation op, int batchSize) throws MithraDatabaseException
    {
        return 0;
    }

    @Override
    public void deleteUsingOperation(Operation op) throws MithraDatabaseException
    {
    }

    @Override
    public void enlistResource(XAResource resource) throws SystemException, RollbackException
    {
    }

    @Override
    public void enrollObject(MithraTransactionalObject obj, Cache cache)
    {
    }

    @Override
    public void executeBufferedOperations() throws MithraDatabaseException
    {
    }

    @Override
    public void executeBufferedOperationsForEnroll(MithraObjectPortal mithraObjectPortal)
    {
    }

    @Override
    public void executeBufferedOperationsForOperation(Operation op, boolean bypassCache) throws MithraDatabaseException
    {
    }

    @Override
    public void executeBufferedOperationsForPortal(MithraObjectPortal mithraObjectPortal)
    {
    }

    @Override
    public void executeBufferedOperationsIfMoreThan(int i) throws MithraDatabaseException
    {
    }

    @Override
    public void expectRollbackWithCause(Throwable throwable)
    {
    }

    @Override
    public void enrollReadLocked(MithraTransactionalObject mithraTransactionalObject)
    {
        //nothing to do
    }

    @Override
    public int getJtaTransactionStatus() throws MithraTransactionException
    {
        if (System.currentTimeMillis() - this.getRealStartTime() > this.getTimeoutInMilliseconds())
        {
            return Status.STATUS_NO_TRANSACTION;
        }
        return Status.STATUS_ACTIVE;
    }

    @Override
    public void insert(MithraTransactionalObject obj) throws MithraDatabaseException
    {
    }

    @Override
    public boolean isCautious()
    {
        return false;
    }

    @Override
    public void purge(MithraTransactionalObject obj) throws MithraDatabaseException
    {
    }

    @Override
    public void purgeUsingOperation(Operation op) throws MithraDatabaseException
    {
    }

    @Override
    public void registerLifeCycleListener(TransactionLifeCycleListener lifeCycleListener)
    {
    }

    @Override
    public void registerSynchronization(Synchronization synchronization)
    {
    }

    @Override
    public void rollback() throws MithraTransactionException
    {
    }

    @Override
    protected void rollbackWithCause(Throwable cause) throws MithraTransactionException
    {
    }

    @Override
    public void setCautious() throws MithraDatabaseException
    {
    }

    @Override
    public void update(MithraTransactionalObject obj, AttributeUpdateWrapper attributeUpdateWrapper) throws MithraDatabaseException
    {
    }
}
