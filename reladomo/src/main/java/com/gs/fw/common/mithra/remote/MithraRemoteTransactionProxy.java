
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

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionException;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionLifeCycleListener;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.notification.MithraNotificationEvent;
import com.gs.fw.common.mithra.portal.MithraTransactionalPortal;
import com.gs.fw.common.mithra.transaction.MithraLocalTransaction;
import com.gs.fw.common.mithra.util.InternalList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class MithraRemoteTransactionProxy extends MithraLocalTransaction
{
    private static final Logger logger = LoggerFactory.getLogger(MithraRemoteTransactionProxy.class.getName());

    private final Set enlistedResources = new UnifiedSet(4);
    private final Set attributes = new UnifiedSet();
    private final Set portals = new UnifiedSet();

    private int proxyStatus = Status.STATUS_ACTIVE;
    private long requestorVmId;
    private boolean cautious;
    private Xid xid;
    private InternalList synchronizations;

    public MithraRemoteTransactionProxy(Xid xid, int timeoutInMilliseconds)
    {
        super(timeoutInMilliseconds);
        this.xid = xid;
        MithraTransactionalPortal.initializeTransactionalQueryCache(this);
    }

    public void setCautious() throws MithraDatabaseException
    {
        this.cautious = true;
    }

    public boolean isCautious()
    {
        return this.cautious;
    }

    public long getRequestorVmId()
    {
        return this.requestorVmId;
    }

    public void setRequestorVmId(long requestorVmId)
    {
        this.requestorVmId = requestorVmId;
    }

    private void delistAll(int flag) throws MithraTransactionException
    {
        try
        {
            for (Iterator iterator = this.enlistedResources.iterator(); iterator.hasNext();)
            {
                XAResource xaResource = (XAResource) iterator.next();
                xaResource.end(this.xid, flag);
            }
        }
        catch (XAException e)
        {
            throw new MithraTransactionException("could not delist resources", e);
        }
    }

    public void enrollAttribute(Attribute attribute)
    {
        this.attributes.add(attribute);
    }

    protected void synchronizedHandleCacheCommit() throws MithraTransactionException
    {
        for (Iterator it = this.portals.iterator(); it.hasNext();)
        {
            MithraObjectPortal portal = (MithraObjectPortal) it.next();
            portal.getPerClassUpdateCountHolder().commitUpdateCount();
        }
        for (Iterator it = this.attributes.iterator(); it.hasNext();)
        {
            Attribute attribute = (Attribute) it.next();
            attribute.commitUpdateCount();
        }
    }

    protected void synchronizedHandleCacheRollback() throws MithraTransactionException
    {
        for (Iterator it = this.portals.iterator(); it.hasNext();)
        {
            MithraObjectPortal portal = (MithraObjectPortal) it.next();
            portal.getPerClassUpdateCountHolder().rollbackUpdateCount();
        }
        for (Iterator it = this.attributes.iterator(); it.hasNext();)
        {
            Attribute attribute = (Attribute) it.next();
            attribute.rollbackUpdateCount();
        }
    }

    public void commit() throws MithraDatabaseException
    {
        throw new RuntimeException("Not implemented");
    }

    public void commit(boolean onePhase) throws MithraDatabaseException
    {
        if (this.proxyStatus != Status.STATUS_ACTIVE)
        {
            throw new MithraTransactionException("Cannot commit rolledback transaction");
        }

        this.delistAll(XAResource.TMSUCCESS);

        try
        {
            this.handleCachePrepare();

            // since we're registered as a synchronization, our afterCompletion method will be called
            // this throws too many exceptions to list
            this.commitAllXaResources(onePhase);

            logger.debug("Committing transaction");
            this.handleCacheCommit();

            this.proxyStatus = Status.STATUS_COMMITTED;
            afterCompletion();
        }
        catch (Exception e)
        {
            throw new MithraTransactionException("Could not commit transaction", e);
        }
    }

    private void commitAllXaResources(boolean onePhase)
    {
        try
        {
            for (Iterator iterator = this.enlistedResources.iterator(); iterator.hasNext();)
            {
                XAResource xaResource = (XAResource) iterator.next();
                xaResource.commit(this.xid, onePhase);
            }
        }
        catch (XAException e)
        {
            throw new MithraTransactionException("Could not delist resources", e);
        }
    }

    private void rollbackAllXaResources()
    {
        try
        {
            for (Iterator iterator = this.enlistedResources.iterator(); iterator.hasNext();)
            {
                XAResource xaResource = (XAResource) iterator.next();
                xaResource.rollback(this.xid);
            }
        }
        catch (XAException e)
        {
            throw new MithraTransactionException("Could not rollback", e);
        }
    }

    protected void synchronizedFinalCleanup()
    {
        this.portals.clear();
        this.attributes.clear();
    }

    private void afterCompletion()
    {
        if (this.synchronizations != null)
        {
            for (int i = 0; i < synchronizations.size(); i++)
            {
                Synchronization s = (Synchronization) synchronizations.get(i);
                s.afterCompletion(this.proxyStatus);
            }
        }

    }

    public void rollback() throws MithraTransactionException
    {
        if (this.proxyStatus == Status.STATUS_ACTIVE)
        {
            this.proxyStatus = Status.STATUS_ROLLING_BACK;
            this.delistAll(XAResource.TMFAIL);
            this.rollbackAllXaResources();
            this.handleCacheRollback();
            this.proxyStatus = Status.STATUS_ROLLEDBACK;
            this.notificationEvents.clear();
            afterCompletion();
        }
        else
        {
            throw new MithraTransactionException("Cannot rollback with proxyStatus " + getJtaTransactionStatusDescription(this.proxyStatus));
        }
    }

    protected void rollbackWithCause(Throwable cause) throws MithraTransactionException
    {
        this.rollback();
    }

    public int getJtaTransactionStatus() throws MithraTransactionException
    {
        return this.proxyStatus;
    }

    public void enrollObject(MithraTransactionalObject obj, Cache cache)
    {
        synchronized (this.txObjects)
        {
            this.txObjects.add(obj);
            this.txCaches.add(cache);
            this.portals.add(obj.zGetPortal());
            this.setWaitingForTransaction(null);
        }
    }

    public void insert(MithraTransactionalObject obj) throws MithraDatabaseException
    {
        obj.zGetPortal().getMithraObjectPersister().insert(obj.zGetTxDataForRead());
        obj.zSetInserted();
    }

    public void update(MithraTransactionalObject obj, AttributeUpdateWrapper attributeUpdateWrapper) throws MithraDatabaseException
    {
        throw new RuntimeException("Not implemented");
    }

    public void delete(MithraTransactionalObject obj) throws MithraDatabaseException
    {
        obj.zGetPortal().getMithraObjectPersister().delete(obj.zGetTxDataForRead());
        obj.zSetDeleted();
    }

    public void purge(MithraTransactionalObject obj) throws MithraDatabaseException
    {
        obj.zGetPortal().getMithraObjectPersister().purge(obj.zGetTxDataForRead());
        obj.zSetDeleted();
    }

    public void deleteUsingOperation(Operation op) throws MithraDatabaseException
    {
        throw new RuntimeException("Not implemented");
    }

    public int deleteBatchUsingOperation(Operation op, int batchSize) throws MithraDatabaseException
    {
        throw new RuntimeException("Not implemented");
    }

    public void purgeUsingOperation(Operation op) throws MithraDatabaseException
    {
        throw new RuntimeException("Not implemented");
    }

    public void addMithraNotificationEvent(String databaseIdentifier, MithraNotificationEvent notificationEvent)
    {
        List notificationEventList = (List) this.notificationEvents.get(databaseIdentifier);
        if(notificationEventList != null)
        {
            notificationEventList.add(notificationEvent);
        }
        else
        {
            notificationEventList = new ArrayList();
            notificationEventList.add(notificationEvent);
            this.notificationEvents.put(databaseIdentifier, notificationEventList);
        }
    }

    public void executeBufferedOperations() throws MithraDatabaseException
    {
    }

    public void executeBufferedOperationsIfMoreThan(int i) throws MithraDatabaseException
    {
    }

    public void executeBufferedOperationsForOperation(Operation op, boolean bypassCache) throws MithraDatabaseException
    {
    }

    public void executeBufferedOperationsForEnroll(MithraObjectPortal mithraObjectPortal)
    {
    }

    public void executeBufferedOperationsForPortal(MithraObjectPortal mithraObjectPortal)
    {
    }

    public void expectRollbackWithCause(Throwable throwable)
    {
    }

    public void addLogicalDeleteForPortal(MithraObjectPortal portal)
    {
    }

    public void enlistResource(XAResource resource) throws SystemException, RollbackException
    {
        this.enlistedResources.add(resource);
        //todo: rezaem: send something about this back to the client side
        //todo: rezaem: call start on the resource
        // resource.start();
    }

    @Override
    public void registerSynchronization(Synchronization synchronization)
    {
        if (this.synchronizations == null)
        {
            this.synchronizations = new InternalList(2);
        }
        this.synchronizations.add(synchronization);
    }

    public void registerLifeCycleListener(TransactionLifeCycleListener lifeCycleListener)
    {
        throw new RuntimeException("not implemented");
    }

    protected Logger getLogger()
    {
        return logger;
    }

    @Override
    protected long getRequesterVmId()
    {
        return this.requestorVmId;
    }
}
