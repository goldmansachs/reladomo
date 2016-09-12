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

package com.gs.fw.common.mithra.remote;

import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.VersionAttribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;



public abstract class MithraRemoteResult implements Externalizable, Runnable
{
    static private Logger logger = LoggerFactory.getLogger(MithraRemoteResult.class.getName());

    private RemoteTransactionId remoteTransactionId;
    private transient Throwable thrown; // only used on the server side

    public MithraRemoteResult()
    {
        // for externalizable
    }

    public Throwable getThrown()
    {
        return thrown;
    }

    public void setThrown(Throwable thrown)
    {
        this.thrown = thrown;
    }

    public RemoteTransactionId getRemoteTransactionId()
    {
        return remoteTransactionId;
    }

    public void setRemoteTransactionId(RemoteTransactionId remoteTransactionId)
    {
        this.remoteTransactionId = remoteTransactionId;
    }

    public void readRemoteTransactionId(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.remoteTransactionId = (RemoteTransactionId) in.readObject();
    }

    public void writeRemoteTransactionId(ObjectOutput out) throws IOException
    {
        out.writeObject(this.remoteTransactionId);
    }

    protected RelatedFinder instantiateRelatedFinder(String finderClassname)
    {
        RelatedFinder finderClass = null;
        try
        {
            finderClass = (RelatedFinder) Class.forName(finderClassname).newInstance();
        }
        catch (InstantiationException e)
        {
            throw new RuntimeException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
        return finderClass;
    }

    protected MithraRemoteOptimisticLockException createOrAddDirtyData(MithraRemoteOptimisticLockException optimException,
            MithraDataObject data)
    {
        if (optimException == null)
        {
            optimException = new MithraRemoteOptimisticLockException("Optimistic Lock Failure");
            optimException.addDirtyData(data);
        }
        return optimException;
    }

    protected MithraRemoteOptimisticLockException checkDatedOptimisticLocking(boolean isOptimisticLocking,
            AsOfAttribute[] asOfAttributes, MithraRemoteOptimisticLockException optimException,
            InTransactionDatedTransactionalObject txObject, MithraDataObject data)
    {
        if (isOptimisticLocking)
        {
            if (!asOfAttributes[asOfAttributes.length - 1].getFromAttribute().valueEquals(data, txObject.zGetTxDataForRead()))
            {
                optimException = createOrAddDirtyData(optimException, data);
            }
        }
        return optimException;
    }

    protected MithraRemoteOptimisticLockException checkOptimisticLocking(boolean isOptimistic,
            VersionAttribute versionAttribute,
            MithraRemoteOptimisticLockException optimException, MithraDataObject data, MithraTransactionalObject mithraObject)
    {
        if (isOptimistic)
        {
            if (!versionAttribute.hasSameVersion(data, mithraObject.zGetTxDataForRead()))
            {
                if (optimException == null)
                {
                    optimException = new MithraRemoteOptimisticLockException("Optimistic lock failure");
                }
                optimException.addDirtyData(data);
            }
        }
        return optimException;
    }

    protected void writeDatabaseIdentifierMap(ObjectOutput out, Map dbIdentifierMap)
            throws IOException
    {
        Set databaseIdentifierKeySet = dbIdentifierMap.keySet();

        out.writeInt(databaseIdentifierKeySet.size());
        for(Iterator it = databaseIdentifierKeySet.iterator(); it.hasNext();)
        {
            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key = (MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey) it.next();
            String classname = key.getFinder().getClass().getName();
            Object sourceAttributeValue = key.getSourceAttributeValue();
            String databaseIdentifier = (String) dbIdentifierMap.get(key);
            out.writeObject(classname);
            out.writeObject(sourceAttributeValue);
            out.writeObject(databaseIdentifier);
        }
    }

    protected Map readDatabaseIdentifierMap(ObjectInput in)
            throws IOException, ClassNotFoundException
    {
        int dbIdSize = in.readInt();
        Map dbIdentifierMap = new UnifiedMap(dbIdSize);
        for(int i = 0; i < dbIdSize; i++)
        {
            String finderClassname = (String)in.readObject();
            RelatedFinder finderClass = null;
            try
            {
                finderClass = (RelatedFinder) Class.forName(finderClassname).newInstance();
            }
            catch (InstantiationException e)
            {
                throw new RuntimeException();
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException();
            }
            Object sourceAttributeValue = in.readObject();
            String databaseIdentifier = (String) in.readObject();

            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key =
                    new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey(sourceAttributeValue, finderClass);
            dbIdentifierMap.put(key, databaseIdentifier);
        }
        return dbIdentifierMap;
    }

    protected void registerForNotification(Map databaseIdentifierMap)
    {
        Set keySet = databaseIdentifierMap.keySet();
        RelatedFinder finder = null;
        for(Iterator it = keySet.iterator(); it.hasNext();)
        {
            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key =
                    (MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey)it.next();

            finder = key.getFinder();
            MithraObjectPortal portal = finder.getMithraObjectPortal();
            portal.registerForNotification((String)databaseIdentifierMap.get(key));
        }
    }
}
