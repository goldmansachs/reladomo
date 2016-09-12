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

package com.gs.fw.common.mithra.portal;

import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.behavior.txparticipation.ReadCacheUpdateCausesRefreshAndLockTxParticipationMode;
import com.gs.fw.common.mithra.util.PersisterId;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.tempobject.MithraTuplePersister;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.behavior.txparticipation.FullTransactionalParticipationMode;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;

import java.util.List;
import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.IOException;
import java.io.ObjectOutput;


public class TupleMithraObjectPortal extends MithraAbstractObjectPortal
{

    private TupleTempContext tupleTempContext;
    private TuplePortalSerializationReplacement tuplePortalSerializationReplacement;

    public TupleMithraObjectPortal(TupleTempContext tupleTempContext)
    {
        super(tupleTempContext.getRelatedFinder());
        setForTempObject(true);
        this.tupleTempContext = tupleTempContext;
    }

    public boolean mapsToUniqueIndex(List attributes)
    {
        return tupleTempContext.mapsToUniqueIndex(attributes); 
    }

    public String getTableNameForQuery(SqlQuery sqlQuery, MapperStackImpl mapperStack, int currentSourceNumber, PersisterId persisterId)
    {
        return this.tupleTempContext.getTableNameForQuery(sqlQuery, mapperStack, currentSourceNumber, persisterId);
    }

    protected CachedQuery findInMemory(AnalyzedOperation op, OrderBy orderby, boolean forRelationship, CachedQuery cachedQuery)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected CachedQuery findInCache(Operation op, AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship)
    {
        throw new RuntimeException("not implemented");
    }

    public void clearTxParticipationMode(MithraTransaction tx)
    {
    }

    public List findForMassDeleteInMemory(Operation op, MithraTransaction tx)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraTuplePersister getMithraTuplePersister()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraObjectPersister getMithraObjectPersister()
    {
        throw new RuntimeException("not implemented");
    }

    public TxParticipationMode getTxParticipationMode()
    {
        return FullTransactionalParticipationMode.getInstance();
    }

    public TxParticipationMode getTxParticipationMode(MithraTransaction tx)
    {
        return ReadCacheUpdateCausesRefreshAndLockTxParticipationMode.getInstance();
    }

    public void prepareForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        throw new RuntimeException("not implemented");
    }

    public void prepareForMassPurge(Operation op, boolean forceImplicitJoin)
    {
        throw new RuntimeException("not implemented");
    }

    public void setDefaultTxParticipationMode(TxParticipationMode mode)
    {
        throw new RuntimeException("not implemented");
    }

    public void setTxParticipationMode(TxParticipationMode txParticipationMode, MithraTransaction tx)
    {
        throw new RuntimeException("not implemented");
    }

    public static class TuplePortalSerializationReplacement implements Externalizable
    {
        private TupleTempContext tempContext;

        public TuplePortalSerializationReplacement()
        {
            // for Externalizable
        }

        private TuplePortalSerializationReplacement(TupleTempContext tempContext)
        {
            this.tempContext = tempContext;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
        {
            this.tempContext = (TupleTempContext) in.readObject();
        }

        public void writeExternal(ObjectOutput out) throws IOException
        {
            out.writeObject(this.tempContext);
        }

        public Object readResolve() throws ClassNotFoundException, IllegalAccessException, InstantiationException
        {
            return this.tempContext.getPortal();
        }
    }
}
