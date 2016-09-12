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

package com.gs.fw.common.mithra.tempobject;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.collections.impl.map.mutable.UnifiedMap;


public class MithraTempContextWithSourceAttribute extends AbstractMithraTemporaryContext
{

    private UnifiedMap existingContexts = new UnifiedMap();

    public MithraTempContextWithSourceAttribute(TempContextContainer container)
    {
        super(container);
    }

    protected void createTable()
    {
        //nothing to do, we'll do this when the source attribute gets added
    }

    public void associateToCurrentThread()
    {
        if (this.isSingleThreaded())
        {
            throw new RuntimeException("The database type is configured for un-shared table usage and cannot be used from multiple threads");
        }
        this.getContainer().associateToCurrentThread(this);
    }

    public void destroy()
    {
        throw new RuntimeException("should never get here");
    }


    public TemporaryContext addSourceAttribute(Object sourceAttribute)
    {
        if (existingContexts.containsKey(sourceAttribute))
        {
            throw new MithraBusinessException("Temporary context for "+getContainer().getClassName()+" already exists for source "+sourceAttribute);
        }
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        MithraTemporaryContext mtc = null;
        if (tx == null)
        {
            mtc = new MithraNonTxTempContext(this.getContainer(), sourceAttribute);
            mtc.init(this.getMithraObjectPortal(), this.getDbObject());
        }
        else
        {
            mtc = new MithraTxTempContext(this.getContainer(), tx, sourceAttribute);
            mtc.init(this.getMithraObjectPortal(), this.getDbObject());
        }
        existingContexts.put(sourceAttribute, mtc);
        return mtc;
    }

    protected void remove(final Object sourceAttributeValue, MithraTransaction tx)
    {
        MithraObjectPortal portal = this.getMithraObjectPortal();
        this.existingContexts.remove(sourceAttributeValue);
        if (existingContexts.isEmpty())
        {
            this.getContainer().clearCurrentContext();
            portal.incrementClassUpdateCount();
            if (tx != null)
            {
                this.getContainer().clearTxContext(tx);
            }
            this.setDestroyed(true);
        }
        else
        {
            final Attribute sourceAttribute = portal.getFinder().getSourceAttribute();
            boolean operationEvalMode = false;
            if (tx != null)
            {
                operationEvalMode = tx.zIsInOperationEvaluationMode();
                tx.zSetOperationEvaluationMode(true);
            }
            try
            {
                portal.getCache().removeAll(new Filter()
                {
                    public boolean matches(Object o)
                    {
                        return sourceAttribute.valueOf(o).equals(sourceAttributeValue);
                    }
                });
                portal.incrementClassUpdateCount();
            }
            finally
            {
                if (tx != null) tx.zSetOperationEvaluationMode(operationEvalMode);
            }
        }
    }

    private class MithraNonTxTempContext extends AbstractMithraTemporaryContext implements TransactionLifeCycleListener
    {
        private Object sourceAttribute;

        public MithraNonTxTempContext(TempContextContainer container, Object sourceAttribute)
        {
            super(container);
            this.sourceAttribute = sourceAttribute;
        }

        public void destroy()
        {
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            if (tx == null)
            {
                this.getDbObject().dropTempTable(sourceAttribute);
            }
            else
            {
                tx.registerLifeCycleListener(this);
            }
            this.setDestroyed(true);
            remove(sourceAttribute, null);
        }

        public void associateToCurrentThread()
        {
            MithraTempContextWithSourceAttribute.this.associateToCurrentThread();
        }

        protected void createTable()
        {
            this.getDbObject().createSharedTempTable(sourceAttribute, this);
        }

        public void beforeCommit()
        {
            this.getDbObject().dropTempTable(sourceAttribute);
        }

        public void beforeRollback()
        {
            this.getDbObject().dropTempTable(sourceAttribute);
        }
    }

    private class MithraTxTempContext extends AbstractMithraTemporaryContext implements TransactionLifeCycleListener
    {
        private Object sourceAttribute;
        private MithraTransaction tx;

        public MithraTxTempContext(TempContextContainer container, MithraTransaction tx, Object sourceAttribute)
        {
            super(container);
            this.sourceAttribute = sourceAttribute;
            tx.registerLifeCycleListener(this);
            this.tx = tx;
            container.associateSourceToTransaction(MithraTempContextWithSourceAttribute.this, tx);
        }

        public void destroy()
        {
            try
            {
                tx.executeBufferedOperations();
            }
            finally
            {
                this.getDbObject().dropTempTable(sourceAttribute);
                remove(this.sourceAttribute, this.tx);
                this.setDestroyed(true);
            }
        }

        public void associateToCurrentThread()
        {
            throw new MithraBusinessException("Temporary objects in a transactional context are bound to the transaction, not the thread");
        }

        protected void createTable()
        {
            this.getDbObject().createNonSharedTempTable(sourceAttribute);
        }

        public void beforeCommit()
        {
            if (!this.isDestroyed()) this.destroy();
        }

        public void beforeRollback()
        {
            if (!this.isDestroyed()) this.destroy();
        }
    }
}
