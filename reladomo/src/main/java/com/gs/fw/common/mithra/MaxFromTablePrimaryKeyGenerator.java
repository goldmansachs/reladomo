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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.ResultSetParser;
import com.gs.fw.common.mithra.finder.longop.LongResultSetParser;
import com.gs.fw.common.mithra.transaction.MithraTransactionalResource;
import com.gs.fw.common.mithra.transaction.TransactionLocal;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class MaxFromTablePrimaryKeyGenerator
{
    private final Attribute primaryKeyAttribute;
    private final Object sourceAttribute;
    private TransactionLocal txLocal = new TransactionLocal();

    public MaxFromTablePrimaryKeyGenerator(Attribute primaryKeyAttribute, Object sourceAttribute)
    {
        this.primaryKeyAttribute = primaryKeyAttribute;
        this.sourceAttribute = sourceAttribute;
    }

    public long getNextId()
    {
        MithraTransaction threadTx = MithraManagerProvider.getMithraManager().getCurrentTransaction();

        if (threadTx == null)
        {
            throw new MithraTransactionException("Max id generator for "+primaryKeyAttribute.getClass().getName()+" must only be used inside a transaction");
        }
        AtomicLong currentId = (AtomicLong) txLocal.get(threadTx);
        if (currentId == null)
        {
            currentId = new AtomicLong(this.getMaxIdForPrimaryKey(primaryKeyAttribute, sourceAttribute));
            txLocal.set(threadTx, currentId);
        }
        return currentId.incrementAndGet();
    }

    public long getMaxIdForPrimaryKey(Attribute primaryKeyAttribute, Object sourceAttribute)
    {
        SourceAttributeType sourceAttributeType = primaryKeyAttribute.getSourceAttributeType();
        MithraObjectPortal ownerPortal = primaryKeyAttribute.getOwnerPortal();
        RelatedFinder finder = ownerPortal.getFinder();

        Operation op = finder.all();

        if(sourceAttribute != null)
        {
            if (sourceAttributeType.isStringSourceAttribute())
            {
                op = op.and(((StringAttribute)primaryKeyAttribute.getSourceAttribute()).eq((String)sourceAttribute));
            }
            else if(sourceAttributeType.isIntSourceAttribute())
            {
                op =  op.and(((IntegerAttribute)primaryKeyAttribute.getSourceAttribute()).eq(((Integer)sourceAttribute).intValue()));
            }
        }

        AsOfAttribute[] asOfAttributes = primaryKeyAttribute.getAsOfAttributes();
        if (asOfAttributes != null)
        {
            for(int i=0;i<asOfAttributes.length;i++)
            {
                op = op.and(asOfAttributes[i].equalsEdgePoint());
            }
        }
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("max", primaryKeyAttribute.max());
        if (!aggregateList.isEmpty())
        {
            AggregateData aggregateData = aggregateList.get(0);
            if (aggregateData.isAttributeNull("max")) return 0;
            return aggregateData.getAttributeAsLong("max");
        }
        return 0;
    }
}
