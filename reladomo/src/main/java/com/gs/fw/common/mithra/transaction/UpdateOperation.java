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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;



public class UpdateOperation extends TransactionOperation
{

    private final List<AttributeUpdateWrapper> updates;
    private static final UpdateWrapperComparator UPDATE_WRAPPER_COMPARATOR = new UpdateWrapperComparator();
    private boolean sorted = false;

    public UpdateOperation(MithraTransactionalObject mithraObject, AttributeUpdateWrapper first)
    {
        super(mithraObject, first.getAttribute().getOwnerPortal());
        updates = new FastList(3);
        updates.add(first);
    }

    public UpdateOperation(MithraTransactionalObject mithraObject, List updates)
    {
        super(mithraObject, ((AttributeUpdateWrapper) updates.get(0)).getAttribute().getOwnerPortal());
        this.updates = updates;
    }

    @Override
    public boolean isUpdate()
    {
        return true;
    }

    public void addOperation(AttributeUpdateWrapper attributeUpdateWrapper)
    {
        for(int i=0;i<updates.size();i++)
        {
            AttributeUpdateWrapper wrapper = updates.get(i);
            AttributeUpdateWrapper combined = attributeUpdateWrapper.combineForSameAttribute(wrapper);
            if (combined != null)
            {
                updates.set(i, combined);
                return;
            }
        }
        updates.add(attributeUpdateWrapper);
        sorted = false;
    }

    @Override
    protected boolean isAsOfAttributeToOnlyUpdate()
    {
        for(int i=0;i<updates.size();i++)
        {
            Attribute attribute = updates.get(i).getAttribute();
            if (!(attribute instanceof TimestampAttribute && ((TimestampAttribute)attribute).isAsOfAttributeTo()))
            {
                return false;
            }
        }
        return true;
    }

    public List<AttributeUpdateWrapper> getUpdates()
    {
        return updates;
    }

    protected void sortOperations()
    {
        if (!sorted && this.updates.size() > 1)
        {
            Collections.sort(updates, UPDATE_WRAPPER_COMPARATOR);
            sorted = true;
        }
    }

    @Override
    public void execute() throws MithraDatabaseException
    {
        this.getPortal().getMithraObjectPersister().update(this.getMithraObject(), updates);
        setUpdated();
    }

    @Override
    public TransactionOperation combinePurge(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        if (incomingPortal == this.getPortal() &&
            obj.zIsSameObjectWithoutAsOfAttributes(this.getMithraObject()))
        {
            return new PurgeOperation(obj, this.getPortal());
        }
        return null;
    }

    public void setUpdated()
    {
        this.getMithraObject().zSetUpdated(updates);
    }

    @Override
    public TransactionOperation combineInsertOperation(TransactionOperation op)
    {
        if (op.getMithraObject() == this.getMithraObject())
        {
            return op;
        }
        return null;
    }

    @Override
    protected int getCombineDirectionForParent()
    {
        return COMBINE_DIRECTION_FORWARD;
    }

    @Override
    protected int getCombineDirectionForChild()
    {
        return COMBINE_DIRECTION_FORWARD;
    }

    @Override
    public TransactionOperation combineUpdate(TransactionOperation op)
    {
        if (op instanceof UpdateOperation && this.getPortal() == op.getPortal())
        {
            UpdateOperation incoming = (UpdateOperation) op;
            if (op.getMithraObject() == this.getMithraObject())
            {
                for(int i=0;i<incoming.updates.size();i++)
                {
                    this.addOperation(incoming.updates.get(i));
                }
                return this;
            }
            if (this.canBeBatched(incoming))
            {
                if (this.getMithraObject() == op.getMithraObject()) return op;
                Attribute diffPk = this.canBeMultiUpdated(incoming);
                if (diffPk != null)
                {
                    return new MultiUpdateOperation(this, incoming, diffPk);
                }
                return new BatchUpdateOperation(this, incoming);
            }
        }
        return null;
    }

    @Override
    public TransactionOperation combineBatchUpdate(TransactionOperation op)
    {
        return op.combineUpdate(this);
    }

    @Override
    public TransactionOperation combineMultiUpdate(TransactionOperation op)
    {
        return op.combineUpdate(this);
    }

    public boolean canBeBatched(UpdateOperation otherOperation)
    {
        if (otherOperation.getPortal() == this.getPortal())
        {
            if (this.updates.size() == otherOperation.updates.size())
            {
                if(!this.getMithraObject().zHasSameNullPrimaryKeyAttributes(otherOperation.getMithraObject()))
                {
                    return false;
                }
                this.sortOperations();
                otherOperation.sortOperations();
                for(int i=0;i<this.updates.size();i++)
                {
                    AttributeUpdateWrapper left = updates.get(i);
                    AttributeUpdateWrapper right = otherOperation.updates.get(i);
                    if (left.getClass() != right.getClass() || !left.getAttribute().equals(right.getAttribute())) return false;
                }
                return true;
            }
        }
        return false;
    }

    public static Attribute findDifferentPk(MithraObjectPortal portal, Object first, Object second)
    {
        first = portal.zChooseDataForMultiupdate((MithraTransactionalObject)first);
        second = portal.zChooseDataForMultiupdate((MithraTransactionalObject)second);
        Attribute sourceAttribute = portal.getFinder().getSourceAttribute();
        if (sourceAttribute != null)
        {
            if (!sourceAttribute.valueEquals(first, second))
            {
                return null;
            }
        }
        Attribute[] primaryKeyAttributes = portal.zGetAddressingAttributes();
        Attribute differentPk = primaryKeyAttributes[0];
        if (primaryKeyAttributes.length > 1)
        {
            int count = 0;
            for(int i=0;i<primaryKeyAttributes.length;i++)
            {
                if (!primaryKeyAttributes[i].valueEquals(first, second))
                {
                    differentPk = primaryKeyAttributes[i];
                    count++;
                    if (count > 1)
                    {
                        return null;
                    }
                }
            }
            if (count == 0)
            {
                return null;
            }
        }
        return differentPk;
    }

    private Attribute canBeMultiUpdated(UpdateOperation op)
    {
        if (!this.getPortal().useMultiUpdate()) return null;
        Attribute differentPk = findDifferentPk(op.getPortal(), this.getMithraObject(), op.getMithraObject());
        if (differentPk == null)
        {
            return null;
        }
        for(int i=0;i<this.updates.size();i++)
        {
            AttributeUpdateWrapper left = updates.get(i);
            AttributeUpdateWrapper right = op.updates.get(i);
            if (!left.hasSameParameter(right)) return null;
        }
        return differentPk;
    }

    public int setSqlParameters(PreparedStatement stm, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        int pos = 1;
        for (int i = 0; i < updates.size(); i++)
        {
            AttributeUpdateWrapper wrapper = updates.get(i);
            pos += wrapper.setSqlParameters(stm, pos, databaseTimeZone, databaseType);
        }
        return pos;
    }

    private static class UpdateWrapperComparator implements Comparator<AttributeUpdateWrapper>
    {
        @Override
        public int compare(AttributeUpdateWrapper left, AttributeUpdateWrapper right)
        {
            return System.identityHashCode(left.getAttribute()) - System.identityHashCode(right.getAttribute());
        }
    }
}
