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
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.MithraFastList;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class MultiUpdateOperation extends TransactionOperation
{
    private final List<AttributeUpdateWrapper> updates;

    private final List mithraObjects;
    private final boolean isDated;

    private List singleValuedPrimaryKeys;
    private final VersionAttribute versionAttribute;
    private MithraDataObject[] dataObjects;
    private int startIndex = 0;
    private int endIndex = 0;
    private String firstPartSql;
    private FullUniqueIndex index;
    private final Attribute diffPk;

    public MultiUpdateOperation(UpdateOperation first, UpdateOperation second, Attribute diffPk)
    {
        super(first.getMithraObject(), first.getPortal());
        this.diffPk = diffPk;
        mithraObjects = new FastList();
        addObject(first);
        addObject(second);
        this.updates = first.getUpdates();
        this.isDated = this.getPortal().getFinder().getAsOfAttributes() != null;
        if (this.getPortal().getTxParticipationMode().isOptimisticLocking())
        {
            this.versionAttribute = first.getPortal().getFinder().getVersionAttribute();
        }
        else
        {
            this.versionAttribute = null;
        }
    }

    public MultiUpdateOperation(List updates, List mithraObjects)
    {
        this(updates, mithraObjects, findDiffPk(mithraObjects, ((AttributeUpdateWrapper) updates.get(0)).getAttribute().getOwnerPortal()));
    }

    private static Attribute findDiffPk(List mithraObjects, MithraObjectPortal ownerPortal)
    {
        return UpdateOperation.findDifferentPk(ownerPortal, mithraObjects.get(0), mithraObjects.get(1));
    }

    protected MultiUpdateOperation(List updates, List mithraObjects, Attribute diffPk)
    {
        super((MithraTransactionalObject) mithraObjects.get(0), ((AttributeUpdateWrapper) updates.get(0)).getAttribute().getOwnerPortal());
        this.diffPk = diffPk;
        isDated = this.getPortal().getFinder().getAsOfAttributes() != null;
        this.updates = updates;
        this.mithraObjects = mithraObjects;
        if (this.getPortal().getTxParticipationMode().isOptimisticLocking())
        {
            this.versionAttribute = this.getPortal().getFinder().getVersionAttribute();
        }
        else
        {
            this.versionAttribute = null;
        }
    }

    private void addObject(UpdateOperation updateOperation)
    {
        mithraObjects.add(updateOperation.getMithraObject());
        if (index != null)
        {
            index.put(updateOperation.getMithraObject());
        }
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

    @Override
    public FullUniqueIndex getIndexedObjects()
    {
        if (index == null)
        {
            index = createFullUniqueIndex(mithraObjects);
        }
        return index;
    }

    @Override
    public int getTotalOperationsSize()
    {
        return mithraObjects.size();
    }

    public List getMithraObjects()
    {
        return mithraObjects;
    }

    public List<AttributeUpdateWrapper> getUpdates()
    {
        return updates;
    }

    @Override
    public void execute() throws MithraDatabaseException
    {
        this.getPortal().getMithraObjectPersister().multiUpdate(this);
        setUpdated();
    }

    public void setUpdated()
    {
        for(int i=0;i<mithraObjects.size();i++)
        {
            MithraTransactionalObject obj = (MithraTransactionalObject) mithraObjects.get(i);
            obj.zSetUpdated(updates);
        }
    }

    @Override
    public TransactionOperation combineUpdate(TransactionOperation op)
    {
        if (op instanceof UpdateOperation)
        {
            UpdateOperation incoming = (UpdateOperation) op;
            if (this.canBeCombined(incoming))
            {
                addObject(incoming);
                return this;
            }
        }
        return null;
    }

    @Override
    public List getAllObjects()
    {
        return this.mithraObjects;
    }

    @Override
    public boolean isMultiUpdate()
    {
        return true;
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
    public TransactionOperation combineMultiUpdate(TransactionOperation op)
    {
        if (this.getPortal() == op.getPortal())
        {
            if (!this.getMithraObject().zHasSameNullPrimaryKeyAttributes(op.getMithraObject()))
            {
                return null;
            }
            MultiUpdateOperation other = (MultiUpdateOperation) op;
            MultiUpdateOperation combined = combineOnSameAttribute(other);
            return combined == null ? combineOnSameObjects(other) : combined;
        }
        return null;
    }

    private TransactionOperation combineOnSameObjects(MultiUpdateOperation other)
    {
        if (other.mithraObjects.size() == this.mithraObjects.size())
        {
            FullUniqueIndex thisIndex = this.getIndexedObjects();
            for (int i = 0; i < other.mithraObjects.size(); i++)
            {
                if (!thisIndex.contains(other.mithraObjects.get(i)))
                {
                    return null;
                }
            }

            int updateSize = this.updates.size();
            for (int i = 0; i < updateSize; i++)
            {
                AttributeUpdateWrapper thisUpdate = this.updates.get(i);

                for (int j = 0; j < other.updates.size(); j++)
                {
                    AttributeUpdateWrapper otherUpdate = other.updates.get(j);
                    if (otherUpdate.getAttribute().equals(thisUpdate.getAttribute()))
                    {
                        this.updates.set(i, otherUpdate.combineForSameAttribute(thisUpdate));
                        other.updates.remove(j);
                        j--;
                    }
                }

            }

            this.updates.addAll(other.updates);

            return this;
        }
        return null;
    }

    private MultiUpdateOperation combineOnSameAttribute(MultiUpdateOperation other)
    {
        if (this.diffPk != other.diffPk)
        {
            return null;
        }
        if (diffPk != UpdateOperation.findDifferentPk(this.getPortal(), this.getMithraObject(), other.getMithraObject()))
        {
            return null;
        }
        if (this.updates.size() == other.updates.size())
        {
            for (int i = 0; i < this.updates.size(); i++)
            {
                AttributeUpdateWrapper left = updates.get(i);
                AttributeUpdateWrapper right = other.getUpdates().get(i);
                if (!left.getAttribute().equals(right.getAttribute()))
                {
                    return null;
                }
                if (!left.hasSameParameter(right))
                {
                    return null;
                }
                AttributeUpdateWrapper newWrapper = left.combineForSameAttribute(right);
                if (newWrapper == null)
                {
                    return null;
                }
                updates.set(i, newWrapper);
            }
            this.mithraObjects.addAll(other.mithraObjects);
            if (index != null)
            {
                index.addAll(other.mithraObjects);
            }
            return this;
        }
        return null;
    }

    public boolean canBeCombined(UpdateOperation otherOperation)
    {
        if (otherOperation.getPortal() == this.getPortal())
        {
            if (this.updates.size() == otherOperation.getUpdates().size())
            {
                if(!this.getMithraObject().zHasSameNullPrimaryKeyAttributes(otherOperation.getMithraObject()))
                {
                    return false;
                }
                otherOperation.sortOperations();
                for(int i=0;i<this.updates.size();i++)
                {
                    AttributeUpdateWrapper left = updates.get(i);
                    AttributeUpdateWrapper right = otherOperation.getUpdates().get(i);
                    if (!left.getAttribute().equals(right.getAttribute())) return false;
                    if (!left.hasSameParameter(right)) return false;
                    if (!right.canBeMultiUpdated(this, otherOperation.getMithraObject())) return false;
                }
                return this.diffPk == UpdateOperation.findDifferentPk(this.getPortal(), this.getMithraObject(), otherOperation.getMithraObject());
            }
        }
        return false;
    }

    public void prepareForSqlGeneration(String fullyQualifiedTableName, DatabaseType databaseType)
    {
        MithraTransactionalObject obj = this.getMithraObject();
        RelatedFinder finder = this.getPortal().getFinder();
        this.dataObjects = getDataObjects(isDated);
        findAllPkAttributes(finder, obj, dataObjects);
        createFirstPartSql(fullyQualifiedTableName);
    }

    private void findAllPkAttributes(RelatedFinder finder, MithraTransactionalObject obj, MithraDataObject[] data)
    {
        Attribute[] primaryKeyAttributes = finder.getPrimaryKeyAttributes();
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        this.singleValuedPrimaryKeys = new MithraFastList(primaryKeyAttributes.length + 3);
        for(int i=0;i < primaryKeyAttributes.length; i++)
        {
            if (!primaryKeyAttributes[i].isSourceAttribute())
            {
                singleValuedPrimaryKeys.add(primaryKeyAttributes[i]);
            }
        }
        if (asOfAttributes != null)
        {
            for(int i=0;i < asOfAttributes.length; i++)
            {
                singleValuedPrimaryKeys.add(asOfAttributes[i].getToAttribute());
            }
        }
        if (obj.zGetPortal().getTxParticipationMode().isOptimisticLocking())
        {
            if (asOfAttributes != null)
            {
                Attribute optimisticAttribute = getOptimisticKey(data);
                if (optimisticAttribute != null) singleValuedPrimaryKeys.add(optimisticAttribute);
            }
        }
        for(int i=0;i<singleValuedPrimaryKeys.size();i++)
        {
            if (diffPk == singleValuedPrimaryKeys.get(i))
            {
                singleValuedPrimaryKeys.remove(i);
                break;
            }
        }
    }

    private Attribute getOptimisticKey(MithraDataObject[] data)
    {
        AsOfAttribute businessDate = getPortal().getClassMetaData().getBusinessDateAttribute();
        AsOfAttribute processingDate = getPortal().getClassMetaData().getProcessingDateAttribute();
        if (processingDate != null)
        {
            boolean mustAddProcessingDate = true;
            if (businessDate != null)
            {
                if (MithraManagerProvider.getMithraManager().getCurrentTransaction().retryOnOptimisticLockFailure())
                {
                    long infinityTime = businessDate.getInfinityDate().getTime();
                    int count = 0;
                    for(;count<data.length;count++)
                    {
                        if (businessDate.getToAttribute().timestampValueOfAsLong(data[count]) != infinityTime)
                        {
                            break;
                        }
                    }
                    mustAddProcessingDate = count < data.length;
                }
            }
            if (mustAddProcessingDate)
            {
                return processingDate.getFromAttribute();
            }
        }
        return null;
    }

    private void createFirstPartSql(String fullyQualifiedTableName)
    {
        StringBuffer buf = createFirstPartSingleKeySql(fullyQualifiedTableName);
        this.firstPartSql = buf.toString();
    }

    private StringBuffer createFirstPartSingleKeySql(String fullyQualifiedTableName)
    {
        StringBuffer buf = new StringBuffer(30 + this.updates.size()*20 + singleValuedPrimaryKeys.size() * 20);
        buf.append("update ");
        buf.append(fullyQualifiedTableName);
        buf.append(" set ");
        for(int i=0;i<updates.size();i++)
        {
            AttributeUpdateWrapper upd = updates.get(i);
            if (i > 0) buf.append(", ");
            buf.append(upd.getSetAttributeSql());
        }
        buf.append(" where ");
        if (this.versionAttribute != null)
        {
            buf.append((this.versionAttribute).getColumnName());
            buf.append(" = ? ");
            buf.append(" and ");
        }
        for(int i=0;i<this.singleValuedPrimaryKeys.size();)
        {
            Attribute attr = (Attribute) singleValuedPrimaryKeys.get(i);
            buf.append(attr.getColumnName());
            if (attr.isAttributeNull(this.dataObjects[0]))
            {
                buf.append(" IS NULL ");
                this.singleValuedPrimaryKeys.remove(i);
            }
            else
            {
                buf.append(" = ? ");
                i++;
            }
            buf.append(" and ");
        }
        return buf;
    }

    private MithraDataObject[] getDataObjects(boolean dated)
    {
        MithraDataObject[] dataObjects = new MithraDataObject[this.mithraObjects.size()];
        if (dated)
        {
            for(int i=0;i<this.mithraObjects.size();i++)
            {
                dataObjects[i] = ((MithraTransactionalObject)this.mithraObjects.get(i)).zGetCurrentData();
            }
        }
        else
        {
            for(int i=0;i<this.mithraObjects.size();i++)
            {
                dataObjects[i] = ((MithraTransactionalObject)this.mithraObjects.get(i)).zGetTxDataForRead();
            }
        }
        return dataObjects;
    }

    public String getNextMultiUpdateSql(int maxParams)
    {
        if (this.endIndex == dataObjects.length)
        {
            return null; //we're done
        }
        int availableParams = maxParams - this.updates.size() + this.singleValuedPrimaryKeys.size();
        if (availableParams <= 0)
        {
            availableParams = 10;
        }
        int todo = dataObjects.length - this.endIndex;
        int batchesLeft = todo / availableParams;
        if (batchesLeft * availableParams < todo)
        {
            batchesLeft++;
        }
        int batchSize = todo / batchesLeft;
        if (this.endIndex + batchSize > dataObjects.length)
        {
            batchSize = todo;
        }
        if (this.endIndex + batchSize == dataObjects.length - 1)
        {
            batchSize++;
        }
        String sql = this.firstPartSql + diffPk.getColumnName()+
                " in (";
        sql += createQuestionMarks(batchSize) + ")";
        this.startIndex = this.endIndex;
        this.endIndex += batchSize;

        return sql;
    }

    protected String createQuestionMarks(int numberOfQuestions)
    {
        int questionLength = ((numberOfQuestions - 1) * 2) + 1;
        StringBuilder bunchOfQuestionMarks = new StringBuilder(questionLength);
        bunchOfQuestionMarks.append('?');
        for (int k = 1; k < numberOfQuestions; k++)
        {
            bunchOfQuestionMarks.append(",?");
        }
        return bunchOfQuestionMarks.toString();
    }

    public void setSqlParameters(PreparedStatement stm, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        int pos = setUpdateParameters(stm, databaseTimeZone, databaseType);
        pos = setSingleValuedKeyParameters(stm, databaseTimeZone, pos, dataObjects[startIndex], databaseType);
        setInClauseParameters(stm, databaseTimeZone, pos, databaseType);
    }

    private void setInClauseParameters(PreparedStatement stm, TimeZone databaseTimeZone, int pos, DatabaseType databaseType) throws SQLException
    {
        SingleColumnAttribute attr = (SingleColumnAttribute) diffPk;
        for(int i=startIndex; i < endIndex; i++)
        {
            attr.setSqlParameters(stm, dataObjects[i], pos, databaseTimeZone, databaseType);
            pos++;
        }
    }

    private int setSingleValuedKeyParameters(PreparedStatement stm, TimeZone databaseTimeZone,
            int pos, MithraDataObject data, DatabaseType databaseType) throws SQLException
    {
        if (this.versionAttribute != null)
        {
            this.versionAttribute.setVersionAttributeSqlParameters(stm, data, pos, databaseTimeZone);
            pos++;
        }
        for(int i=0;i<singleValuedPrimaryKeys.size();i++)
        {
            SingleColumnAttribute attr = (SingleColumnAttribute) singleValuedPrimaryKeys.get(i);
            attr.setSqlParameters(stm, data, pos, databaseTimeZone, databaseType);
            pos++;
        }
        return pos;
    }

    private int setUpdateParameters(PreparedStatement stm, TimeZone databaseTimeZone, DatabaseType databaseType)  throws SQLException
    {
        int pos = 1;
        for(int i=0;i<updates.size();i++)
        {
            AttributeUpdateWrapper wrapper = updates.get(i);
            wrapper.setSqlParameters(stm, pos, databaseTimeZone, databaseType);
            pos++;
        }
        return pos;
    }

    public String getFirstPartSql()
    {
        return firstPartSql;
    }

    public void checkUpdateResult(int actualUpdates, Logger logger) throws MithraDatabaseException
    {
        int expectedUpdates = getExpectedUpdates();
        if (actualUpdates < expectedUpdates)
        {
            MithraObjectPortal mithraObjectPortal = this.getPortal();
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            boolean optimistic = mithraObjectPortal.getTxParticipationMode(tx).isOptimisticLocking();
            if (optimistic)
            {
                for(int i=startIndex; i < endIndex; i++)
                {
                    logger.error("Optimistic lock possibly failed on " + dataObjects[i].zGetPrintablePrimaryKey());
                    mithraObjectPortal.getCache().markDirtyForReload(dataObjects[i], tx);
                }
                MithraOptimisticLockException mithraOptimisticLockException = new MithraOptimisticLockException("Optimistic lock failed, see above log for specific objects.");
                if (tx.retryOnOptimisticLockFailure())
                {
                    mithraOptimisticLockException.setRetriable(true);
                }
                throw mithraOptimisticLockException;
            }
            else
            {
                logger.warn("multi update command did not update the correct number of rows. Expected "+expectedUpdates+ " got "+actualUpdates);
            }
        }
    }

    public int getExpectedUpdates()
    {
        return endIndex - startIndex;
    }

    public MithraDataObject[] getDataObjectsForNotification()
    {
        return getDataObjects(isDated);
    }

    public String getPrintableSql()
    {
        String result = this.getFirstPartSql();
        result += diffPk.getColumnName()+
                " in (?...)";
        return result;
    }

    public boolean isEligibleForUpdateViaInsert()
    {
        return this.getMithraObjects().size() == this.getIndexedObjects().size() &&
                this.getPortal().useMultiUpdate();
    }
}
