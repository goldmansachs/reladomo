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

import com.gs.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;
import com.gs.fw.common.mithra.cache.ExtractorBasedHashStrategy;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.cache.HashStrategy;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.MultiHashMap;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class MultiUpdateOperation extends TransactionOperation
{
    private static final byte MULTI_IN = (byte) 10;
    private static final byte MULTI_OR = (byte) 20;
    private static final byte SINGLE_UPDATE = (byte) 30;

    private final List<AttributeUpdateWrapper> updates;

    private final List mithraObjects;
    private boolean isDated;

    private List singleValuedPrimaryKeys;
    private VersionAttribute versionAttribute;
    private MithraFastList multiValuedPrimaryKeys;
    private MithraDataObject[] orderedDataObjects;
    private int startIndex = 0;
    private int endIndex = 0;
    private String firstPartSql;
    private int ungroupableSize = 0;
    private HashStrategy hashStrategy;
    private byte currentState = MULTI_IN;
    private int multiOrClauses;
    private int firstPartSqlSingleKeyPosition;
    private String multiOrSql;
    private boolean issuedMultiUpdate = false;
    private boolean hasMultiIn = false;
    private FullUniqueIndex index;

    public MultiUpdateOperation(UpdateOperation first, UpdateOperation second)
    {
        super(first.getMithraObject(), first.getPortal());
        mithraObjects = new FastList();
        addObject(first);
        addObject(second);
        this.updates = first.getUpdates();
        this.isDated = this.getPortal().getFinder().getAsOfAttributes() != null;
    }

    public MultiUpdateOperation(List updates, List mithraObjects)
    {
        super((MithraTransactionalObject) mithraObjects.get(0), ((AttributeUpdateWrapper) updates.get(0)).getAttribute().getOwnerPortal());
        isDated = this.getPortal().getFinder().getAsOfAttributes() != null;
        this.updates = updates;
        this.mithraObjects = mithraObjects;
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
                        this.updates.set(i, otherUpdate);
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
                return true;
            }
        }
        return false;
    }

    public void prepareForSqlGeneration(String fullyQualifiedTableName, DatabaseType databaseType)
    {
        MithraTransactionalObject obj = this.getMithraObject();
        RelatedFinder finder = this.getPortal().getFinder();
        MithraDataObject[] dataObjects = getDataObjects(isDated);
        findAllPkAttributes(finder, obj, dataObjects);
        ObjectIntHashMap totalCounts = countUniquePkInstances(dataObjects);
        if (multiValuedPrimaryKeys.size() == 0)
        {
            // this seems to happen for some odd reason
            this.currentState = SINGLE_UPDATE;
        }
        else
        {
            sortDataByCount(totalCounts, dataObjects);
            createHashStrategy();
        }
        this.orderedDataObjects = dataObjects;
        this.hasMultiIn = databaseType.supportsMultiValueInClause();
        createFirstPartSql(fullyQualifiedTableName);
    }

    private void createHashStrategy()
    {
        if (multiValuedPrimaryKeys.size() > 1)
        {
            Extractor[] extractors = new Extractor[multiValuedPrimaryKeys.size() - 1];
            for(int i=0;i<multiValuedPrimaryKeys.size() - 1;i++)
            {
                extractors[i] = (Extractor) multiValuedPrimaryKeys.get(i);
            }
            this.hashStrategy = ExtractorBasedHashStrategy.create(extractors);
        }
        else
        {
            this.hashStrategy = TrivialHashStrategy.instance;
        }
    }

    private void sortDataByCount(ObjectIntHashMap totalCounts, MithraDataObject[] dataObjects)
    {
        if (multiValuedPrimaryKeys.size() > 1)
        {
            multiValuedPrimaryKeys.sortThis(new ByCountComparator(totalCounts));
            OrderBy orderBy = ((Attribute) multiValuedPrimaryKeys.get(0)).ascendingOrderBy();
            for(int i=1;i<multiValuedPrimaryKeys.size();i++)
            {
                orderBy = orderBy.and(((Attribute) multiValuedPrimaryKeys.get(i)).ascendingOrderBy());
            }
            Arrays.sort(dataObjects, orderBy);
        }
    }

    private ObjectIntHashMap countUniquePkInstances(MithraDataObject[] dataObjects)
    {
        ObjectIntHashMap totalCounts = new ObjectIntHashMap(multiValuedPrimaryKeys.size());
        this.singleValuedPrimaryKeys = new FastList(multiValuedPrimaryKeys.size());
        for(int i=0;i < multiValuedPrimaryKeys.size(); )
        {
            Attribute attr = (Attribute) multiValuedPrimaryKeys.get(i);
            int count = attr.zCountUniqueInstances(dataObjects);
            if (count == 1)
            {
                singleValuedPrimaryKeys.add(multiValuedPrimaryKeys.remove(i));
            }
            else
            {
                i++;
                totalCounts.put(attr, count);
            }
        }
        return totalCounts;
    }

    private void findAllPkAttributes(RelatedFinder finder, MithraTransactionalObject obj, MithraDataObject[] data)
    {
        Attribute[] primaryKeyAttributes = finder.getPrimaryKeyAttributes();
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        VersionAttribute versionAttribute = finder.getVersionAttribute();
        this.multiValuedPrimaryKeys = new MithraFastList(primaryKeyAttributes.length + 3);
        for(int i=0;i < primaryKeyAttributes.length; i++)
        {
            if (!primaryKeyAttributes[i].isSourceAttribute())
            {
                multiValuedPrimaryKeys.add(primaryKeyAttributes[i]);
            }
        }
        if (asOfAttributes != null)
        {
            isDated = true;
            for(int i=0;i < asOfAttributes.length; i++)
            {
                multiValuedPrimaryKeys.add(asOfAttributes[i].getToAttribute());
            }
        }
        if (obj.zGetPortal().getTxParticipationMode().isOptimisticLocking())
        {
            if (versionAttribute != null)
            {
                this.versionAttribute = versionAttribute;
            }
            if (asOfAttributes != null)
            {
                Attribute optimisticAttribute = getOptimisticKey(asOfAttributes, data);
                if (optimisticAttribute != null) multiValuedPrimaryKeys.add(optimisticAttribute);
            }
        }
    }

    private Attribute getOptimisticKey(AsOfAttribute[] asOfAttributes, MithraDataObject[] data)
    {
        AsOfAttribute businessDate = null;
        AsOfAttribute processingDate = null;
        if (asOfAttributes.length == 2)
        {
            businessDate = asOfAttributes[0];
            processingDate = asOfAttributes[1];
        }
        else if (asOfAttributes[0].isProcessingDate())
        {
            processingDate = asOfAttributes[0];
        }
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
        for(int i=0;i<multiValuedPrimaryKeys.size() - 1; i++)
        {
            SingleColumnAttribute attr = (SingleColumnAttribute) multiValuedPrimaryKeys.get(i);
            buf.append(attr.getColumnName());
            buf.append(" = ? and ");
        }
        this.firstPartSql = buf.toString();
    }

    private StringBuffer createFirstPartSingleKeySql(String fullyQualifiedTableName)
    {
        StringBuffer buf = new StringBuffer(30 + this.updates.size()*20 + singleValuedPrimaryKeys.size() * 20+(multiValuedPrimaryKeys.size() - 1)*20);
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
            if (attr.isAttributeNull(this.orderedDataObjects[0]))
            {
                buf.append(" IS NULL ");
                this.singleValuedPrimaryKeys.remove(i);
            }
            else
            {
                buf.append(" = ? ");
                i++;
            }
            if (i < this.singleValuedPrimaryKeys.size() || multiValuedPrimaryKeys.size() > 0)
            {
                buf.append(" and ");
            }
        }
        this.firstPartSqlSingleKeyPosition = buf.length();
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
        switch(currentState)
        {
            case MULTI_IN:
                return getNextMultiUpdateSqlForMultiIn(maxParams);
            case MULTI_OR:
                return getNextMultiUpdateSqlForMultiOr();
            default:
                return getSingleUpdate();
        }
    }

    private String getSingleUpdate()
    {
        if (issuedMultiUpdate)
        {
            return null;
        }
        this.issuedMultiUpdate = true;
        return this.firstPartSql;
    }

    private String getNextMultiUpdateSqlForMultiOr()
    {
        if (issuedMultiUpdate)
        {
            this.startIndex = this.endIndex + 1;
        }
        this.issuedMultiUpdate = true;
        if (startIndex >= this.ungroupableSize)
        {
            return null; // we're done with multi updates
        }
        this.endIndex = startIndex + this.multiOrClauses - 1;
        if (endIndex >= this.ungroupableSize)
        {
            this.endIndex = this.ungroupableSize - 1;
            this.multiOrClauses = endIndex - startIndex + 1;
            this.createSqlForMultiOr();
        }
        return this.multiOrSql;
    }

    public String getNextMultiUpdateSqlForMultiIn(int maxParams)
    {
        if (this.multiValuedPrimaryKeys.size() == 0) return null;
        maxParams -= this.updates.size() + this.singleValuedPrimaryKeys.size();
        if (this.endIndex > 0)
        {
            this.startIndex = this.endIndex + 1;
        }
        if (startIndex == this.orderedDataObjects.length)
        {
            setMultiOr(maxParams);
            return getNextMultiUpdateSqlForMultiOr();
        }
        int pos = this.startIndex + 1;
        int count = 1;
        while(count == 1 && pos < orderedDataObjects.length)
        {
            while(pos < orderedDataObjects.length && count < maxParams
                    && this.hashStrategy.equals(orderedDataObjects[startIndex], orderedDataObjects[pos]))
            {
                pos++;
                count++;
            }
            if (count == 1)
            {
                addToUngroupable();
                pos++;
            }
        }
        if (count == 1)
        {
            addToUngroupable();
            setMultiOr(maxParams);
            return getNextMultiUpdateSqlForMultiOr();
        }
        this.endIndex = pos - 1;
        if (startIndex < orderedDataObjects.length)
        {
            String sql = this.firstPartSql + ((SingleColumnAttribute)multiValuedPrimaryKeys.get(multiValuedPrimaryKeys.size() - 1)).getColumnName()+
                    " in (";
            sql += createQuestionMarks(pos - startIndex) + ")";
            return sql;
        }
        setMultiOr(maxParams);
        return getNextMultiUpdateSqlForMultiOr();
    }

    private void setMultiOr(int maxParams)
    {
        this.currentState = MULTI_OR;
        this.startIndex = 0;
        this.endIndex = 0;
        if (this.ungroupableSize > 0)
        {
            maxParams -= this.updates.size() + this.singleValuedPrimaryKeys.size();
            multiOrClauses = maxParams/multiValuedPrimaryKeys.size();
            if (multiOrClauses > this.ungroupableSize)
            {
                multiOrClauses = this.ungroupableSize;
            }
            createSqlForMultiOr();
        }
    }

    private void createSqlForMultiOr()
    {
        if (this.hasMultiIn)
        {
            StringBuilder singleValuesClause = new StringBuilder(multiValuedPrimaryKeys.size()*2+2);
            singleValuesClause.append("(");
            for(int i=0;i<multiValuedPrimaryKeys.size() - 1; i++)
            {
                singleValuesClause.append("?,");
            }
            singleValuesClause.append("?)");
            StringBuilder buf = new StringBuilder(this.firstPartSqlSingleKeyPosition + multiValuedPrimaryKeys.size() * 20 +
                    40 + 2*multiOrClauses + multiOrClauses *singleValuesClause.length());
            buf.append(firstPartSql, 0, this.firstPartSqlSingleKeyPosition);
            buf.append("(");

            for(int i=0;i<multiValuedPrimaryKeys.size() - 1;i++)
            {
                SingleColumnAttribute attr = (SingleColumnAttribute) multiValuedPrimaryKeys.get(i);
                buf.append(attr.getColumnName());
                buf.append(",");
            }
            buf.append(((SingleColumnAttribute)multiValuedPrimaryKeys.get(multiValuedPrimaryKeys.size() - 1)).getColumnName());
            buf.append(") in (select * from (VALUES ");

            for(int i=0;i<multiOrClauses - 1;i++)
            {
                buf.append(singleValuesClause);
                buf.append(",");
            }
            buf.append(singleValuesClause);
            buf.append(") as m0)");
            this.multiOrSql = buf.toString();
        }
        else
        {
            StringBuilder singleAndClause = new StringBuilder(multiValuedPrimaryKeys.size()*20+2);
            singleAndClause.append("(");
            for(int i=0;i<multiValuedPrimaryKeys.size() - 1; i++)
            {
                SingleColumnAttribute attr = (SingleColumnAttribute) multiValuedPrimaryKeys.get(i);
                singleAndClause.append(attr.getColumnName());
                singleAndClause.append(" = ? and ");
            }
            singleAndClause.append(((SingleColumnAttribute)multiValuedPrimaryKeys.get(multiValuedPrimaryKeys.size() - 1)).getColumnName());
            singleAndClause.append(" = ?)");
            StringBuilder buf = new StringBuilder(this.firstPartSqlSingleKeyPosition + 2 + 4*multiOrClauses + multiOrClauses *singleAndClause.length());
            buf.append(firstPartSql, 0, this.firstPartSqlSingleKeyPosition);
            buf.append("(");
            for(int i=0;i<multiOrClauses - 1;i++)
            {
                buf.append(singleAndClause);
                buf.append(" or ");
            }
            buf.append(singleAndClause);
            buf.append(")");
            this.multiOrSql = buf.toString();
        }
    }

    private void addToUngroupable()
    {
        orderedDataObjects[ungroupableSize] = orderedDataObjects[startIndex];
        ungroupableSize++;
        startIndex++;
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
        pos = setSingleValuedKeyParameters(stm, databaseTimeZone, pos, orderedDataObjects[startIndex], databaseType);
        if (currentState == MULTI_IN)
        {
            pos = setInitialKeyParameters(stm, databaseTimeZone, pos, orderedDataObjects[startIndex], multiValuedPrimaryKeys.size() - 1, databaseType);
            setInClauseParameters(stm, databaseTimeZone, pos, databaseType);
        }
        else if (currentState == MULTI_OR)
        {
            for(int i=startIndex; i<=endIndex; i++)
            {
                pos = setInitialKeyParameters(stm, databaseTimeZone, pos, orderedDataObjects[i], multiValuedPrimaryKeys.size(), databaseType);
            }
        }
    }

    private void setInClauseParameters(PreparedStatement stm, TimeZone databaseTimeZone, int pos, DatabaseType databaseType) throws SQLException
    {
        SingleColumnAttribute attr = (SingleColumnAttribute) multiValuedPrimaryKeys.get(multiValuedPrimaryKeys.size() - 1);
        for(int i=startIndex; i <= endIndex; i++)
        {
            attr.setSqlParameters(stm, orderedDataObjects[i], pos, databaseTimeZone, databaseType);
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

    private int setInitialKeyParameters(PreparedStatement stm, TimeZone databaseTimeZone,
            int pos, MithraDataObject data, int size, DatabaseType databaseType) throws SQLException
    {
        for(int i=0;i<size;i++)
        {
            SingleColumnAttribute attr = (SingleColumnAttribute) multiValuedPrimaryKeys.get(i);
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

    public List segregateUpdatesBySourceAttribute()
    {
        MultiHashMap<Object, MithraTransactionalObject> map = new MultiHashMap<Object, MithraTransactionalObject>();
        Attribute sourceAttribute = this.getPortal().getFinder().getSourceAttribute();
        if (isDated)
        {
            for(int i=0;i<this.mithraObjects.size();i++)
            {
                MithraTransactionalObject mithraTransactionalObject = (MithraTransactionalObject) this.mithraObjects.get(i);
                MithraDataObject data = mithraTransactionalObject.zGetCurrentData();
                map.put(sourceAttribute.valueOf(data), mithraTransactionalObject);
            }
        }
        else
        {
            for(int i=0;i<this.mithraObjects.size();i++)
            {
                MithraTransactionalObject mithraTransactionalObject = (MithraTransactionalObject) this.mithraObjects.get(i);
                MithraDataObject data = mithraTransactionalObject.zGetTxDataForRead();
                map.put(sourceAttribute.valueOf(data), mithraTransactionalObject);
            }
        }

        if (map.size() > 1)
        {
            FastList result = new FastList(map.size());
            for (List<MithraTransactionalObject> objects : map.values())
            {
                result.add(new MultiUpdateOperation(this.updates, objects));
            }
            return result;
        }
        else
        {
            return ListFactory.create(this);
        }
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
                for(int i=startIndex; i <= endIndex; i++)
                {
                    logger.error("Optimistic lock possibly failed on " + orderedDataObjects[i].zGetPrintablePrimaryKey());
                    mithraObjectPortal.getCache().markDirtyForReload(orderedDataObjects[i], tx);
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
        return endIndex - startIndex + 1;
    }

    public MithraDataObject[] getDataObjectsForNotification()
    {
        return getDataObjects(isDated);
    }

    public String getPrintableSql()
    {
        String result = this.getFirstPartSql();
        if (multiValuedPrimaryKeys.size() > 0)
        {
            result += ((SingleColumnAttribute)multiValuedPrimaryKeys.get(multiValuedPrimaryKeys.size() - 1)).getColumnName()+
                " in (?...)";
        }
        return result;
    }


    private static class ByCountComparator implements Comparator
    {
        private ObjectIntHashMap countByAttribute;

        public ByCountComparator(ObjectIntHashMap countByAttribute)
        {
            this.countByAttribute = countByAttribute;
        }

        public int compare(Object o1, Object o2)
        {
            return countByAttribute.get(o1) - countByAttribute.get(o2);
        }
    }

    private static class TrivialHashStrategy implements HashStrategy
    {
        private static final TrivialHashStrategy instance = new TrivialHashStrategy();

        public int computeHashCode(Object o)
        {
            return 0;
        }

        public boolean equals(Object first, Object second)
        {
            return true;
        }
    }

    public boolean isEligibleForUpdateViaInsert()
    {
        return this.getMithraObjects().size() == this.getIndexedObjects().size() &&
                this.getPortal().useMultiUpdate();
    }
}
