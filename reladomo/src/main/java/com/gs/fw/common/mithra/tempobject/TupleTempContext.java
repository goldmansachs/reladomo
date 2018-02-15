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

package com.gs.fw.common.mithra.tempobject;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.cache.ExtractorBasedHashStrategy;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationClassLevelNotificationListener;
import com.gs.fw.common.mithra.portal.TupleMithraObjectPortal;
import com.gs.fw.common.mithra.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;


public class TupleTempContext implements Serializable, CommonTempContext
{
    private static final Logger logger = LoggerFactory.getLogger(TupleTempContext.class);

    private Extractor tupleSourceAttribute;
    private SingleColumnAttribute[] persistentTupleAttributes;
    private int[] maxLengths;
    private transient Attribute[] prototypeAttributes;
    private transient Extractor prototypeSourceAttribute;
    private transient MithraObjectPortal portal;
    private transient RelatedFinder tupleRelatedFinder;
    private transient volatile Attribute[] tupleAttributesAsPlainAttributes;
    private transient boolean isSingleThreaded = false;
    private Map<NameKey, String> nameMap = new UnifiedMap();
    private Map<NameKey, String> fullyQualifiedNameMap = new UnifiedMap(); // also denotes that temp table has been created

    private static final AtomicInteger txCount = new AtomicInteger(-1);

    private static short pid;

    private static long ip;
    private static volatile char[] constantStart = new char[20];
    private boolean prefersMultiThreadedDataAccess = true;
    private final boolean isForQuery;
    private boolean retryHook;
    private transient InsertRetryBlock retryBlock;

    static
    {
        ip = MithraProcessInfo.getIpAsLong();
        pid = MithraProcessInfo.getPidAsShort();
        fillConstantStart();
    }


    public TupleTempContext(Attribute[] prototypeAttributes, int[] maxLengths, boolean isForQuery)
    {
        this(prototypeAttributes, prototypeAttributes[0].getSourceAttribute(), maxLengths, isForQuery);
    }

    public TupleTempContext(Attribute[] prototypeAttributes, Extractor sourceAttribute, int[] maxLengths, boolean isForQuery)
    {
        this.maxLengths = maxLengths;
        this.prototypeSourceAttribute = sourceAttribute;
        this.persistentTupleAttributes = createPersistentTupleAttributes(prototypeAttributes, prototypeSourceAttribute);
        this.isForQuery = isForQuery;
    }

    public TupleTempContext(Attribute[] prototypeAttributes, boolean isForQuery)
    {
        this(prototypeAttributes, null, isForQuery);
    }

    public boolean isForQuery()
    {
        return isForQuery;
    }

    private SingleColumnAttribute[] createPersistentTupleAttributes(Attribute[] prototypeAttributes, Extractor prototypeSourceAttribute)
    {
        int sourceIndex = -1;
        if (prototypeSourceAttribute != null)
        {
            for(int i=prototypeAttributes.length - 1; i >= 0; i--)
            {
                if (prototypeAttributes[i].equals(prototypeSourceAttribute))
                {
                    sourceIndex = i;
                    break;
                }
            }
            if (sourceIndex == -1 && prototypeSourceAttribute instanceof Attribute)
            {
                sourceIndex = prototypeAttributes.length;
                Attribute[] withSource = new Attribute[prototypeAttributes.length + 1];
                System.arraycopy(prototypeAttributes, 0, withSource, 0, prototypeAttributes.length);
                withSource[prototypeAttributes.length] = (Attribute) prototypeSourceAttribute;
                prototypeAttributes = withSource;
            }
            if (prototypeSourceAttribute instanceof SingleColumnAttribute)
            {
                SingleColumnAttribute sourceAttr = ((SingleColumnAttribute)prototypeSourceAttribute).createTupleAttribute(sourceIndex, this);
                sourceAttr.setColumnName(null);
                this.tupleSourceAttribute = (Extractor) sourceAttr;
            }
            else
            {
                this.tupleSourceAttribute = prototypeSourceAttribute;
            }
        }
        this.prototypeAttributes = prototypeAttributes;
        int length = prototypeAttributes.length;
        if (sourceIndex != -1) length--;
        SingleColumnAttribute[] result = new SingleColumnAttribute[length];
        int count = 0;
        for(int i=0;i<prototypeAttributes.length;i++)
        {
            if (i != sourceIndex)
            {
                if (prototypeAttributes[i] instanceof AsOfAttribute)
                {
                    result[count++] = ((SingleColumnAttribute)((AsOfAttribute)prototypeAttributes[i]).getToAttribute()).createTupleAttribute(i, this);
                }
                else
                {
                    result[count++] = ((SingleColumnAttribute)prototypeAttributes[i]).createTupleAttribute(i, this);
                }
            }
        }
        return result;
    }

    public Extractor getPrototypeSourceAttribute()
    {
        return prototypeSourceAttribute;
    }

    public RelatedFinder getRelatedFinder()
    {
        if (this.tupleRelatedFinder == null)
        {
            this.tupleRelatedFinder = new TupleRelatedFinder();
        }
        return this.tupleRelatedFinder;
    }

    public MithraObjectPortal getPortal()
    {
        if (this.portal == null)
        {
            this.portal = new TupleMithraObjectPortal(this);
        }
        return this.portal;
    }

    public Operation exists(Object source)
    {
        Mapper mapper;
        if (this.prototypeAttributes.length == 1)
        {
            mapper = new EqualityMapper(this.prototypeAttributes[0], (Attribute) this.persistentTupleAttributes[0]);
        }
        else
        {
            SingleColumnAttribute prototypeSourceAttribute = (SingleColumnAttribute) prototypeAttributes[0].getSourceAttribute();
            InternalList mappers = new InternalList(prototypeAttributes.length);
            int count = 0;
            for(int i=0;i<prototypeAttributes.length;i++)
            {
                if (prototypeSourceAttribute != null && prototypeAttributes[i].equals(prototypeSourceAttribute))
                {
                    mappers.add(new EqualityMapper(this.prototypeAttributes[i], (Attribute) this.tupleSourceAttribute));
                }
                else
                {
                    mappers.add(new EqualityMapper(this.prototypeAttributes[i], (Attribute) this.persistentTupleAttributes[count++]));
                }
            }
            mapper = new MultiEqualityMapper(mappers);
        }
        Operation rightHand;
        if (tupleSourceAttribute != null)
        {
            rightHand = getSourceOperation(source, null);
        }
        else
        {
            rightHand = new All((Attribute)this.persistentTupleAttributes[0]);
        }
        return new MappedOperation(mapper, rightHand);
    }

    public void insert(List prototypeObjects, MithraObjectPortal destination, int bulkInsertThreshold, boolean isParallel)
    {
        this.prefersMultiThreadedDataAccess = isParallel;
        FullUniqueIndex index = new FullUniqueIndex(ExtractorBasedHashStrategy.create(prototypeAttributes), prototypeObjects.size());
        index.addAll(prototypeObjects);
        if (prototypeObjects.size() != index.size())
        {
            prototypeObjects = index.getAll();
        }
        destination.getMithraTuplePersister().insertTuples(this,
                new LazyListAdaptor(prototypeObjects, LazyTuple.createFactory(prototypeAttributes)), bulkInsertThreshold);
        if (!this.prefersMultiThreadedDataAccess && this.retryHook)
        {
            this.retryBlock = new InsertRetryBlock(prototypeObjects, destination, bulkInsertThreshold);
        }
    }

    public void insert(SetBasedAtomicOperation op, MithraObjectPortal destination, int bulkInsertThreshold, Object source, boolean isParallel)
    {
        insert(destination, bulkInsertThreshold, source, new SetBasedTupleList(op), isParallel);
    }

    public void insert(MithraObjectPortal destination, int bulkInsertThreshold, Object source, List tupleList, boolean isParallel)
    {
        this.prefersMultiThreadedDataAccess = isParallel;
        destination.getMithraTuplePersister().insertTuplesForSameSource(this, tupleList,
                bulkInsertThreshold, source);
    }

    private static void fillConstantStart()
    {
        long startTime = TempTableNamer.maskUpperBits(System.currentTimeMillis() >> 4, 52) | (TempTableNamer.maskUpperBits(pid >> 13, 3) << 52);
        char[] head = new char[20];
        TempTableNamer.fillBits(head, (TempTableNamer.maskUpperBits(pid, 13) << 32) | ip, 0, 13+32);
        TempTableNamer.fillBits(head, startTime, (13+32)/5, 55);
        constantStart = head;
    }

    public Extractor getTupleSourceExtractor()
    {
        return tupleSourceAttribute;
    }

    public Operation getSourceOperation(Object source, Attribute resultSourceAttribute)
    {
        if (!hasSourceAttribute()) return null;
        if (tupleSourceAttribute instanceof Attribute)
        {
            return ((Attribute) tupleSourceAttribute).nonPrimitiveEq(source);
        }
        else if (tupleSourceAttribute instanceof OperationParameterExtractor)
        {
            SingleColumnAttribute tupleSrcAttribute = ((SingleColumnAttribute) resultSourceAttribute).createTupleAttribute(persistentTupleAttributes.length, this);
            tupleSrcAttribute.setColumnName(null);
            return ((Attribute) tupleSrcAttribute).nonPrimitiveEq(source);
        }
        throw new RuntimeException("Should not get here");
    }

    public boolean hasSourceAttribute()
    {
        return tupleSourceAttribute != null;
    }

    public SingleColumnAttribute[] getPersistentTupleAttributes()
    {
        return persistentTupleAttributes;
    }

    public synchronized String getNominalTableName(Object source, PersisterId persisterId)
    {
        NameKey key = new NameKey(persisterId, source);
        String nominalName = nameMap.get(key);
        if (nominalName == null)
        {
            char[] toFill = new char[25];
            toFill[0] = 'X';
            long next = TempTableNamer.maskUpperBits(txCount.incrementAndGet(), 20);
            if (next == 500000)
            {
                fillConstantStart();
            }
            TempTableNamer.fillBits(toFill, next, 1, 20);
            System.arraycopy(constantStart, 0, toFill, 5, 20);
            nominalName = new String(toFill);
            nameMap.put(key, nominalName);
        }
        return nominalName;
    }

    public synchronized String getFullyQualifiedTableName(Object source, PersisterId persisterId)
    {
        NameKey key = new NameKey(persisterId, source);
        return fullyQualifiedNameMap.get(key);
    }

    public synchronized void setFullyQualifiedTableName(Object source, PersisterId persisterId, String tableName, MithraObjectPortal mithraObjectPortal)
    {
        NameKey key = new NameKey(persisterId, source);
        key.setPortal(mithraObjectPortal);
        this.fullyQualifiedNameMap.put(key, tableName);
    }

    public Attribute[] getTupleAttributesAsAttributeArray()
    {
        Attribute[] result = this.tupleAttributesAsPlainAttributes;
        if (result == null)
        {
            result = new Attribute[this.persistentTupleAttributes.length];
            for(int i=0;i<this.persistentTupleAttributes.length;i++)
            {
                result[i] = (Attribute) this.persistentTupleAttributes[i];
            }
            this.tupleAttributesAsPlainAttributes = result;
        }
        return result;
    }

    public synchronized void updateTempTableNames(TupleTempContext incoming, MithraObjectPortal portal)
    {
        nameMap.putAll(incoming.nameMap);
        for(Map.Entry<NameKey,String> entry : incoming.fullyQualifiedNameMap.entrySet())
        {
            NameKey nameKey = entry.getKey();
            if (!fullyQualifiedNameMap.containsKey(nameKey))
            {
                nameKey.setPortal(portal);
                fullyQualifiedNameMap.put(nameKey, entry.getValue());
            }
        }
    }

    public synchronized void destroy()
    {
        for(Map.Entry<NameKey,String> entry : this.fullyQualifiedNameMap.entrySet())
        {
            NameKey nameKey = entry.getKey();
            String tableName = entry.getValue();
            try
            {
                nameKey.portal.getMithraTuplePersister().destroyTempContext(tableName, nameKey.source, this.isForQuery);
            }
            catch (Throwable t)
            {
                logger.error("Could not destroy temp context", t);
            }
        }
        clearNames();
        clearRetry();
    }

    private void clearRetry()
    {
        this.retryBlock = null;
    }

    private synchronized void clearNames()
    {
        this.nameMap.clear();
        this.fullyQualifiedNameMap.clear();
    }

    public boolean mapsToUniqueIndex(List attributes)
    {
        int size = this.persistentTupleAttributes.length + (this.tupleSourceAttribute == null ? 0 : 1);
        return attributes.size() == size;
    }

    public String getTableNameForQuery(SqlQuery sqlQuery, MapperStackImpl mapperStack, int currentSourceNumber, PersisterId persisterId)
    {
        Object source = null;
        if (tupleSourceAttribute != null)
        {
            source = sqlQuery.getSourceAttributeValue(mapperStack,currentSourceNumber);
        }
        return this.getFullyQualifiedTableName(source, persisterId);
    }

    public int getTupleAttributeCount()
    {
        return this.persistentTupleAttributes.length + (tupleSourceAttribute == null ? 0 : 1);
    }

    public List<Tuple> parseResultSet(ResultSet rs, int attributeCount, DatabaseType databaseType, TimeZone databaseTimeZone) throws SQLException
    {
        FastList<Tuple> result = new FastList<Tuple>();
        while(rs.next())
        {
            Object[] tupleValues = new Object[attributeCount + (tupleSourceAttribute == null ? 0 : 1)];
            ArrayTuple arrayTuple = new ArrayTuple(tupleValues);
            for(int i=0;i<attributeCount;i++)
            {
                ((Attribute)persistentTupleAttributes[i]).setValue(arrayTuple,
                        persistentTupleAttributes[i].readResultSet(rs, i+1, databaseType, databaseTimeZone));
            }
            result.add(arrayTuple);
        }
        return result;
    }

    public Operation all()
    {
        return new All((Attribute)persistentTupleAttributes[0]);
    }

    public Map<Attribute, Attribute> getPrototypeToTupleAttributeMap()
    {
        Map<Attribute, Attribute> result = new UnifiedMap<Attribute, Attribute>(this.persistentTupleAttributes.length);
        Attribute prototypeSourceAttribute = prototypeAttributes[0].getSourceAttribute();
        int pos = 0;
        for(Attribute protoAttr: prototypeAttributes)
        {
            if (!protoAttr.equals(prototypeSourceAttribute))
            {
                result.put(protoAttr, (Attribute)persistentTupleAttributes[pos++]);
            }
        }
        return result;
    }

    @Override
    public void markSingleThreaded()
    {
        // will become interesting later when we introduce parallelization for certain retrievals.
        isSingleThreaded = true;
    }

    @Override
    public void cleanupAndRecreate()
    {
        this.clearNames();
        if (this.retryBlock != null)
        {
            this.insert(this.retryBlock.prototypeObjects, this.retryBlock.destination, this.retryBlock.bulkInsertThreshold, false);
            this.retryBlock = null;
        }
    }

    public boolean isSingleThreaded()
    {
        return isSingleThreaded;
    }

    public void setPrefersMultiThreadedDataAccess(boolean prefersMultiThreadedDataAccess)
    {
        this.prefersMultiThreadedDataAccess = prefersMultiThreadedDataAccess;
    }

    public boolean prefersMultiThreadedDataAccess()
    {
        return prefersMultiThreadedDataAccess;
    }

    public int getMaxLength(int pos)
    {
        if (this.maxLengths != null && pos < this.maxLengths.length)
        {
            return maxLengths[pos];
        }
        return 0;
    }

    public void enableRetryHook()
    {
        this.retryHook = !MithraManagerProvider.getMithraManager().isInTransaction();
    }

    private static class NameKey implements Serializable
    {
        private Object source;
        private PersisterId persisterId;
        private transient MithraObjectPortal portal;

        private NameKey(PersisterId persisterId, Object source)
        {
            this.persisterId = persisterId;
            this.source = source;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NameKey nameKey = (NameKey) o;

            if (!persisterId.equals(nameKey.persisterId)) return false;
            return !(source != null ? !source.equals(nameKey.source) : nameKey.source != null);

        }

        public void setPortal(MithraObjectPortal portal)
        {
            this.portal = portal;
        }

        public int hashCode()
        {
            int result = persisterId.hashCode();
            result = HashUtil.combineHashes(result, (source != null ? source.hashCode() : HashUtil.NULL_HASH));
            return result;
        }
    }

    private synchronized void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        if (this.tupleSourceAttribute != null && this.tupleSourceAttribute instanceof SingleColumnAttribute)
        {
            ((SingleColumnAttribute)this.tupleSourceAttribute).setColumnName(null);
        }
    }

    private class TupleRelatedFinder implements RelatedFinder
    {
        public Operation all()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public MithraList constructEmptyList()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public MithraList findMany(com.gs.fw.finder.Operation operation)
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public MithraList findManyBypassCache(com.gs.fw.finder.Operation operation)
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public Object findOne(com.gs.fw.finder.Operation operation)
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public Object findOneBypassCache(com.gs.fw.finder.Operation operation)
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public AsOfAttribute[] getAsOfAttributes()
        {
            return null;
        }

        public Attribute getAttributeByName(String attributeName)
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public Function getAttributeOrRelationshipSelector(String attributeName)
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public List getDependentRelationshipFinders()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public String getFinderClassName()
        {
            return ".<TemporaryTuple>Finder";
        }

        public int getHierarchyDepth()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public MithraObjectPortal getMithraObjectPortal()
        {
            return getPortal();
        }

        public Attribute[] getPersistentAttributes()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public Attribute[] getPrimaryKeyAttributes()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public RelatedFinder getRelationshipFinderByName(String relationshipName)
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public List getRelationshipFinders()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public int getSerialVersionId()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public Attribute getSourceAttribute()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public SourceAttributeType getSourceAttributeType()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public VersionAttribute getVersionAttribute()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public boolean isPure()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        public boolean isTemporary()
        {
            // todo: rezaem: implement not implemented method
            throw new RuntimeException("not implemented");
        }

        @Override
        public void registerForNotification(MithraApplicationClassLevelNotificationListener listener)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void registerForNotification(Set sourceAttributeValueSet, MithraApplicationClassLevelNotificationListener listener)
        {
            throw new RuntimeException("not implemented");
        }
    }

    private class InsertRetryBlock
    {
        List prototypeObjects;
        MithraObjectPortal destination;
        int bulkInsertThreshold;

        public InsertRetryBlock(List prototypeObjects, MithraObjectPortal destination, int bulkInsertThreshold)
        {
            this.prototypeObjects = prototypeObjects;
            this.destination = destination;
            this.bulkInsertThreshold = bulkInsertThreshold;
        }
    }
}
