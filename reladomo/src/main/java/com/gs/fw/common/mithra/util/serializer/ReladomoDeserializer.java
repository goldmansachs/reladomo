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

package com.gs.fw.common.mithra.util.serializer;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.BigDecimalAttribute;
import com.gs.fw.common.mithra.attribute.BooleanAttribute;
import com.gs.fw.common.mithra.attribute.ByteArrayAttribute;
import com.gs.fw.common.mithra.attribute.ByteAttribute;
import com.gs.fw.common.mithra.attribute.CharAttribute;
import com.gs.fw.common.mithra.attribute.DateAttribute;
import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.attribute.FloatAttribute;
import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.attribute.LongAttribute;
import com.gs.fw.common.mithra.attribute.ShortAttribute;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.attribute.TimeAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.attribute.TupleAttribute;
import com.gs.fw.common.mithra.behavior.state.PersistedState;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.cache.HashStrategy;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.NoOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.ImmutableTimestamp;
import com.gs.fw.common.mithra.util.KeyWithHashStrategy;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.MultiHashMap;
import com.gs.fw.common.mithra.util.ReflectionMethodCache;
import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.finder.Navigation;
import com.gs.reladomo.metadata.ReladomoClassMetaData;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * This class is not thread safe. A deserializer must be instantiated for every deserialized stream.
 *
 * "Field" means either an attribute or an annotated method.
 *
 * @param <T>
 */
public class ReladomoDeserializer<T extends MithraObject>
{
    private static final Map<String, Object> EMPTY_MAP = Maps.fixedSize.of();
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final Object[] NULL_ARGS = (Object[]) null;
    private static final Object[] SINGLE_NULL = new Object[1];
    private final SimpleDateFormat timestampFormat = new SimpleDateFormat(DATE_FORMAT);

    //todo: key here should be both class and finder to support inheritance
    private static final Function<RelatedFinder, DeserializationClassMetaData> DESERIALIZATION_META_DATA_FACTORY = new Function<RelatedFinder, DeserializationClassMetaData>()
    {
        @Override
        public DeserializationClassMetaData valueOf(RelatedFinder relatedFinder)
        {
            return new DeserializationClassMetaData(relatedFinder);
        }
    };

    public Attribute getCurrentAttribute()
    {
        return this.data.attribute;
    }

    public enum FieldOrRelation
    {
        Attribute, ToOneRelationship, ToManyRelationship, AnnotatedMethod, Unknown
    }

    private static final UnifiedMap<String, DeserializationClassMetaData> DESERIALIZATION_META_DATA_CACHE = UnifiedMap.newMap();

    private StackableData data;
    private Stack<StackableData> dataStack = new Stack<StackableData>();
    private Map<DeserializationClassMetaData, List<PartialDeserialized>> objectsToResolve = UnifiedMap.newMap();
    private State unknownState = InUnknownField.INSTANCE;

    public ReladomoDeserializer(Class aClass)
    {
        this.data = new StackableData();
        this.data.metaData = findDeserializationMetaData(ReladomoClassMetaData.fromBusinessClass(aClass).getFinderInstance());
        this.data.currentState = StartStateHaveMeta.INSTANCE;
    }

    public ReladomoDeserializer(RelatedFinder<T> rootFinder)
    {
        this.data = new StackableData();
        this.data.metaData = findDeserializationMetaData(rootFinder);
        this.data.currentState = StartStateHaveMeta.INSTANCE;
    }

    public ReladomoDeserializer()
    {
        this.data = new StackableData();
        this.data.currentState = StartStateNoMeta.INSTANCE;
    }

    public void setIgnoreUnknown()
    {
        this.unknownState = IgnoreUnknownField.INSTANCE;
    }

    protected DeserializationClassMetaData findDeserializationMetaData(RelatedFinder<T> finder)
    {
        synchronized (DESERIALIZATION_META_DATA_CACHE)
        {
            return DESERIALIZATION_META_DATA_CACHE.getIfAbsentPutWith(finder.getFinderClassName(), DESERIALIZATION_META_DATA_FACTORY, finder);
        }
    }

    protected void pushDataAndStartEmpty(RelatedFinder finder)
    {
        pushDataAndStartEmpty(findDeserializationMetaData(finder));
    }

    protected void pushDataAndStartEmpty(DeserializationClassMetaData deserializationClassMetaData)
    {
        dataStack.push(data);
        this.data = new StackableData();
        this.data.metaData = deserializationClassMetaData;
    }

    protected boolean popData()
    {
        if (dataStack.size() > 0)
        {
            this.data = dataStack.pop();
            return true;
        }
        return false;
    }

    public void storeReladomoClassName(String className) throws DeserializationException
    {
        this.data.currentState.storeReladomoClassName(this, className);
    }

    public void startObjectOrList() throws DeserializationException
    {
        this.data.currentState.startObjectOrList(this);
    }

    public void endObjectOrList() throws DeserializationException
    {
        this.data.currentState.endObjectOrList(this);
    }

    public void startObject() throws DeserializationException
    {
        this.data.currentState.startObject(this);
    }

    public void endObject() throws DeserializationException
    {
        this.data.currentState.endObject(this);
    }

    public void startList() throws DeserializationException
    {
        this.data.currentState.startList(this);
    }

    public void endList() throws DeserializationException
    {
        this.data.currentState.endList(this);
    }

    public FieldOrRelation startFieldOrRelationship(String name) throws DeserializationException
    {
        return this.data.currentState.startFieldOrRelationship(name, this);
    }

    /**
     * skips this field or relationship, as if it was not in the stream. This 
     * is NOT equivalent to being null.
     * 
     * @throws DeserializationException
     */
    public void skipCurrentFieldOrRelationship() throws DeserializationException
    {
        this.data.currentState.skipCurrentFieldOrRelationship(this);
    }

    /**
     *
     * @param name
     * @return null if the name is not an attribute. Must call skipAttributeOrRelationship if the return value is null.
     * @throws DeserializationException
     */
    public Attribute startAttribute(String name) throws DeserializationException
    {
        return this.data.currentState.startAttribute(name, this);
    }

    /**
     *
     * @param name
     * @return ToOneRelationship, ToManyRelationship, Unknown. Must call skipAttributeOrRelationship if the return value is Unknown
     */
    public FieldOrRelation startRelationship(String name) throws DeserializationException
    {
        return this.data.currentState.startRelationship(name, this);
    }

    /**
     * works for attributes and annotated methods. The following formats are the only ones expected:
     * Date: yyyy-MM-dd
     * Timestamp: yyyy-MM-dd HH:mm:ss.SSS
     * Time: HH:mm:ss.SSS
     * byte array: hex encoded values, 2 chars per byte, no 0x in front, e.g. DEA307 is a 3 byte array.
     * boolean: true/false or 1/0
     * numbers: anything parsable by the parse method on the boxed wrapper.
     * 
     * the value "null" is handled for all types, except for String attributes. Call setFieldOrRelationshipNull for strings
     * or anywhere you need to signal null explicitly.
     * 
     * If you need a different format, parse it yourself and call the appropriate set methods (e.g. setTimestampField)
     * @param value
     * @throws DeserializationException
     */
    public void parseFieldFromString(String value) throws DeserializationException
    {
        this.data.currentState.parseFieldFromString(value, this);
    }
    
    public void setByteField(byte value) throws DeserializationException
    {
        this.data.currentState.setByteField(value, this);
    }
    
    public void setShortField(short value) throws DeserializationException
    {
        this.data.currentState.setShortField(value, this);
    }

    public void setIntField(int value) throws DeserializationException
    {
        this.data.currentState.setIntField(value, this);
    }

    public void setLongField(long value) throws DeserializationException
    {
        this.data.currentState.setLongField(value, this);
    }
    
    public void setFloatField(float value) throws DeserializationException
    {
        this.data.currentState.setFloatField(value, this);
    }
    
    public void setDoubleField(double value) throws DeserializationException
    {
        this.data.currentState.setDoubleField(value, this);
    }
    
    public void setBooleanField(boolean value) throws DeserializationException
    {
        this.data.currentState.setBooleanField(value, this);
    }

    public void setCharField(char value) throws DeserializationException
    {
        this.data.currentState.setCharField(value, this);
    }

    public void setByteArrayField(byte[] value) throws DeserializationException
    {
        this.data.currentState.setByteArrayField(value, this);
    }

    public void setStringField(String value) throws DeserializationException
    {
        this.data.currentState.setStringField(value, this);
    }
    
    public void setBigDecimalField(BigDecimal value) throws DeserializationException
    {
        this.data.currentState.setBigDecimalField(value, this);
    }
    
    public void setDateField(Date value) throws DeserializationException
    {
        this.data.currentState.setDateField(value, this);
    }
    
    public void setTimestampField(Timestamp value) throws DeserializationException
    {
        this.data.currentState.setTimestampField(value, this);
    }
    
    public void setTimeField(Time value) throws DeserializationException
    {
        this.data.currentState.setTimeField(value, this);
    }
    
    public void setFieldOrRelationshipNull() throws DeserializationException
    {
        this.data.currentState.setFieldOrRelationshipNull(this);
    }

    public void startListElements() throws DeserializationException
    {
        this.data.currentState.startListElements(this);
    }

    public void endListElements() throws DeserializationException
    {
        this.data.currentState.endListElements(this);
    }

    /**
     * valid values are:
     *
     * ReladomoSerializationContext.READ_ONLY_STATE
     * ReladomoSerializationContext.DETACHED_STATE
     * ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE
     * ReladomoSerializationContext.IN_MEMORY_STATE
     *
     * In the final stage of deserialization, the database is queried to determine how the object should
     * behave after deserialization.
     *
     * If this method is not called, the object will be automatically deserialized in the following states:
     * - detached: if the pk specified exists in the database and the object is transactional.
     * - read-only: if the pk specified exists in the database and the object is read-only.
     * - in-memory: if the pk specified does not exist in the database.
     *
     * If the method is called, then following behavior is enforced:
     *
     * ReladomoSerializationContext.READ_ONLY_STATE: object must be read-only and must exist in the database
     * ReladomoSerializationContext.DETACHED_STATE: object must be transactional and must exist in the database
     * ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE: object must be transactional and must exist in the database
     * ReladomoSerializationContext.IN_MEMORY_STATE: nothing is enforced. If this object is later inserted, application must ensure no duplicate exists.
     *
     * @param objectState
     * @throws DeserializationException
     */
    public void setReladomoObjectState(int objectState) throws DeserializationException
    {
        this.data.currentState.setReladomoObjectState(objectState, this);
    }

    protected void addSingleObjectToResolve() throws DeserializationException
    {
        AsOfAttribute businessDateAttribute = this.data.metaData.getBusinessDateAttribute();
        if (businessDateAttribute != null && this.data.partial.businessDate == null && businessDateAttribute.getDefaultDate() == null)
        {
            throw new DeserializationException("Business date must be supplied for "+this.data.partial.dataObject.zGetPrintablePrimaryKey());
        }
        DeserializationClassMetaData key = this.data.metaData;
        List<PartialDeserialized> partialDeserializeds = objectsToResolve.get(key);
        if (partialDeserializeds == null)
        {
            partialDeserializeds = FastList.newList();
            objectsToResolve.put(this.data.metaData, partialDeserializeds);
        }
        partialDeserializeds.add(this.data.partial);
    }

    public Serialized<T> getDeserializedResult() throws DeserializationException
    {
        return new Serialized<T>(this);
    }

    public SerializedList getDeserializedResultAsList() throws DeserializationException
    {
        return new SerializedList(this);
    }

    public T getDeserializationResultAsObject()
    {
        try
        {
            if (this.data.metaData.isConfigured())
            {
                return findAndDeserializeSingleObject();
            }
            else
            {
                return deserializeSingleObjectAsUnconfigured();
            }
        }
        catch (DeserializationException e)
        {
            throw new RuntimeException("Could not deserialize", e);
        }
    }

    public List<T> getDeserializationResultAsList()
    {
        try
        {
            if (this.data.metaData.isConfigured())
            {
                return findAndDeserializeList();
            }
            else
            {
                return deserializeListAsUnconfigured();
            }
        }
        catch (DeserializationException e)
        {
            throw new RuntimeException("Could not deserialize", e);
        }
    }

    private List<T> deserializeListAsUnconfigured()
    {
        throw new RuntimeException("not implemented yet");
    }

    protected T deserializeSingleObjectAsUnconfigured() throws DeserializationException
    {

        for (Map.Entry<DeserializationClassMetaData, List<PartialDeserialized>> entry : objectsToResolve.entrySet())
        {
            List<PartialDeserialized> partialDeserializeds = entry.getValue();
            for(int i=0;i<partialDeserializeds.size();i++)
            {
                handleInMemoryObject(partialDeserializeds.get(i), entry.getKey());
            }
        }
        wireRelationshipsAsUnconfigured();
        return (T) this.data.partial.deserialized;
    }

    protected void wireRelationshipsAsUnconfigured()
    {
        throw new RuntimeException("not implemented yet");
    }

    protected void checkSingleObjectDeserialized() throws DeserializationException
    {
        if (this.data.partial == null)
        {
            String message = "No object was deserialized!";
            if (this.data.list != null)
            {
                message = "Looks like we deserialized a list, not an object!";
            }
            throw new DeserializationException(message);
        }
    }

    protected void checkListObjectDeserialized() throws DeserializationException
    {
        if (this.data.list == null)
        {
            String message = "No list was deserialized!";
            if (this.data.partial != null)
            {
                message = "Looks like we deserialized a single object not a list!";
            }
            throw new DeserializationException(message);
        }
    }

    protected List<T> findAndDeserializeList() throws DeserializationException
    {
        resolveAndFetchAllObjects();
        MithraList mithraList = this.data.metaData.getRelatedFinder().constructEmptyList();
        for(int i=0;i<this.data.list.size();i++)
        {
            mithraList.add(this.data.list.get(i).deserialized);
        }
        return mithraList;
    }

    protected T findAndDeserializeSingleObject() throws DeserializationException
    {
        resolveAndFetchAllObjects();
        return (T) this.data.partial.deserialized;
    }

    protected void resolveAndFetchAllObjects() throws DeserializationException
    {
        for (Map.Entry<DeserializationClassMetaData, List<PartialDeserialized>> entry : objectsToResolve.entrySet())
        {
            resolveList(entry.getKey(), entry.getValue());
        }
        wireRelationships();
    }

    protected void wireRelationships() throws DeserializationException
    {
        for (Map.Entry<DeserializationClassMetaData, List<PartialDeserialized>> entry : objectsToResolve.entrySet())
        {
            wireRelationshipsForList(entry.getKey(), entry.getValue());
        }
    }

    protected void wireRelationshipsForList(DeserializationClassMetaData metaData, List<PartialDeserialized> list) throws DeserializationException
    {
        if (list.size() > 0)
        {
            List<PartialDeserialized> detached = FastList.newList(list.size());
            for (int i = 0; i < list.size(); i++)
            {
                PartialDeserialized partialDeserialized = list.get(i);
                switch (partialDeserialized.deserializedState)
                {
                    case ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE:
                    case ReladomoSerializationContext.READ_ONLY_STATE:
                        //do nothing for these
                        break;
                    case ReladomoSerializationContext.IN_MEMORY_STATE:
                        wireRelationshipsForInMemory(metaData, partialDeserialized);
                        break;
                    case ReladomoSerializationContext.DETACHED_STATE:
                        if (partialDeserialized.partialRelationships != EMPTY_MAP)
                        {
                            detached.add(partialDeserialized);
                        }
                        break;
                }
            }
            if (!detached.isEmpty())
            {
                wireRelationshipsForDetached(metaData, detached);
            }
        }
    }

    protected void wireRelationshipsForInMemory(DeserializationClassMetaData metaData, PartialDeserialized partialDeserialized) throws DeserializationException
    {
        for (String name : partialDeserialized.partialRelationships.keySet())
        {
            RelatedFinder relationshipFinderByName = metaData.getRelationshipFinderByName(name);
            try
            {
                if (((AbstractRelatedFinder) relationshipFinderByName).isToOne())
                {
                    Object o = partialDeserialized.partialRelationships.get(name);
                    setToOneRelationship(name, metaData, partialDeserialized, decodeToOne(name, partialDeserialized, o));
                }
                else
                {
                    setToManyRelationship(name, metaData, partialDeserialized);
                }
            }
            catch (Exception e)
            {
                throw new DeserializationException("Could not set to-one relationship on " + partialDeserialized.dataObject.zGetPrintablePrimaryKey() + " for " + name);
            }
        }
    }

    protected void wireRelationshipsForDetached(DeserializationClassMetaData metaData, List<PartialDeserialized> detachedList) throws DeserializationException
    {
        Set<String> dependents = UnifiedSet.newSet();
        Set<String> dependentRelationshipNames = metaData.getDependentRelationshipNames();
        for(int i=0;i<detachedList.size();i++)
        {
            Set<String> names = detachedList.get(i).partialRelationships.keySet();
            for(String name: names)
            {
                if (dependentRelationshipNames.contains(name))
                {
                    dependents.add(name);
                }
            }
        }
        for(String name: dependents)
        {
            filterAndWireDetachedDependentRelationship(name, metaData, detachedList);
        }
        wireNonDependenRelationshipsForDetached(metaData, detachedList);
    }

    protected void wireNonDependenRelationshipsForDetached(DeserializationClassMetaData metaData, List<PartialDeserialized> detachedList) throws DeserializationException
    {
        Set<String> dependentRelationshipNames = metaData.getDependentRelationshipNames();
        for(int i=0;i<detachedList.size();i++)
        {
            PartialDeserialized partialDeserialized = detachedList.get(i);
            for (String name : partialDeserialized.partialRelationships.keySet())
            {
                if (!dependentRelationshipNames.contains(name))
                {
                    RelatedFinder relationshipFinderByName = metaData.getRelationshipFinderByName(name);
                    if (((AbstractRelatedFinder) relationshipFinderByName).isToOne())
                    {
                        Object o = partialDeserialized.partialRelationships.get(name);
                        try
                        {
                            setToOneRelationship(name, metaData, partialDeserialized, decodeToOne(name, partialDeserialized, o));
                        }
                        catch (Exception e)
                        {
                            throw new DeserializationException("Could not set to-one relationship on "+partialDeserialized.dataObject.zGetPrintablePrimaryKey()+" for "+name);
                        }
                    }
                }
            }

        }
    }

    protected void filterAndWireDetachedDependentRelationship(String name, DeserializationClassMetaData metaData, List<PartialDeserialized> detachedList) throws DeserializationException
    {
        List<PartialDeserialized> withRel = FastList.newList(detachedList.size());
        for(int i=0;i<detachedList.size();i++)
        {
            PartialDeserialized partialDeserialized = detachedList.get(i);
            if (partialDeserialized.partialRelationships.containsKey(name))
            {
                withRel.add(partialDeserialized);
            }
        }
        wireDependentRelationship(name, metaData, withRel);
    }

    protected void wireDependentRelationship(String name, DeserializationClassMetaData metaData, List<PartialDeserialized> withRel) throws DeserializationException
    {
        MithraList forDeepFetch = metaData.getRelatedFinder().constructEmptyList();
        for(int i=0;i<withRel.size();i++)
        {
            forDeepFetch.add(withRel.get(i).deserialized);
        }
        RelatedFinder related = metaData.getRelationshipFinderByName(name);
        forDeepFetch.deepFetch((Navigation) related);
        forDeepFetch.forceResolve();
        AbstractRelatedFinder abstractRelatedFinder = (AbstractRelatedFinder) related;
        try
        {
            if (abstractRelatedFinder.isToOne())
            {
                for(int i=0;i<withRel.size();i++)
                {
                    PartialDeserialized partialDeserialized = withRel.get(i);
                    Object o = partialDeserialized.partialRelationships.get(name);
                    setToOneRelationship(name, metaData, partialDeserialized, decodeToOne(name, partialDeserialized, o));
                }
            }
            else
            {
                for(int i=0;i<withRel.size();i++)
                {
                    PartialDeserialized partialDeserialized = withRel.get(i);
                    setToManyRelationship(name, metaData, partialDeserialized);
                }
            }
        }
        catch (Exception e)
        {
            throw new DeserializationException("Could not set related object on for relationship "+name, e);
        }
    }

    protected void setToManyRelationship(String name, DeserializationClassMetaData metaData, PartialDeserialized partialDeserialized) throws IllegalAccessException, InvocationTargetException
    {
        RelatedFinder related = metaData.getRelationshipFinderByName(name);
        AbstractRelatedFinder abstractRelatedFinder = (AbstractRelatedFinder) related;
        Object o = partialDeserialized.partialRelationships.get(name);
        MithraList relatedList = abstractRelatedFinder.constructEmptyList();
        if (o == null)
        {
            //do nothing
        }
        else if (o instanceof ReladomoDeserializer.PartialDeserialized)
        {
            filterObjectForToManyRelationship(relatedList, ((PartialDeserialized)o));
        }
        else // a List<PartialDeserialzied)
        {
            for(PartialDeserialized p: (List<PartialDeserialized>) o)
            {
                filterObjectForToManyRelationship(relatedList, p);
            }
            List existing = (List) abstractRelatedFinder.valueOf(partialDeserialized.deserialized);
            reconcileToMany(abstractRelatedFinder, existing, relatedList);
        }
        metaData.getRelationshipSetter(name).invoke(partialDeserialized.deserialized, relatedList);
    }

    private void reconcileToMany(AbstractRelatedFinder abstractRelatedFinder, List existing, MithraList relatedList)
    {
        if (existing.isEmpty() || relatedList.isEmpty())
        {
            return;
        }
        FullUniqueIndex index = new FullUniqueIndex(abstractRelatedFinder.getPrimaryKeyAttributes(), existing.size());
        index.addAll(existing);
        for(int i=0;i<relatedList.size();i++)
        {
            Object newDetached = relatedList.get(i);

            MithraTransactionalObject existingDetached = (MithraTransactionalObject) index.getFromData(newDetached);
            if (existingDetached != null)
            {
                existingDetached.copyNonPrimaryKeyAttributesFrom((MithraTransactionalObject) newDetached);
                relatedList.set(i, existingDetached);
            }
        }
    }

    protected void filterObjectForToManyRelationship(MithraList relatedList, PartialDeserialized relatedDeserialized)
    {
        if (relatedDeserialized.deserializedState != ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE)
        {
            relatedList.add(relatedDeserialized.deserialized);
        }
    }

    protected PartialDeserialized decodeToOne(String name, PartialDeserialized partialDeserialized, Object o) throws DeserializationException
    {
        if (o == null)
        {
            return null;
        }
        else if (o instanceof ReladomoDeserializer.PartialDeserialized)
        {
            return (PartialDeserialized) o;
        }
        else // a List<PartialDeserialzied)
        {
            List<PartialDeserialized> list = (List<PartialDeserialized>) o;
            if (list.size() == 0)
            {
                return null;
            }
            if (list.size() == 1)
            {
                return list.get(0);
            }
            else
            {
                throw new DeserializationException("Cannot set a list for a to-one relationship in object "+
                        partialDeserialized.dataObject.zGetPrintablePrimaryKey()+" and relationship "+name);
            }
        }
    }

    protected void setToOneRelationship(String name, DeserializationClassMetaData metaData, PartialDeserialized partialDeserialized, PartialDeserialized toSet) throws IllegalAccessException, InvocationTargetException
    {
        if (toSet == null || toSet.deserializedState == ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE)
        {
            metaData.getRelationshipSetter(name).invoke(partialDeserialized.deserialized, SINGLE_NULL);
        }
        else
        {
            metaData.getRelationshipSetter(name).invoke(partialDeserialized.deserialized, toSet.deserialized);
        }
    }

    protected List<List<PartialDeserialized>> segregateByAsOfAndSourceAttribute(DeserializationClassMetaData metaData, List<PartialDeserialized> dbLookUp)
    {
        if (dbLookUp.size() == 1)
        {
            return ListFactory.create(dbLookUp);
        }
        PartialDeserializedSegregationHashingStrategy hashStrategy = new PartialDeserializedSegregationHashingStrategy(metaData.getSourceAttribute(),
                metaData.getProcessingDateAttribute(), metaData.getBusinessDateAttribute());
        //todo: optimize this for non-repeated asOfAttributes
        MultiHashMap map = new MultiHashMap();
        for (int i = 0; i < dbLookUp.size(); i++)
        {
            Object o = dbLookUp.get(i);
            map.put(new KeyWithHashStrategy(o, hashStrategy), o);
        }

        if (map.size() > 1)
        {
            return map.valuesAsList();
        }
        else
        {
            return ListFactory.create(dbLookUp);
        }
    }

    protected void resolveList(DeserializationClassMetaData metaData, List<PartialDeserialized> listToResolve) throws DeserializationException
    {
        List<PartialDeserialized> dbLookUp = FastList.newList(listToResolve.size());
        for(PartialDeserialized partDes: listToResolve)
        {
            switch (partDes.state)
            {
                case 0: // not set
                case ReladomoSerializationContext.READ_ONLY_STATE:
                case ReladomoSerializationContext.DETACHED_STATE:
                case ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE:
                    dbLookUp.add(partDes);
                    break;
                case ReladomoSerializationContext.IN_MEMORY_STATE:
                    handleInMemoryObject(partDes, metaData);
                    break;
            }
        }

        if (!dbLookUp.isEmpty())
        {
            RelatedFinder finder = metaData.getRelatedFinder();
            Attribute[] pkAttributesNoSource = metaData.getPrimaryKeyAttributesWithoutSource();
            Attribute sourceAttribute = metaData.getSourceAttribute();
            AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
            if (asOfAttributes != null || sourceAttribute != null)
            {
                List<List<PartialDeserialized>> segregated = segregateByAsOfAndSourceAttribute(metaData, dbLookUp);
                for (int i = 0; i < segregated.size(); i++)
                {
                    List<PartialDeserialized> list = segregated.get(i);
                    Operation segOp = NoOperation.instance();
                    PartialDeserialized first = list.get(0);
                    if (sourceAttribute != null)
                    {
                        if (!first.isAttributeSet(sourceAttribute, metaData))
                        {
                            handleUnresolvableObjects(list, metaData);
                            break; // can't be resolved.
                        }
                        segOp = segOp.and(sourceAttribute.nonPrimitiveEq(sourceAttribute.valueOf(first.dataObject)));
                    }
                    if (metaData.getBusinessDateAttribute() != null && first.businessDate != null)
                    {
                        segOp = segOp.and(metaData.getBusinessDateAttribute().eq(first.businessDate));
                    }
                    if (metaData.getProcessingDateAttribute() != null && first.processingDate != null)
                    {
                        segOp = segOp.and(metaData.getProcessingDateAttribute().eq(first.processingDate));
                    }
                    forceRefresh(metaData, list, pkAttributesNoSource, segOp);
                }
            }
            else
            {
                forceRefresh(metaData, dbLookUp, pkAttributesNoSource, NoOperation.instance());
            }
        }
    }

    protected void handleUnresolvableObjects(List<PartialDeserialized> segregated, DeserializationClassMetaData metaData) throws DeserializationException
    {
        for(PartialDeserialized partDes: segregated)
        {
            switch (partDes.state)
            {
                case 0: // not set
                    handleInMemoryObject(partDes, metaData);
                    break;
                case ReladomoSerializationContext.READ_ONLY_STATE:
                case ReladomoSerializationContext.DETACHED_STATE:
                case ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE:
                    throw new DeserializationException("SourceAttribute missing in serial stream for "+partDes.dataObject.zGetPrintablePrimaryKey());
            }
        }

    }

    protected void handleInMemoryObject(PartialDeserialized partDes, DeserializationClassMetaData metaData) throws DeserializationException
    {
        MithraObject inMemory = constructObjectAndSetAttributes(partDes, metaData);
        partDes.setDeserialized(inMemory, ReladomoSerializationContext.IN_MEMORY_STATE);
    }

    protected void forceRefresh(DeserializationClassMetaData metaData, List<PartialDeserialized> listToRefresh, Attribute[] pkAttributes, Operation extraOp) throws DeserializationException
    {
        List<MithraDataObject> unwrapped = FastList.newList(listToRefresh.size());
        for (int i = 0; i < listToRefresh.size(); i++)
        {
            PartialDeserialized partial = listToRefresh.get(i);
            if (arePkAttributesSetWithNullableCheck(metaData, pkAttributes, partial))
            {
                unwrapped.add(partial.dataObject);
            }
        }
        Operation op = extraOp;
        if (pkAttributes.length == 1)
        {
            op = op.and(constructSinglePkOp(unwrapped, pkAttributes[0]));
        }
        else
        {
            op = op.and(constructMultiPkOp(unwrapped, pkAttributes));
        }
        MithraList many = metaData.getRelatedFinder().findMany(op);
        FullUniqueIndex index = new FullUniqueIndex(pkAttributes, many.size());
        index.addAll(many);

        for (int i = 0; i < listToRefresh.size(); i++)
        {
            PartialDeserialized partial = listToRefresh.get(i);
            if (arePkAttributesSet(metaData, pkAttributes, partial))
            {
                MithraObject fromDb = (MithraObject) index.getFromData(partial.dataObject);
                if (fromDb != null)
                {
                    handleFoundObject(fromDb, partial, metaData);
                }
                else
                {
                    handleNotFoundObject(partial, metaData);
                }
            }
        }
    }

    protected void handleNotFoundObject(PartialDeserialized partial, DeserializationClassMetaData metaData) throws DeserializationException
    {
        MithraObject inMemory;
        switch (partial.state)
        {
            case 0: // not set
                inMemory = constructObjectAndSetAttributes(partial, metaData);
                partial.setDeserialized(inMemory, ReladomoSerializationContext.IN_MEMORY_STATE);
                break;
            case ReladomoSerializationContext.READ_ONLY_STATE:
            case ReladomoSerializationContext.DETACHED_STATE:
                throw new DeserializationException("Object "+partial.dataObject.zGetPrintablePrimaryKey()+" doesn't exist in the db");
            case ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE:
                inMemory = constructObjectAndSetAttributes(partial, metaData);
                inMemory.zSetNonTxPersistenceState(PersistedState.DELETED);
                partial.setDeserialized(inMemory, ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE);
                break;
            default:
                throw new DeserializationException("should not get here!");
        }
    }

    protected MithraObject constructObjectAndSetAttributes(PartialDeserialized partial, DeserializationClassMetaData metaData) throws DeserializationException
    {
        MithraObject obj = metaData.constructObject(partial.businessDate, partial.processingDate);
        setAttributesAndMethods(obj, partial, metaData);
        return obj;
    }

    protected void setAttributesAndMethods(MithraObject obj, PartialDeserialized partial, DeserializationClassMetaData metaData)
    {
        List<Attribute> settableAttributes = metaData.getSettableAttributes();
        for(int i=0;i<settableAttributes.size();i++)
        {
            Attribute attr = settableAttributes.get(i);
            if (partial.isAttributeSet(attr, metaData))
            {
                attr.setValue(obj, attr.valueOf(partial.dataObject));
            }
        }
        //todo: deserializable methods
    }

    protected void handleFoundObject(MithraObject fromDb, PartialDeserialized partial, DeserializationClassMetaData metaData) throws DeserializationException
    {
        MithraTransactionalObject detachedCopy;
        switch (partial.state)
        {
            case 0: // not set
                if (metaData.getRelatedFinder().getMithraObjectPortal().isTransactional())
                {
                    detachedCopy = ((MithraTransactionalObject) fromDb).getDetachedCopy();
                    setAttributesAndMethods(detachedCopy, partial, metaData);
                    partial.setDeserialized(detachedCopy, ReladomoSerializationContext.DETACHED_STATE);
                }
                else
                {
                    partial.setDeserialized(fromDb, ReladomoSerializationContext.READ_ONLY_STATE);
                }
                break;
            case ReladomoSerializationContext.READ_ONLY_STATE:
                partial.setDeserialized(fromDb, ReladomoSerializationContext.READ_ONLY_STATE);
                break;
            case ReladomoSerializationContext.DETACHED_STATE:
                detachedCopy = ((MithraTransactionalObject) fromDb).getDetachedCopy();
                setAttributesAndMethods(detachedCopy, partial, metaData);
                partial.setDeserialized(detachedCopy, ReladomoSerializationContext.DETACHED_STATE);
                break;
            case ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE:
                detachedCopy = ((MithraTransactionalObject) fromDb).getDetachedCopy();
                if (metaData.getRelatedFinder().getAsOfAttributes() != null)
                {
                    ((MithraDatedTransactionalObject)detachedCopy).terminate();
                }
                else
                {
                    detachedCopy.delete();
                }
                partial.setDeserialized(detachedCopy, ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE);
                break;
            default:
                throw new DeserializationException("should not get here!");
        }
    }

    protected boolean arePkAttributesSetWithNullableCheck(DeserializationClassMetaData metaData, Attribute[] pkAttributes, PartialDeserialized partial)
    {
        for (Attribute a : pkAttributes)
        {
            if (!partial.isAttributeSet(a, metaData))
            {
                if (a.getMetaData().isNullable())
                {
                    a.setValueNull(partial.dataObject);
                    partial.markAttributeDone(a, metaData);
                }
                else
                {
                    return false;
                }
            }
        }
        return true;
    }

    protected boolean arePkAttributesSet(DeserializationClassMetaData metaData, Attribute[] pkAttributes, PartialDeserialized partial)
    {
        for (Attribute a : pkAttributes)
        {
            if (!partial.isAttributeSet(a, metaData))
            {
                return false;
            }
        }
        return true;
    }

    protected Operation constructMultiPkOp(List<MithraDataObject> listToRefresh, Attribute[] pkAttributes)
    {
        TupleAttribute ta = pkAttributes[0].tupleWith(pkAttributes[1]);
        for(int i=2;i<pkAttributes.length;i++)
        {
            ta = ta.tupleWith(pkAttributes[i]);
        }
        return ta.in(listToRefresh, pkAttributes);
    }

    protected Operation constructSinglePkOp(List<MithraDataObject> listToRefresh, Attribute pkAttribute)
    {
        return pkAttribute.in(listToRefresh, pkAttribute);
    }


    //    public MithraList<T> getDeserializationResultAsList()
//    {
//        resolveStuff();
//        //todo
//    }

    protected Object invokeStaticMethod(Class classToInvoke, String methodName) throws DeserializationException
    {
        try
        {
            Method method = ReflectionMethodCache.getZeroArgMethod(classToInvoke, methodName);
            return method.invoke(null, NULL_ARGS);
        }
        catch (Exception e)
        {
            throw new DeserializationException("Could not invoke method "+methodName+" on class "+classToInvoke, e);
        }
    }

    protected static class StackableData
    {
        protected DeserializationClassMetaData metaData;
        protected PartialDeserialized partial;
        protected List<PartialDeserialized> list;
        protected Attribute attribute;
        protected RelatedFinder related;
        protected Method method;
        protected State currentState;
    }

    protected static class PartialDeserialized
    {
        protected MithraDataObject dataObject;
        protected Timestamp businessDate;
        protected Timestamp processingDate;
        protected BitSet populatedAttributes;
        protected int state;
        protected Map<String, Object> partialRelationships = EMPTY_MAP; //Object is either PartialDeserialized or List<PartialDeserialized>
        protected byte deserializedState;
        protected MithraObject deserialized; // constructed at the end of the stream

        protected void storeRelated(RelatedFinder relatedFinder, Object related)
        {
            if (this.partialRelationships == EMPTY_MAP)
            {
                this.partialRelationships = UnifiedMap.newMap();
            }
            this.partialRelationships.put(((AbstractRelatedFinder)relatedFinder).getRelationshipName(), related);
        }


        public void markAttributeDone(Attribute attribute, DeserializationClassMetaData metaData)
        {
            if (populatedAttributes == null)
            {
                populatedAttributes = new BitSet(metaData.getTotalAttributes());
            }
            populatedAttributes.set(metaData.getAttriutePosition(attribute));
        }

        public boolean isAttributeSet(Attribute attr, DeserializationClassMetaData metaData)
        {
            return populatedAttributes != null && populatedAttributes.get(metaData.getAttriutePosition(attr));
        }

        protected void setDeserialized(MithraObject deserialized, byte state)
        {
            deserializedState = state;
            this.deserialized = deserialized;
        }
    }

    protected static class PartialDeserializedSegregationHashingStrategy implements HashStrategy
    {
        private Attribute sourceAttribute;
        private AsOfAttribute processingDateAttr;
        private AsOfAttribute businessDateAttr;

        public PartialDeserializedSegregationHashingStrategy(Attribute sourceAttribute, AsOfAttribute processingDateAttr, AsOfAttribute businessDateAttr)
        {
            this.sourceAttribute = sourceAttribute;
            this.processingDateAttr = processingDateAttr;
            this.businessDateAttr = businessDateAttr;
        }

        @Override
        public int computeHashCode(Object o)
        {
            PartialDeserialized partialDeserialized = (PartialDeserialized) o;
            int hash = HashUtil.NULL_HASH;
            if (sourceAttribute != null)
            {
                hash = HashUtil.combineHashes(hash, sourceAttribute.valueHashCode(partialDeserialized.dataObject));
            }
            if (businessDateAttr != null)
            {
                hash = HashUtil.combineHashes(hash, partialDeserialized.businessDate.hashCode());
            }
            if (processingDateAttr != null && partialDeserialized.processingDate != null)
            {
                hash = HashUtil.combineHashes(hash, partialDeserialized.processingDate.hashCode());
            }
            return hash;
        }

        @Override
        public boolean equals(Object o1, Object o2)
        {
            PartialDeserialized left = (PartialDeserialized) o1;
            PartialDeserialized right = (PartialDeserialized) o2;
            if (sourceAttribute != null && !sourceAttribute.valueEquals(left.dataObject, right.dataObject))
            {
                return false;
            }
            if (businessDateAttr != null && !left.businessDate.equals(right.businessDate))
            {
                return false;
            }
            if (processingDateAttr != null)
            {
                if (left.processingDate == right.processingDate) // covers both null
                {
                    return true;
                }
                if (left.processingDate != null)
                {
                    return left.processingDate.equals(right.processingDate) || (left.processingDate.equals(processingDateAttr.getDefaultDate()) && right.processingDate == null);
                }
                return right.processingDate.equals(left.processingDate) || (right.processingDate.equals(processingDateAttr.getDefaultDate()) && left.processingDate == null);
            }
            return true;
        }
    }

    protected static class StartStateNoMeta extends State
    {
        private static final StartStateNoMeta INSTANCE = new StartStateNoMeta();

        @Override
        public void startObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.startObject(deserializer);
        }

        @Override
        public void startObject(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = AwaitingMeta.INSTANCE;
        }
    }

    protected static class AwaitingMeta extends State
    {
        private static final AwaitingMeta INSTANCE = new AwaitingMeta();

        @Override
        public void storeNested(ReladomoDeserializer deserializer, PartialDeserialized toSave) throws DeserializationException
        {
            deserializer.data.partial = toSave;
            deserializer.data.currentState = EndState.INSTANCE;
        }

        @Override
        public void storeNestedList(ReladomoDeserializer deserializer, List<PartialDeserialized> toSave) throws DeserializationException
        {
            deserializer.data.list = toSave;
            deserializer.data.currentState = EndState.INSTANCE;
        }

        @Override
        public void storeReladomoClassName(ReladomoDeserializer deserializer, String className) throws DeserializationException
        {
            RelatedFinder relatedFinder = getFinderInstance(deserializer, className);
            deserializer.data.metaData = deserializer.findDeserializationMetaData(relatedFinder);
            StartStateHaveMeta.INSTANCE.startObject(deserializer);
        }
    }

    protected static RelatedFinder getFinderInstance(ReladomoDeserializer deserializer, String className) throws DeserializationException
    {
        //todo: polymorphic case; need to search up until we find a finder. Also store the classname
        return ReladomoClassMetaData.fromBusinessClassName(className).getFinderInstance();
    }

    protected static void checkClassNameConsistency(ReladomoDeserializer deserializer, String className) throws DeserializationException
    {
        RelatedFinder finderInstance = getFinderInstance(deserializer, className);
        if (!deserializer.data.metaData.getRelatedFinder().getFinderClassName().equals(finderInstance.getFinderClassName()))
        {
            throw new DeserializationException("inconsistent meta data. was expecting "+deserializer.data.metaData.getRelatedFinder().getFinderClassName()+"" +
                    " but got "+finderInstance.getFinderClassName());
        }
    }

    protected static class StartStateHaveMeta extends State
    {
        private static final StartStateHaveMeta INSTANCE = new StartStateHaveMeta();

        @Override
        public void storeReladomoClassName(ReladomoDeserializer deserializer, String className) throws DeserializationException
        {
            checkClassNameConsistency(deserializer, className);
        }

        @Override
        public void startObject(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.pushDataAndStartEmpty(deserializer.data.metaData);
            commonStartState(deserializer);
        }

        @Override
        public void storeNested(ReladomoDeserializer deserializer, PartialDeserialized toSave) throws DeserializationException
        {
            deserializer.data.partial = toSave;
            deserializer.data.currentState = EndState.INSTANCE;
        }

        @Override
        public void storeNestedList(ReladomoDeserializer deserializer, List<PartialDeserialized> toSave) throws DeserializationException
        {
            deserializer.data.list = toSave;
            deserializer.data.currentState = EndState.INSTANCE;
        }

        @Override
        public void startList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.pushDataAndStartEmpty(deserializer.data.metaData);
            deserializer.data.currentState = InListState.INSTANCE;
        }
    }

    protected static class EndState extends State
    {
        private static final EndState INSTANCE = new EndState();
    }

    protected static class InObjectState extends State
    {
        private static final InObjectState INSTANCE = new InObjectState();

        @Override
        public void storeReladomoClassName(ReladomoDeserializer deserializer, String className) throws DeserializationException
        {
            checkClassNameConsistency(deserializer, className);
        }

        @Override
        public void endObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.endObject(deserializer);
        }

        @Override
        public void endObject(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.addSingleObjectToResolve();
            StackableData toSave = deserializer.data;
            if (deserializer.popData())
            {
                deserializer.data.currentState.storeNested(deserializer, toSave.partial);
            }
        }

        @Override
        public void setReladomoObjectState(int objectState, ReladomoDeserializer deserializer) throws DeserializationException
        {
            if (objectState != ReladomoSerializationContext.DETACHED_STATE &&
                    objectState != ReladomoSerializationContext.IN_MEMORY_STATE &&
                    objectState != ReladomoSerializationContext.READ_ONLY_STATE &&
                    objectState != ReladomoSerializationContext.DELETED_OR_TERMINATED_STATE)
            {
                throw new DeserializationException("Unrecognized object state "+objectState);
            }
            deserializer.data.partial.state = objectState;
        }

        @Override
        public FieldOrRelation startFieldOrRelationship(String name, ReladomoDeserializer deserializer) throws DeserializationException
        {
            Attribute attribute = deserializer.data.metaData.getAttributeByName(name);
            if (attribute != null)
            {
                deserializer.data.attribute = attribute;
                deserializer.data.currentState = InAttributeState.INSTANCE;
                return FieldOrRelation.Attribute;
            }
            RelatedFinder relatedFinder = deserializer.data.metaData.getRelationshipFinderByName(name);
            if (relatedFinder != null)
            {
                deserializer.data.related = relatedFinder;
                if (((AbstractRelatedFinder)relatedFinder).isToOne())
                {
                    deserializer.data.currentState = InToOneRelationshipState.INSTANCE;
                    return FieldOrRelation.ToOneRelationship;
                }
                else
                {
                    deserializer.data.currentState = InToManyRelationshipState.INSTANCE;
                    return FieldOrRelation.ToManyRelationship;
                }
            }
            Method annotatedMethod = deserializer.data.metaData.getAnnotatedMethodByName(name);
            if (annotatedMethod != null)
            {
                deserializer.data.method = annotatedMethod;
                deserializer.data.currentState = InAnnotatedMethod.INSTANCE;
                return FieldOrRelation.AnnotatedMethod;
            }
            deserializer.data.currentState = deserializer.unknownState;
            return FieldOrRelation.Unknown;
        }

        @Override
        public Attribute startAttribute(String name, ReladomoDeserializer deserializer) throws DeserializationException
        {
            Attribute attribute = deserializer.data.metaData.getAttributeByName(name);
            if (attribute != null)
            {
                deserializer.data.attribute = attribute;
                deserializer.data.currentState = InAttributeState.INSTANCE;
            }
            else
            {
                deserializer.data.currentState = deserializer.unknownState;
            }
            return attribute;
        }

        @Override
        public FieldOrRelation startRelationship(String name, ReladomoDeserializer deserializer) throws DeserializationException
        {
            RelatedFinder relatedFinder = deserializer.data.metaData.getRelationshipFinderByName(name);
            if (relatedFinder != null)
            {
                deserializer.data.related = relatedFinder;
                if (((AbstractRelatedFinder)relatedFinder).isToOne())
                {
                    deserializer.data.currentState = InToOneRelationshipState.INSTANCE;
                    return FieldOrRelation.ToOneRelationship;
                }
                else
                {
                    deserializer.data.currentState = InToManyRelationshipState.INSTANCE;
                    return FieldOrRelation.ToManyRelationship;
                }
            }
            deserializer.data.currentState = deserializer.unknownState;
            return FieldOrRelation.Unknown;
        }
    }

    protected static class InUnknownField extends State
    {
        private static final InUnknownField INSTANCE = new InUnknownField();

        @Override
        public void skipCurrentFieldOrRelationship(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }
    }

    protected static class IgnoreEverything extends State
    {
        private int objectOrListCount = 0;
        private int listElementCount = 0;

        public IgnoreEverything(int objectOrListCount, int listElementCount)
        {
            this.objectOrListCount = objectOrListCount;
            this.listElementCount = listElementCount;
        }

        @Override
        public void startObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.objectOrListCount++;
        }

        @Override
        public void endObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.objectOrListCount--;
            possiblySetState(deserializer);
        }

        private void possiblySetState(ReladomoDeserializer deserializer)
        {
            if (objectOrListCount == 0 && listElementCount == 0)
            {
                deserializer.data.currentState = InObjectState.INSTANCE;
            }
        }

        @Override
        public void startObject(ReladomoDeserializer deserializer) throws DeserializationException
        {
            objectOrListCount++;
        }

        @Override
        public void endObject(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.objectOrListCount--;
            possiblySetState(deserializer);
        }

        @Override
        public void startList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            objectOrListCount++;
        }

        @Override
        public void endList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.objectOrListCount--;
            possiblySetState(deserializer);
        }

        @Override
        public void startListElements(ReladomoDeserializer deserializer) throws DeserializationException
        {
            listElementCount++;
        }

        @Override
        public void endListElements(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.listElementCount--;
            possiblySetState(deserializer);
        }

        @Override
        public FieldOrRelation startFieldOrRelationship(String name, ReladomoDeserializer deserializer) throws DeserializationException
        {
            return FieldOrRelation.Unknown;
        }

        @Override
        public Attribute startAttribute(String name, ReladomoDeserializer deserializer) throws DeserializationException
        {
            return null;
        }

        @Override
        public FieldOrRelation startRelationship(String name, ReladomoDeserializer deserializer) throws DeserializationException
        {
            return FieldOrRelation.Unknown;
        }

        @Override
        public void parseFieldFromString(String value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setFieldOrRelationshipNull(ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void skipCurrentFieldOrRelationship(ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setByteField(byte value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setShortField(short value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setIntField(int value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setLongField(long value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setFloatField(float value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setDoubleField(double value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setBooleanField(boolean value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setCharField(char value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setByteArrayField(byte[] value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setStringField(String value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setBigDecimalField(BigDecimal value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setDateField(Date value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setTimestampField(Timestamp value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }

        @Override
        public void setTimeField(Time value, ReladomoDeserializer deserializer) throws DeserializationException
        {
        }
    }

    protected static class IgnoreUnknownField extends State
    {
        private static final IgnoreUnknownField INSTANCE = new IgnoreUnknownField();

        @Override
        public void startObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = new IgnoreEverything(1, 0);
        }

        @Override
        public void startObject(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = new IgnoreEverything(1, 0);
        }

        @Override
        public void startList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = new IgnoreEverything(1, 0);
        }

        @Override
        public void startListElements(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = new IgnoreEverything(0, 1);
        }

        @Override
        public void parseFieldFromString(String value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setFieldOrRelationshipNull(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setReladomoObjectState(int objectState, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setByteField(byte value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setShortField(short value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setIntField(int value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setLongField(long value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setFloatField(float value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setDoubleField(double value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setBooleanField(boolean value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setCharField(char value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setByteArrayField(byte[] value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setStringField(String value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setBigDecimalField(BigDecimal value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setDateField(Date value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setTimestampField(Timestamp value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setTimeField(Time value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void skipCurrentFieldOrRelationship(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }
    }

    protected static class InToManyRelationshipState extends State
    {
        private static final InToManyRelationshipState INSTANCE = new InToManyRelationshipState();

        @Override
        public void startObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.startList(deserializer);
        }

        @Override
        public void startList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.pushDataAndStartEmpty(deserializer.data.related);
            deserializer.data.currentState = InListState.INSTANCE;
        }

        @Override
        public void storeNestedList(ReladomoDeserializer deserializer, List<PartialDeserialized> toSave) throws DeserializationException
        {
            deserializer.data.partial.storeRelated(deserializer.data.related, toSave);
            deserializer.data.currentState = InObjectState.INSTANCE;
        }
    }

    protected static class InListState extends State
    {
        private static final InListState INSTANCE = new InListState();

        @Override
        public void storeReladomoClassName(ReladomoDeserializer deserializer, String className) throws DeserializationException
        {
            checkClassNameConsistency(deserializer, className);
        }

        @Override
        public void startListElements(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.pushDataAndStartEmpty(deserializer.data.metaData);
            deserializer.data.currentState = InListElementState.INSTANCE;
            deserializer.data.list = FastList.newList();
        }

        @Override
        public void storeNestedList(ReladomoDeserializer deserializer, List<PartialDeserialized> toSave) throws DeserializationException
        {
            deserializer.data.list = toSave;
        }

        @Override
        public void endObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.endList(deserializer);
        }

        @Override
        public void endList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            List<PartialDeserialized> toSave = deserializer.data.list;
            deserializer.popData();
            deserializer.data.currentState.storeNestedList(deserializer, toSave);
        }
    }

    protected static class InListElementState extends State
    {
        private static final InListElementState INSTANCE = new InListElementState();

        @Override
        public void startObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.startObject(deserializer);
        }

        @Override
        public void startObject(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.pushDataAndStartEmpty(deserializer.data.metaData);
            commonStartState(deserializer);
        }

        @Override
        public void storeNested(ReladomoDeserializer deserializer, PartialDeserialized toSave) throws DeserializationException
        {
            deserializer.data.list.add(toSave);
        }

        @Override
        public void endListElements(ReladomoDeserializer deserializer) throws DeserializationException
        {
            List<PartialDeserialized> toSave = deserializer.data.list;
            deserializer.popData();
            deserializer.data.currentState.storeNestedList(deserializer, toSave);
        }
    }

    protected static class InToOneRelationshipState extends State
    {
        private static final InToOneRelationshipState INSTANCE = new InToOneRelationshipState();

        @Override
        public void startObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            this.startObject(deserializer);
        }

        @Override
        public void startObject(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.pushDataAndStartEmpty(deserializer.data.related);
            commonStartState(deserializer);
        }

        @Override
        public void storeNested(ReladomoDeserializer deserializer, PartialDeserialized toSave) throws DeserializationException
        {
            deserializer.data.partial.storeRelated(deserializer.data.related, toSave);
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void setFieldOrRelationshipNull(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.partial.storeRelated(deserializer.data.related, null);
            deserializer.data.currentState = InObjectState.INSTANCE;
        }
    }

    protected static class InAnnotatedMethod extends State
    {
        private static final InAnnotatedMethod INSTANCE = new InAnnotatedMethod();

        //todo
    }

    protected static class InAttributeState extends State
    {
        private static final InAttributeState INSTANCE = new InAttributeState();

        protected void attributeDone(ReladomoDeserializer deserializer)
        {
            deserializer.data.partial.markAttributeDone(deserializer.data.attribute, deserializer.data.metaData);
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        @Override
        public void parseFieldFromString(String value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            if (deserializer.data.attribute instanceof AsOfAttribute)
            {
                AsOfAttribute asOfAttribute = (AsOfAttribute) deserializer.data.attribute;
                Timestamp parsed = null;
                try
                {
                    parsed = new ImmutableTimestamp(deserializer.timestampFormat.parse(value).getTime());
                }
                catch (ParseException e)
                {
                    throw new DeserializationException("Could not parse " + value, e);
                }
                if (asOfAttribute.isProcessingDate())
                {
                    deserializer.data.partial.processingDate = parsed;
                }
                else
                {
                    deserializer.data.partial.businessDate = parsed;
                }
            }
            else
            {
                if (("null".equals(value) || value == null) && !(deserializer.data.attribute instanceof StringAttribute))
                {
                    deserializer.data.attribute.setValueNull(deserializer.data.partial.dataObject);
                }
                else
                {
                    try
                    {
                        deserializer.data.attribute.parseStringAndSet(value, deserializer.data.partial.dataObject, 0, deserializer.timestampFormat);
                    }
                    catch (ParseException e)
                    {
                        throw new DeserializationException("Could not parse " + value, e);
                    }
                }
            }
            attributeDone(deserializer);
        }

        @Override
        public void setFieldOrRelationshipNull(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.attribute.setValueNull(deserializer.data.partial.dataObject);
            attributeDone(deserializer);
        }

        @Override
        public void setByteField(byte value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((ByteAttribute)deserializer.data.attribute).setByteValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setShortField(short value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((ShortAttribute)deserializer.data.attribute).setShortValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setIntField(int value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((IntegerAttribute)deserializer.data.attribute).setIntValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setLongField(long value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((LongAttribute)deserializer.data.attribute).setLongValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setFloatField(float value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((FloatAttribute)deserializer.data.attribute).setFloatValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setDoubleField(double value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((DoubleAttribute)deserializer.data.attribute).setDoubleValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setBooleanField(boolean value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((BooleanAttribute)deserializer.data.attribute).setBooleanValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setCharField(char value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((CharAttribute)deserializer.data.attribute).setCharValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setByteArrayField(byte[] value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((ByteArrayAttribute)deserializer.data.attribute).setByteArrayValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setStringField(String value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((StringAttribute)deserializer.data.attribute).setStringValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setBigDecimalField(BigDecimal value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((BigDecimalAttribute)deserializer.data.attribute).setBigDecimalValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setDateField(Date value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((DateAttribute)deserializer.data.attribute).setDateValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void setTimestampField(Timestamp value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            if (deserializer.data.attribute instanceof AsOfAttribute)
            {
                AsOfAttribute asOfAttribute = (AsOfAttribute) deserializer.data.attribute;
                if (asOfAttribute.isProcessingDate())
                {
                    deserializer.data.partial.processingDate = value;
                }
                else
                {
                    deserializer.data.partial.businessDate = value;
                }
            }
            else
            {
                ((TimestampAttribute) deserializer.data.attribute).setTimestampValue(deserializer.data.partial.dataObject, value);
            }
            attributeDone(deserializer);
        }

        @Override
        public void setTimeField(Time value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            ((TimeAttribute)deserializer.data.attribute).setTimeValue(deserializer.data.partial.dataObject, value);
            attributeDone(deserializer);
        }

        @Override
        public void skipCurrentFieldOrRelationship(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.currentState = InObjectState.INSTANCE;
        }
    }
    
    protected static class State
    {
        protected void commonStartState(ReladomoDeserializer deserializer) throws DeserializationException
        {
            deserializer.data.partial = new PartialDeserialized();
            deserializer.data.partial.dataObject = deserializer.data.metaData.constructData();
            deserializer.data.currentState = InObjectState.INSTANCE;
        }

        public void startObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call startObjectOrList in "+this.getClass().getName());
        }

        public void endObjectOrList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call endObjectOrList in "+this.getClass().getName());
        }

        public void startObject(ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call startObject in "+this.getClass().getName());
        }

        public void endObject(ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call endObject in "+this.getClass().getName());
        }

        public void startList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call startList in "+this.getClass().getName());
        }

        public void endList(ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call endList in "+this.getClass().getName());
        }

        public void startListElements(ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call startListElements in "+this.getClass().getName());
        }

        public void endListElements(ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call endListElements in "+this.getClass().getName());
        }

        public FieldOrRelation startFieldOrRelationship(String name, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call startFieldOrRelationship '"+name+"' in "+this.getClass().getName());
        }

        public Attribute startAttribute(String name, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call startAttribute in "+this.getClass().getName());
        }

        public FieldOrRelation startRelationship(String name, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call startRelationship in "+this.getClass().getName());
        }

        public void parseFieldFromString(String value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call parseFieldFromString in "+this.getClass().getName() +" value '"+value+"'");
        }

        public void setFieldOrRelationshipNull(ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setFieldOrRelationshipNull in "+this.getClass().getName());
        }

        public void skipCurrentFieldOrRelationship(ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call skipCurrentFieldOrRelationship in "+this.getClass().getName());
        }

        public void setReladomoObjectState(int objectState, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setReladomoObjectState in "+this.getClass().getName());
        }

        public void setByteField(byte value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setByteField in "+this.getClass().getName());
        }

        public void setShortField(short value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setShortField in "+this.getClass().getName());
        }

        public void setIntField(int value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setIntField in "+this.getClass().getName());
        }

        public void setLongField(long value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setLongField in "+this.getClass().getName());
        }

        public void setFloatField(float value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setFloatField in "+this.getClass().getName());
        }

        public void setDoubleField(double value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setDoubleField in "+this.getClass().getName());
        }

        public void setBooleanField(boolean value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setBooleanField in "+this.getClass().getName());
        }

        public void setCharField(char value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setCharField in "+this.getClass().getName());
        }

        public void setByteArrayField(byte[] value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setByteArrayField in "+this.getClass().getName());
        }

        public void setStringField(String value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setStringField in "+this.getClass().getName());
        }

        public void setBigDecimalField(BigDecimal value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setBigDecimalField in "+this.getClass().getName());
        }

        public void setDateField(Date value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setDateField in "+this.getClass().getName());
        }

        public void setTimestampField(Timestamp value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setTimestampField in "+this.getClass().getName());
        }

        public void setTimeField(Time value, ReladomoDeserializer deserializer) throws DeserializationException
        {
            throw new DeserializationException("Should not call setTimeField in "+this.getClass().getName());
        }

        public void storeNested(ReladomoDeserializer deserializer, PartialDeserialized toSave) throws DeserializationException
        {
            throw new DeserializationException("Should not call storeNested in "+this.getClass().getName());
        }

        public void storeNestedList(ReladomoDeserializer deserializer, List<PartialDeserialized> toSave) throws DeserializationException
        {
            throw new DeserializationException("Should not call storeNestedList in "+this.getClass().getName());
        }

        public void storeReladomoClassName(ReladomoDeserializer deserializer, String className) throws DeserializationException
        {
            throw new DeserializationException("Should not call storeReladomoClassName in "+this.getClass().getName());
        }
    }

}
