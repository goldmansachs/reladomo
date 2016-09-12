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

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SourceAttributeType;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.extractor.NormalAndListValueSelector;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationClassLevelNotificationListener;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Set;


public abstract class AbstractRelatedFinder<ReturnType, ParentOwnerType, ReturnOrRetunListType, ReturnListType extends List, OwnerType> implements RelatedFinder<ReturnType>, Serializable
{
    private static Logger logger = LoggerFactory.getLogger(DeepRelationshipUtility.class.getName());

    protected static final byte COMPLEX_TO_ONE = 5;
    protected static final byte COMPLEX_TO_MANY = 7;
    protected static final byte SIMPLE_TO_ONE = 10;
    protected static final byte SIMPLE_TO_MANY = 20;

    protected Mapper mapper;
    protected AbstractRelatedFinder _parentSelector;
    protected OrderBy _orderBy;
    protected byte _type;
    protected String _name;
    protected transient DeepFetchStrategy deepFetchStrategy;
    protected transient RelationshipMultiExtractor relationshipMultiExtractor;

    public AbstractRelatedFinder(Mapper mapper)
    {
        this.mapper = mapper;
    }

    public AbstractRelatedFinder()
    {
    }

    private DeepFetchStrategy getDeepFetchStrategy()
    {
        DeepFetchStrategy strategy = this.deepFetchStrategy;
        if (strategy == null)
        {
            if (this._type == SIMPLE_TO_ONE)
            {
                strategy = new SimpleToOneDeepFetchStrategy(this.mapper, this._orderBy);
            }
            else if (this._type == SIMPLE_TO_MANY)
            {
                strategy = new SimpleToManyDeepFetchStrategy(this.mapper, this._orderBy);
            }
            else if (this._type == COMPLEX_TO_ONE || this._type == COMPLEX_TO_MANY)
            {
                strategy = new ChainedDeepFetchStrategy(this.mapper, this._orderBy);
            }
            this.deepFetchStrategy = strategy;
        }
        return strategy;
    }

    public Mapper zGetMapper()
    {
        return this.mapper;
    }

    public String getRelationshipPath()
    {
        return mapper.getRelationshipPath();
    }

    public List<String> getRelationshipPathAsList()
    {
        return mapper.getRelationshipPathAsList();
    }

    public void setParentDeepRelationshipAttribute(DeepRelationshipAttribute parent)
    {
         this._parentSelector = (AbstractRelatedFinder) parent;
    }

    public DeepRelationshipAttribute getParentDeepRelationshipAttribute()
    {
        return (DeepRelationshipAttribute) this._parentSelector;
    }

    public static SourceAttributeType zGetSourceAttributeType()
    {
        return null;
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return null;
    }

    protected NormalAndListValueSelector zGetValueSelector()
    {
        return null;
    }

    public Operation all()
    {
        return new All(this.getPrimaryKeyAttributes()[0]);
    }

    public Operation exists()
    {
        return new MappedOperation(this.mapper, this.all());
    }

    public Operation notExists()                              
    {
        return this.mapper.createNotExistsOperation(this.all());
    }

    public Operation notExists(Operation op)
    {
        return this.mapper.createNotExistsOperation(op);
    }

    /**
     * This method is the boolean negation of exists. plain notExists is often not the boolean negation of notExists
     * @return
     */
    public Operation recursiveNotExists()
    {
        return this.mapper.createRecursiveNotExistsOperation(this.all());
    }

    public Operation recursiveNotExists(Operation op)
    {
        return this.mapper.createRecursiveNotExistsOperation(op);
    }

    public SourceAttributeType getSourceAttributeType()
    {
        return null;
    }

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o.getClass() != this.getClass())
        {
            return false;
        }
        final AbstractRelatedFinder relatedFinder = (AbstractRelatedFinder) o;
        return  (mapper != null ? mapper.equals(relatedFinder.mapper) : relatedFinder.mapper == null);
    }

    public int hashCode()
    {
        return (mapper != null ? mapper.hashCode() : this.getClass().hashCode());
    }

    public void forEach(final ObjectProcedure proc, ParentOwnerType _obj, Object context)
    {
        if (this._parentSelector != null)
        {
            this._parentSelector.forEach(new ObjectProcedure()
            {
                public boolean execute(Object object, Object innerContext)
                {
                    plainForEach(proc, (OwnerType) object, innerContext);
                    return true;
                }
            }, _obj, context);
        }
        else
        {
            plainForEach(proc, (OwnerType) _obj, context);
        }
    }

    private void plainForEach(final ObjectProcedure proc, OwnerType _obj, Object context)
    {
        Object relatedObjOrList = this.plainValueOf(_obj);
        if (this._type == SIMPLE_TO_MANY || this._type == COMPLEX_TO_MANY)
        {
            List list = (List) relatedObjOrList;
            int size = list.size();
            for (int i = 0; i < size; i++)
            {
                if (!proc.execute(list.get(i), context))
                {
                    break;
                }
            }
        }
        else
        {
            proc.execute(relatedObjOrList, context);
        }
    }

    public boolean isModifiedSinceDetachment(MithraTransactionalObject _obj)
    {
       return false;
    }

    public ReturnOrRetunListType valueOf(ParentOwnerType o)
    {
        Object value = o;
        if (this._parentSelector != null)
        {
            value = this._parentSelector.valueOf(o);
        }
        if (value == null) return null;
        return value instanceof MithraList ? (ReturnOrRetunListType) plainListValueOf(value) : plainValueOf((OwnerType)value);
    }

    protected ReturnOrRetunListType plainValueOf(OwnerType o)
    {
        // subclass to override
        return null;
    }

    public List listValueOf(Object obj)
    {
        if (this._parentSelector != null)
        {
            obj = this._parentSelector.listValueOf(obj);
        }
        return obj == null ? null : plainListValueOf(obj);
    }

    protected ReturnListType plainListValueOf(Object obj)
    {
        return null;  //subclass to override
    }

    protected boolean isSimple()
    {
        return (this._type == SIMPLE_TO_MANY || this._type == SIMPLE_TO_ONE) && (_parentSelector == null ||
            _parentSelector.isSimple());
    }

    public abstract MithraObjectPortal getMithraObjectPortal();

    protected boolean isSimpleToOne()
    {
        return this._type == SIMPLE_TO_ONE;
    }

    protected boolean isSimpleToMany()
    {
        return this._type == SIMPLE_TO_MANY;
    }

    protected static Logger getLogger()
    {
        return logger;
    }

    public MithraList findManyWithMapper(Operation parentOp)
    {
        Operation result = this.mapper.createMappedOperationForDeepFetch(parentOp);
        return result.getResultObjectPortal().getFinder().findMany(result);
    }

    public String getAssociationName()
    {
        return this._name;
    }

    public String getRelationshipName()
    {
        return this._name;
    }

    public boolean isToOne()
    {
        return this._type == SIMPLE_TO_ONE || this._type == COMPLEX_TO_ONE;
    }

    public List zDeepFetch(DeepFetchNode node, boolean bypassCache, boolean forceImplicitJoin)
    {
        DeepFetchStrategy strategy = this.getDeepFetchStrategy();
        if (strategy != null)
        {
            return strategy.deepFetch(node, bypassCache, forceImplicitJoin);
        }
        return null;
    }

    public DeepFetchResult zDeepFetchFirstLinkInMemory(DeepFetchNode node)
    {
        DeepFetchStrategy strategy = this.getDeepFetchStrategy();
        if (strategy != null)
        {
            return strategy.deepFetchFirstLinkInMemory(node);
        }
        return DeepFetchResult.incompleteResult();
    }

    public List zFinishAdhocDeepFetch(DeepFetchNode deepFetchNode, DeepFetchResult result)
    {
        return this.getDeepFetchStrategy().finishAdhocDeepFetch(deepFetchNode, result);
    }

    public List zDeepFetchWithTempContext(DeepFetchNode node, TupleTempContext tempContext, Object parentPrototype, List immediateParentList)
    {
        return this.getDeepFetchStrategy().deepFetchAdhocUsingTempContext(node, tempContext, parentPrototype, immediateParentList);
    }

    public RelationshipMultiExtractor zGetRelationshipMultiExtractor()
    {
        return relationshipMultiExtractor;
    }

    protected void zSetRelationshipMultiExtractor(RelationshipMultiExtractor relationshipMultiExtractor)
    {
        this.relationshipMultiExtractor = relationshipMultiExtractor;
    }

    public OrderBy zGetOrderBy()
    {
        return _orderBy;
    }

    public List zDeepFetchWithInClause(DeepFetchNode deepFetchNode, Attribute singleAttribute, List parentList)
    {
        return this.getDeepFetchStrategy().deepFetchAdhocUsingInClause(deepFetchNode, singleAttribute, parentList);
    }

    public boolean zCanFinishAdhocDeepFetchResult()
    {
        return this.getDeepFetchStrategy().canFinishAdhocDeepFetchResult();
    }

    @Override
    public void registerForNotification(MithraApplicationClassLevelNotificationListener listener)
    {
        this.getMithraObjectPortal().registerForApplicationClassLevelNotification(listener);
    }

    @Override
    public void registerForNotification(Set sourceAttributeValueSet, MithraApplicationClassLevelNotificationListener listener)
    {
        this.getMithraObjectPortal().registerForApplicationClassLevelNotification(sourceAttributeValueSet, listener);
    }
}
