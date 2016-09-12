<%--
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
--%>
<%@ page import="java.util.*" %>
<%@ page import="com.gs.fw.common.mithra.generator.*" %>
<%@ page import="com.gs.fw.common.mithra.generator.type.StringJavaType" %>
<%@ page import="com.gs.fw.common.mithra.generator.util.StringUtility" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
	AbstractAttribute[] normalAttributes = wrapper.getSortedNormalAndSourceAttributes();
    EmbeddedValue[] embeddedValueObjects = wrapper.getEmbeddedValueObjects();
    Attribute[] nullablePrimitiveAttributes = wrapper.getNullablePrimitiveAttributes();
	RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
    AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
    Attribute[] pkAttributes = wrapper.getPrimaryKeyAttributes();
    TransactionalMethodSignature[] transactionalMethodSignatures = wrapper.getTransactionalMethodSignatures();
    TransactionalMethodSignature[] datedTransactionalMethodSignatures = wrapper.getDatedTransactionalMethodSignatures();
    String finderClassName = wrapper.getFinderClassName();
    String semiSpecificFinderTypeName = finderClassName + '.' + wrapper.getClassName() + "RelatedFinder";  // i.e. DomainFinder.DomainRelatedFinder
    MithraInterfaceType[] mithraInterfaceTypes = wrapper.getMithraInterfaces();
%>

package <%= wrapper.getPackageName() %>;

<%@  include file="../Import.jspi" %>
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.list.*;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;
import java.util.Arrays;
import java.util.HashSet;
import com.gs.fw.common.mithra.behavior.*;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.behavior.state.DatedPersistedState;
import com.gs.fw.common.mithra.attribute.update.*;
import com.gs.fw.common.mithra.util.StatisticCounter;

<%@  include file="../DoNotModifyWarning.jspi" %>
// Generated from templates/datedtransactional/Abstract.jsp
<% if (wrapper.hasSuperClass()) { %>
public abstract class <%=wrapper.getAbstractClassName()%> extends <%=wrapper.getFullyQualifiedSuperClassType()%> implements MithraDatedTransactionalObject, Serializable
<% if (wrapper.hasMithraInterfaces()) { %>
    <% for (int i=0;i<mithraInterfaceTypes.length;i++) { %>
        , <%=mithraInterfaceTypes[i].getClassName()%>
       <% } %>
   <% } %>
<% } else { %>
public abstract class <%=wrapper.getAbstractClassName()%> extends com.gs.fw.common.mithra.superclassimpl.MithraDatedTransactionalObjectImpl implements Serializable
<% if (wrapper.hasMithraInterfaces()) { %>
      <% for (int i=0;i<mithraInterfaceTypes.length;i++) { %>
          , <%=mithraInterfaceTypes[i].getClassName()%>
        <% } %>
    <% } %>
<% } %>
{
<% if (wrapper.generateTxMethods()) { %>
    private static TemporalDirector temporalDirector = new <%= wrapper.getTemporalDirectorClass() %>(
    <% for(int i=0;i<asOfAttributes.length;i++) { %>
        <% if (i > 0) { %>, <% } %>
        <%=wrapper.getFinderClassName()%>.<%= asOfAttributes[i].getName()%>()
    <% } %>, <%=wrapper.getFinderClassName()%>.zGetDoubleAttributes(),<%=wrapper.getFinderClassName()%>.zGetBigDecimalAttributes());
    <% for (int i=0;i<asOfAttributes.length;i++) { %>
            protected transient Timestamp <%= asOfAttributes[i].getName()%>;
    <% } %>
<% } %>
    private static final Logger logger = LoggerFactory.getLogger(<%=wrapper.getClassName()%>.class.getName());
    private static byte MEMORY_STATE = DatedPersistenceState.IN_MEMORY;
    private static byte PERSISTED_STATE = DatedPersistenceState.PERSISTED;
<% if (wrapper.hasUpdateListener()) { %>
    protected static final MithraUpdateListener mithraUpdateListener = new <%= wrapper.getUpdateListener()%>();
<% }%>
<%@  include file="../Relationships.jspi" %>
    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
    <% if (wrapper.isTransactional() && relationshipAttributes[i].mustPersistRelationshipChanges()){ %>
        public boolean is<%=StringUtility.firstLetterToUpper(relationshipAttributes[i].getName())%>ModifiedSinceDetachment()
        {
            DatedTransactionalBehavior _behavior = zGetTransactionalBehavior();
                <%=wrapper.getDataClassName()%> _data = (<%=wrapper.getDataClassName()%>)  _behavior.getCurrentDataForRead(this);
                <%if(!relationshipAttributes[i].isToMany()){%>
            if (_data.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation)
            {
                 <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> _existing = <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%=relationshipAttributes[i].getMithraImplTypeAsString()%>)<%}%> this.getOriginalPersistentObject().<%=relationshipAttributes[i].getGetter()%>();
                return _existing != null;
            }
                <%}%>
               <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                        (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _data.<%=relationshipAttributes[i].getGetter()%>();
            if( <%= relationshipAttributes[i].getName()%> != null)
            {
                return <%= relationshipAttributes[i].getName()%>.isModifiedSinceDetachment();
            }
            return false;
        }
   <%}%>
    <% } %>

<% for (RelationshipAttribute rel : relationshipAttributes) { %>
    <% if (rel.isDirectReferenceInBusinessObject()) { %>
    private Object <%= rel.getDirectRefVariableName() %>;
    <% } %>
<% } %>

    public <%=wrapper.getAbstractClassName()%>(Timestamp <%= asOfAttributes[0].getName()%>
    <% for (int i=1;i<asOfAttributes.length;i++) { %>
            , Timestamp <%= asOfAttributes[i].getName()%>
    <% } %>
        )
    {
        <% if (wrapper.isTablePerClassSubClass()) { %>
            super(<%= asOfAttributes[0].getName() %>
            <% for (int i = 1; i < asOfAttributes.length; i++) { %>
                ,<%= asOfAttributes[i].getName() %>
            <% } %>
            );
        <% } else { %>
            <% for (int i = 0; i < asOfAttributes.length; i++) { %>
                <% if (asOfAttributes[i].isPoolable()) { %>
                    this.<%= asOfAttributes[i].getName() %> = TimestampPool.getInstance().getOrAddToCache(<%= asOfAttributes[i].getName() %>, <%= wrapper.getFinderClassName()%>.isFullCache());
                <% } else { %>
                    this.<%= asOfAttributes[i].getName() %> = <%= asOfAttributes[i].getName() %>;
                <% } %>
            <% } %>
        <% } %>
        this.persistenceState = MEMORY_STATE;
    }

    <% if (wrapper.hasBusinessDateAsOfAttribute() && wrapper.hasProcessingDate()) { %>
    public <%=wrapper.getAbstractClassName()%>(Timestamp <%= wrapper.getBusinessDateAsOfAttribute().getName()%>)
    {
        this(<%= wrapper.getBusinessDateAsOfAttribute().getName() %>, <%= wrapper.getProcessingDateAttribute().getInfinityExpression() %>);
    }
    <% } %>

    public <%= wrapper.getDataClassName() %> zSynchronizedGetData()
    {
        return (<%= wrapper.getDataClassName() %>) super.zSynchronizedGetData();
    }

<% if (wrapper.hasDirectRefsInBusinessObject()) { %>
    protected void zClearAllDirectRefs()
    {
    <% for (RelationshipAttribute rel : relationshipAttributes) { %>
        <% if (rel.isDirectReferenceInBusinessObject()) { %>
        <%= rel.getDirectRefVariableName() %> = null;
        <% } %>
    <% } %>
    }
<% } %>
    protected boolean checkAsOfAttributesForRefresh(MithraDataObject current)
    {
        boolean refresh = !<%= wrapper.getClassName() %>Finder.<%= asOfAttributes[0].getName()%>().dataMatches(current, this.<%= asOfAttributes[0].getName()%>);
        <% for(int i=1;i<asOfAttributes.length;i++) { %>
        if (!refresh)
        {
            refresh = !<%= wrapper.getClassName() %>Finder.<%= asOfAttributes[i].getName()%>().dataMatches(current, this.<%= asOfAttributes[i].getName()%>);
        }
        <% } %>
        return refresh;
    }

    public TemporalDirector zGetTemporalDirector()
    {
        return temporalDirector;
    }

<% if((!wrapper.isTablePerClassSubClass())&&(wrapper.hasMultipleLifeCycleParents() || wrapper.hasPkGeneratorStrategy())){%>
    public void insert() throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        <% if(wrapper.hasMultipleLifeCycleParents()){%>
        if (behavior.isPersisted()) return;
        <% } %>
        <% if(wrapper.hasPkGeneratorStrategy()){%>
          this.checkAndGeneratePrimaryKeys();
        <%}%>
        behavior.insert(this);
    }
<% } %>

<% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
    <% if(wrapper.hasMultipleLifeCycleParents() || wrapper.hasPkGeneratorStrategy()){%>
    public void insertUntil(Timestamp exclusiveUntil) throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        <% if(wrapper.hasMultipleLifeCycleParents()){%>
        if (behavior.isPersisted()) return;
        <% } %>
        <% if(wrapper.hasPkGeneratorStrategy()){%>
        checkAndGeneratePrimaryKeys();
        <%}%>
        behavior.insertUntil(this, exclusiveUntil);
    }

    <% } %>
<% } else { %>
    public void insertUntil(Timestamp exclusiveUntil) throws MithraBusinessException
    {
        throw new MithraBusinessException("insertUntil is only supported for dated objects with a business date");
    }

    public void terminateUntil(Timestamp exclusiveUntil) throws MithraBusinessException
    {
        throw new MithraBusinessException("terminateUntil is only supported for dated objects with a business date");
    }
<% } %>

<% if(wrapper.hasPkGeneratorStrategy()){%>
    public void insertForRecovery() throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        checkAndGeneratePrimaryKeys();
        behavior.insertForRecovery(this);
    }

    public void insertWithIncrement() throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        this.checkAndGeneratePrimaryKeys();
        behavior.insertWithIncrement(this);
    }

    public void insertWithIncrementUntil(Timestamp exclusiveUntil) throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        this.checkAndGeneratePrimaryKeys();
        behavior.insertWithIncrementUntil(this, exclusiveUntil);
    }
<%}%>

    protected void cascadeInsertImpl() throws MithraBusinessException
    {
        DatedTransactionalBehavior _behavior = zGetTransactionalBehaviorForWrite();
        <% if(wrapper.hasMultipleLifeCycleParents()){%>
        if (_behavior.isPersisted()) return;
        <% } %>
          <% if(wrapper.hasPkGeneratorStrategy()){%>
            this.checkAndGeneratePrimaryKeys();
          <%}%>
          <% if (wrapper.hasDependentRelationships()) { %>
            <%=wrapper.getDataClassName()%> _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);
            <% for (int i=0;i<relationshipAttributes.length;i++)
                if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
            <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName() %> =
                (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> ) _data.<%=relationshipAttributes[i].getGetter()%>();
            <% } %>
          <% } %>
            <% if (wrapper.isTablePerClassSubClass()) { %>
                super.cascadeInsertImpl();
            <% } else { %>
                _behavior.insert(this);
            <% } %>
          <% if (wrapper.hasDependentRelationships()) { %>
            <% for (int i=0;i<relationshipAttributes.length;i++)
                if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()
                && relationshipAttributes[i].getRelatedObject().isTransactional()) { %>
                if (<%= relationshipAttributes[i].getName() %> != null)
                {
                    <% if (relationshipAttributes[i].isToMany()) { %>
                    <%= relationshipAttributes[i].getName() %>.cascadeInsertAll();
                    <% } else { %>
                    <%= relationshipAttributes[i].getName() %>.cascadeInsert();
                    <% } %>
                }
            <% } %>
          <% } %>
    }

    @Override
    public Map< RelatedFinder, StatisticCounter > zAddNavigatedRelationshipsStats(RelatedFinder finder, Map< RelatedFinder, StatisticCounter > navigationStats)
    {
        DatedTransactionalBehavior _behavior = zGetTransactionalBehaviorForWrite();
        _behavior.addNavigatedRelationshipsStats(this, finder, navigationStats);
        return navigationStats;
    }

    @Override
    public Map< RelatedFinder, StatisticCounter > zAddNavigatedRelationshipsStatsForUpdate(RelatedFinder parentFinderGeneric, Map< RelatedFinder, StatisticCounter > navigationStats)
    {
        <% if (wrapper.hasDependentRelationships()) { %>
          <%=semiSpecificFinderTypeName%> parentFinder = (<%=semiSpecificFinderTypeName%>) parentFinderGeneric;
          DatedTransactionalBehavior _behavior = zGetTransactionalBehaviorForWrite();
          <%=wrapper.getDataClassName()%> _newData = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);

          <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
            <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
                <% if (!relationshipAttributes[i].isToMany()) { // isToOne %>
                if (_newData.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation)
                {
                    RelatedFinder dependentFinder = parentFinder.<%= relationshipAttributes[i].getName() %>();
                    DeepRelationshipUtility.zAddToNavigationStats(dependentFinder, true, navigationStats);
                    DeepRelationshipUtility.zAddAllDependentNavigationsStatsForDelete(dependentFinder, navigationStats);
                }
                else
                <% } %>
                {
                    <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                        (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _newData.<%=relationshipAttributes[i].getGetter()%>();
                    RelatedFinder dependentFinder = parentFinder.<%= relationshipAttributes[i].getName() %>();
                    DeepRelationshipUtility.zAddToNavigationStats(dependentFinder, <%= relationshipAttributes[i].getName()%> != null, navigationStats);
                    if (<%= relationshipAttributes[i].getName()%> != null)
                    {
                        <% if (relationshipAttributes[i].isToMany()) { %>
                            <%= relationshipAttributes[i].getName() %>.zCascadeAddNavigatedRelationshipsStats(dependentFinder, navigationStats);
                        <% } else { %>
                            _behavior.addNavigatedRelationshipsStats(<%= relationshipAttributes[i].getName() %>, dependentFinder, navigationStats);
                        <% } %>
                    }
                }
            <% } %>
          <% } %>
        <% } %>
        return navigationStats;
    }

    @Override
    public Map< RelatedFinder, StatisticCounter > zAddNavigatedRelationshipsStatsForDelete(RelatedFinder parentFinder, Map< RelatedFinder, StatisticCounter > navigationStats)
    {
        <% if (wrapper.hasDependentRelationships()) { %>
            DeepRelationshipUtility.zAddAllDependentNavigationsStatsForDelete(parentFinder, navigationStats);
        <% } %>
        return navigationStats;
    }

    public <%=wrapper.getImplClassName()%> zCascadeCopyThenInsert() throws MithraBusinessException
    {
        DatedTransactionalBehavior _behavior = zGetTransactionalBehaviorForWrite();
        <% if(wrapper.hasMultipleLifeCycleParents()){%>
        if (_behavior.isPersisted()) return (<%=wrapper.getImplClassName()%>) this;
        <% } %>
          <% if(wrapper.hasPkGeneratorStrategy()){%>
            this.checkAndGeneratePrimaryKeys();
          <%}%>
          <% if (wrapper.hasDependentRelationships()) { %>
            <%=wrapper.getDataClassName()%> _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);
            <% for (int i=0;i<relationshipAttributes.length;i++)
                if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
            <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName() %> =
                (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> ) _data.<%=relationshipAttributes[i].getGetter()%>();
            <% } %>
          <% } %>
            <% if (wrapper.isTablePerClassSubClass()) { %>
                <%= wrapper.getImplClassName() %> original = (<%= wrapper.getImplClassName() %>) super.zCascadeCopyThenInsert();
            <% } else { %>
                <%= wrapper.getImplClassName() %> original = (<%= wrapper.getImplClassName() %>) _behavior.copyThenInsert(this);
            <% } %>
          <% if (wrapper.hasDependentRelationships()) { %>
            <% for (int i=0;i<relationshipAttributes.length;i++)
                if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()
                && relationshipAttributes[i].getRelatedObject().isTransactional()) { %>
                if (<%= relationshipAttributes[i].getName() %> != null)
                {
                    <% if (relationshipAttributes[i].isToMany()) { %>
                    <%= relationshipAttributes[i].getName() %>.zCascadeCopyThenInsertAll();
                    <% } else { %>
                    <%= relationshipAttributes[i].getName() %>.zCascadeCopyThenInsert();
                    <% } %>
                }
            <% } %>
          <% } %>
            return original;
    }

    public void cascadeInsertUntil(Timestamp exclusiveUntil) throws MithraBusinessException
    {
      <% if (wrapper.hasDependentRelationships()) { %>
        <% for (int i=0;i<relationshipAttributes.length;i++)
            if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()
            && relationshipAttributes[i].getRelatedObject().isTransactional()
                    && relationshipAttributes[i].getRelatedObject().hasBusinessDateAsOfAttribute()) { %>
            <% if (relationshipAttributes[i].isToMany()) { %>
                <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>((<%=relationshipAttributes[i].getImplTypeAsString()%>)<%}%>this.<%=relationshipAttributes[i].getGetter()%>()<%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>)<%}%>.cascadeInsertAllUntil(exclusiveUntil);
            <% } else { %>
                {
                    <%= relationshipAttributes[i].getImplTypeAsString()%> related = <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%= relationshipAttributes[i].getImplTypeAsString()%>)<%}%>this.<%=relationshipAttributes[i].getGetter()%>();
                    if (related != null)
                        related.cascadeInsertUntil(exclusiveUntil);
                }
            <% } %>
        <% } %>
      <% } %>
        this.insertUntil(exclusiveUntil);
    }


<% if (!wrapper.hasProcessingDate()) { %>
    public void inactivateForArchiving(Timestamp processingDateTo, Timestamp businessDateTo) throws MithraBusinessException
    {
        throw new MithraBusinessException("inactivateForArchiving is only supported for dated objects with a processing date");
    }
<% } %>

    <% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
    public <%=wrapper.getImplClassName()%> copyDetachedValuesToOriginalOrInsertIfNewUntil(Timestamp exclusiveUntil)
    {
        return (<%=wrapper.getImplClassName()%>) this.zCopyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil);
    }

    public <%=wrapper.getImplClassName()%> copyDetachedValuesToOriginalOrInsertIfNewUntilImpl(Timestamp exclusiveUntil, MithraTransaction tx)
    {
        DatedTransactionalBehavior behavior = zGetTransactionalBehaviorForWrite();
        return (<%=wrapper.getImplClassName()%>) behavior.updateOriginalOrInsertUntil(this, exclusiveUntil);
    }
    <% } %>

    protected DatedTransactionalState zCreateDatedTransactionalState(TemporalContainer container, MithraDataObject data, MithraTransaction threadTx)
    {
        return new DatedTransactionalState(threadTx,
                this.persistenceState, container, data, <%= wrapper.getBusinessDateAsOfAttributeName() %>,
                    <%= wrapper.getIsProcessingDateCurrentExperssion() %>);
    }

    public <%= wrapper.getImplClassName() %> getNonPersistentCopy() throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehavior();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        MithraDataObject newData = data.copy(!behavior.isPersisted());
        Timestamp[] asOfAttributes = new Timestamp[<%= asOfAttributes.length %>];
        <% for(int i=0;i<asOfAttributes.length;i++) { %>
        asOfAttributes[<%= i %>] = this.<%= asOfAttributes[i].getName()%>;
        <% } %>
        <%= wrapper.getAbstractClassName() %> result = (<%= wrapper.getAbstractClassName() %>)
                ((MithraDatedObjectFactory) this.zGetPortal().getMithraDatedObjectFactory()).createObject(newData, asOfAttributes);
        result.zSetNonTxPersistenceState(MEMORY_STATE);
        if (result.transactionalState != null)
        {
            result.zSetTxPersistenceState(DatedPersistenceState.IN_MEMORY);
        }
        return (<%= wrapper.getImplClassName() %> ) result;
    }

    public <%=wrapper.getImplClassName()%> zFindOriginal()
    {
        <%= wrapper.getDataClassName() %> data = (<%= wrapper.getDataClassName() %>) this.currentData;
        <%= wrapper.getPrimaryKeyOperation()%>
        return <%if(wrapper.isGenerateInterfaces()){%>(<%=wrapper.getImplClassName()%>)<%}%> <%= wrapper.getFinderClassName()%>.findOne(op);
    }

    public <%= wrapper.getImplClassName() %> getDetachedCopy() throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehavior();
        Timestamp[] asOfAttributes = new Timestamp[<%= asOfAttributes.length %>];
        <% for(int i=0;i<asOfAttributes.length;i++) { %>
        asOfAttributes[<%= i %>] = this.<%= asOfAttributes[i].getName()%>;
        <% } %>
        <%= wrapper.getAbstractClassName() %> result = (<%= wrapper.getAbstractClassName() %>) behavior.getDetachedCopy(this, asOfAttributes);
        if (result.transactionalState != null)
        {
            result.zSetTxPersistenceState(DatedPersistenceState.DETACHED);
        }
        return (<%= wrapper.getImplClassName() %> ) result;
    }

    public boolean isModifiedSinceDetachmentByDependentRelationships()
    {
       if(this.isModifiedSinceDetachment()) return true;
        <%for(int i = 0 ; i < relationshipAttributes.length; i++){
            if(relationshipAttributes[i].isRelatedDependent()){%>
       if(is<%=StringUtility.firstLetterToUpper(relationshipAttributes[i].getName())%>ModifiedSinceDetachment()) return true;
        <%} }%>
       return false;
    }

    public <%=wrapper.getImplClassName()%> copyDetachedValuesToOriginalOrInsertIfNewImpl(MithraTransaction tx)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        <% if(wrapper.hasMultipleLifeCycleParents()){%>
        if (behavior.isPersisted()) return (<%=wrapper.getClassName()%>) this;
        <% } %>
        return (<%=wrapper.getImplClassName()%>) behavior.updateOriginalOrInsert(this);
    }

    public <%=wrapper.getImplClassName()%> copyDetachedValuesToOriginalOrInsertIfNew()
    {
        return (<%=wrapper.getImplClassName()%>) this.zCopyDetachedValuesToOriginalOrInsertIfNew();
    }

    public Object readResolve() throws ObjectStreamException
    {
        if (this.persistenceState == DatedPersistenceState.PERSISTED)
        {
            <%= wrapper.getDataClassName() %> data = (<%= wrapper.getDataClassName() %>) this.currentData;
            Operation op = <%= wrapper.getFinderClassName() %>.<%= asOfAttributes[0].getName()%>().eq(this.<%= asOfAttributes[0].getName()%>);
            <% for (int i=1;i < asOfAttributes.length;i++) { %>
            op = op.and(<%= wrapper.getFinderClassName() %>.<%= asOfAttributes[i].getName()%>().eq(this.<%= asOfAttributes[i].getName()%>));
            <% } %>
            <% for (int i=0; i < normalAttributes.length ; i++) if (normalAttributes[i].isPrimaryKey()) { %>
               <% if (normalAttributes[i].isNullable()) { %>
               if( data.<%= normalAttributes[i].getNullGetter() %> )
               {
                  op = op.and(<%= wrapper.getFinderClassName() %>.<%= normalAttributes[i].getName()%>().isNull());
               }
               else
               {
               <% } %>
            op = op.and(<%= wrapper.getFinderClassName() %>.<%= normalAttributes[i].getName()%>().eq(data.<%= normalAttributes[i].getGetter()%>()));
               <% if (normalAttributes[i].isNullable()) { %>
               }
               <% } %>
            <% } %>
            <% if (wrapper.hasSourceAttribute()) { %>
            op = op.and(<%= wrapper.getFinderClassName() %>.<%= wrapper.getSourceAttribute().getName()%>().eq(data.<%= wrapper.getSourceAttribute().getGetter()%>()));
            <% } %>
            return <%= wrapper.getFinderClassName() %>.findOne(op);
        }
        return this;
    }

    public boolean zHasSameNullPrimaryKeyAttributes(MithraTransactionalObject other)
    {
        <%if(wrapper.hasNullablePrimaryKeys()){%>
        <%=wrapper.getDataClassName()%> otherData = (<%=wrapper.getDataClassName()%>)other.zGetTxDataForRead();
        return ((<%=wrapper.getDataClassName()%>) this.zGetTxDataForRead()).zHasSameNullPrimaryKeyAttributes(otherData);
        <% } else{ %>
        return true;
        <% } %>
    }
    <% if (wrapper.isTablePerClassSubClass()) { %>
        public void insert() throws MithraBusinessException
        {
            MithraManagerProvider.getMithraManager().mustBeInTransaction("insert for inherited classed must be done in a transaction.");
            super.insert();
        }
    <% } %>

<% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
    protected boolean zMustCheckCurrent(MithraDataObject d)
    {
        <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) d;
        Timestamp businessTo = data.get<%=StringUtility.firstLetterToUpper(wrapper.getBusinessDateAsOfAttributeName())%>To();
        return businessTo != null && <%= wrapper.getFinderClassName() %>.<%= wrapper.getBusinessDateAsOfAttributeName() %>().getInfinityDate().getTime() == businessTo.getTime();
    }

    protected boolean zMustCheckCurrent(MithraDataObject d, Timestamp exclusiveUntil)
    {
        <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) d;
        Timestamp businessTo = data.get<%=StringUtility.firstLetterToUpper(wrapper.getBusinessDateAsOfAttributeName())%>To();
        return businessTo != null && businessTo.getTime() >= exclusiveUntil.getTime();
    }

<% } %>
    <% for (int i = 0; i < normalAttributes.length; i ++) {%>
    <%= normalAttributes[i].getVisibility() %> <%=normalAttributes[i].isFinalGetter() ? "final " : ""%> boolean <%=normalAttributes[i].getNullGetter()%>
    {
        return this.zSynchronizedGetData().<%=normalAttributes[ i ].getNullGetter()%>;
    }
    <%}%>
    <% for (int i = 0; i < normalAttributes.length; i ++) { %>
    <%= normalAttributes[i].getVisibility() %> <%=normalAttributes[i].isFinalGetter() ? "final " : ""%><%=normalAttributes[i].getTypeAsString()%> <%=normalAttributes[ i ].getGetter()%>()
    {
        <%if(normalAttributes[i].isNullablePrimitive() && (normalAttributes[i]).getDefaultIfNull() == null){%>
        <%=wrapper.getDataClassName()%> data = this.zSynchronizedGetData();
        if (data.<%=normalAttributes[i].getNullGetter()%>) MithraNullPrimitiveException.throwNew("<%=normalAttributes[i].getName()%>", data);
        return data.<%=normalAttributes[ i ].getGetter()%>();
        <%} else { //nullable primitive%>
        return this.zSynchronizedGetData().<%=normalAttributes[ i ].getGetter()%>();
        <%}//nullable primitive%>
    }

    <%= normalAttributes[i].getVisibility() %> void <%= normalAttributes[i].getSetter()%>(<%=normalAttributes[i].getTypeAsString()%> newValue)
    {
        <% if (normalAttributes[i].mustTrim()) { %>
        if (newValue != null) newValue = newValue.trim();
        <% } %>

        <% if ( normalAttributes[i].isStringAttribute() && normalAttributes[i].hasMaxLength() ) { %>
          if (newValue != null && newValue.length() > <%=normalAttributes[i].getMaxLength()%>)
          {
              <% if (normalAttributes[i].truncate()) { %>
              newValue = newValue.substring(0, <%= normalAttributes[i].getMaxLength() %> )<%if (normalAttributes[i].mustTrim()) { %>.trim()<% } %>;
              <% } else { %>
              throw new MithraBusinessException("Attribute '<%=normalAttributes[i].getName()%>' cannot exceed maximum length of <%=normalAttributes[i].getMaxLength()%>: " + newValue);
              <% } %>
          }
        <% } %>
        <% if (normalAttributes[i].isTimeAttribute() && normalAttributes[i].hasModifyTimePrecisionOnSet()) { %>
            <% if (normalAttributes[i].getModifyTimePrecisionOnSet().isSybase()) { %>
                if(newValue != null)
                    newValue = newValue.createOrReturnTimeWithSybaseMillis();
            <% } else if (normalAttributes[i].getModifyTimePrecisionOnSet().isTenMillisecond())%>
                    if(newValue != null)
                        newValue = newValue.createOrReturnTimeTenMillisecond();
        <% } %>
        <% if (!normalAttributes[i].mustSetRelatedObjectAttribute()) { %>
        zSet<%= normalAttributes[i].getType().getJavaTypeStringPrimary()%>(<%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(), newValue, <%= ""+((normalAttributes[i].isPrimaryKey() )||normalAttributes[i].isReadonly() || normalAttributes[i] == wrapper.getOptimisticLockAttribute())%> <%if (normalAttributes[i].isPrimitive()) { %>,<%= normalAttributes[i].isNullable()%><% } %>);
        <% } else { %>
        MithraDataObject d = zSet<%= normalAttributes[i].getType().getJavaTypeStringPrimary()%>(<%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(), newValue, <%= ""+((normalAttributes[i].isPrimaryKey() )||normalAttributes[i].isReadonly() || normalAttributes[i] == wrapper.getOptimisticLockAttribute())%> <%if (normalAttributes[i].isPrimitive()) { %>,<%= normalAttributes[i].isNullable()%><% } %>);
        if (d == null) return;
        <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) d;
        DatedTransactionalBehavior _behavior = this.zGetTransactionalBehaviorForWrite();
        if (!_behavior.isPersisted())
        {
        <%
            DependentRelationship[] relationshipsToSet = normalAttributes[i].getDependentRelationships();
            for(int r=0;r<relationshipsToSet.length;r++)
            {
                RelationshipAttribute relationshipAttribute = relationshipsToSet[ r ].getRelationshipAttribute();
                if (relationshipAttribute.isRelatedDependent())
                {
        %>
                <%=relationshipAttribute.getMithraImplTypeAsString()%> <%= relationshipAttribute.getName() %> =
                (<%=relationshipAttribute.getMithraImplTypeAsString()%>) data.<%=relationshipAttribute.getGetter()%>();
            if (<%= relationshipAttribute.getName() %> != null)
            {
                <%= relationshipAttribute.getName() %>.<%= relationshipsToSet[r].getAttributeToSet().getSetter() %>(newValue);
            }
        <%
                }
            } %>
        }
        <% } %>
    }

    <% if((normalAttributes[i].isInPlaceUpdate())) {%>
    <%= normalAttributes[i].getVisibility() %> void <%= normalAttributes[i].getSetter()%>UsingInPlaceUpdate(<%=normalAttributes[i].getTypeAsString()%> newValue)
    {
        <%String typeName = normalAttributes[i].getType().getTypeName();
          if ("DOUBLE".equalsIgnoreCase(typeName))
          {
        %>
        this.zCheckDoubleValue(newValue);
        <%}else if("FLOAT".equalsIgnoreCase(typeName)){%>
        this.zCheckFloatValue(newValue);
        <%}%>

        <%if (normalAttributes[i].mustTrim()) { %>
        if (newValue != null) newValue = newValue.trim();
        <% } %>

        <% if ( normalAttributes[i].isStringAttribute() && normalAttributes[i].hasMaxLength() ) { %>
          if (newValue != null && newValue.length() > <%=normalAttributes[i].getMaxLength()%>)
          {
              <% if (normalAttributes[i].truncate()) { %>
              newValue = newValue.substring(0, <%= normalAttributes[i].getMaxLength() %> )<%if (normalAttributes[i].mustTrim()) { %>.trim()<% } %>;
              <% } else { %>
              throw new MithraBusinessException("Attribute '<%=normalAttributes[i].getName()%>' cannot exceed maximum length of <%=normalAttributes[i].getMaxLength()%>: " + newValue);
              <% } %>
          }
        <% } %>
        <%= wrapper.getDataClassName() %> data;
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        data = (<%= wrapper.getDataClassName() %>) behavior.getCurrentDataForRead(this);
        <% if (normalAttributes[i].isPrimitive()) { %>
        if (<% if (normalAttributes[i].isNullablePrimitive()) { %> !data.<%= normalAttributes[i].getNullGetter()%> && <% } %>
                newValue == data.<%= normalAttributes[i].getGetter()%>()) return;
        <% } else { %>
        <%= normalAttributes[i].getTypeAsString()%> current = data.<%= normalAttributes[i].getGetter()%>();
        if (current == null)
        {
            if (newValue == null) return;
        }
        else
        {
            if (current.equals(newValue)) return;
        }
        <% } %>
        <% if (normalAttributes[i].isPrimaryKey()) { %>
        if (!behavior.maySetPrimaryKey()) throw new MithraBusinessException("cannot change the primary key");
        <% } %>
        AttributeUpdateWrapper updateWrapper =
            new <%= normalAttributes[i].getType().getJavaTypeStringPrimary()%>UpdateWrapper(
                <%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(),
                behavior.getCurrentDataForWrite(this),
                newValue);

           behavior.inPlaceUpdate(this, updateWrapper, <%= normalAttributes[i].isReadonly() || normalAttributes[i].isAsOfAttributeFrom() || normalAttributes[i].isAsOfAttributeTo() %>);
        <% if (wrapper.hasEmbeddedValueObjects()) { %>this.zResetEmbeddedValueObjects(behavior); <% } %>
        <% if (normalAttributes[i].mustSetRelatedObjectAttribute()) {

            DependentRelationship[] relationshipsToSet = normalAttributes[i].getDependentRelationships();
            for(int r=0;r<relationshipsToSet.length;r++)
            {
                RelationshipAttribute relationshipAttribute = relationshipsToSet[ r ].getRelationshipAttribute();
                if (relationshipAttribute.isRelatedDependent())
                {
        %>
                <%=relationshipAttribute.getMithraTypeAsString()%> <%= relationshipAttribute.getName() %> =
                (<%=relationshipAttribute.getMithraTypeAsString()%>) data.<%=relationshipAttribute.getGetter()%>();
            if (<%= relationshipAttribute.getName() %> != null)
            {
                <%= relationshipAttribute.getName() %>.<%= relationshipsToSet[r].getAttributeToSet().getSetter() %>(newValue);
            }
        <%
                }
            }
           } %>
    }
    <% } %>

    <% if ((wrapper.hasBusinessDateAsOfAttribute()) && !(normalAttributes[i].isAsOfAttributeFrom() || normalAttributes[i].isAsOfAttributeTo() )) { %>
    <%= normalAttributes[i].getVisibility() %> void <%= normalAttributes[i].getSetter()%>Until(<%=normalAttributes[i].getTypeAsString()%> newValue, Timestamp exclusiveUntil)
    {
        <% if (normalAttributes[i].mustTrim()) { %>
        if (newValue != null) newValue = newValue.trim();
        <% } %>

        <% if ( normalAttributes[i].isStringAttribute() && normalAttributes[i].hasMaxLength() ) { %>
          if (newValue != null && newValue.length() > <%=normalAttributes[i].getMaxLength()%>)
          {
              <% if (normalAttributes[i].truncate()) { %>
              newValue = newValue.substring(0, <%= normalAttributes[i].getMaxLength() %> )<%if (normalAttributes[i].mustTrim()) { %>.trim()<% } %>;
              <% } else { %>
              throw new MithraBusinessException("Attribute '<%=normalAttributes[i].getName()%>' cannot exceed maximum length of <%=normalAttributes[i].getMaxLength()%>: " + newValue);
              <% } %>
          }
        <% } %>
        <% if (!normalAttributes[i].mustSetRelatedObjectAttribute()) { %>
        zSet<%= normalAttributes[i].getType().getJavaTypeStringPrimary()%>(<%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(), newValue, <%= ""+((normalAttributes[i].isPrimaryKey() )||normalAttributes[i].isReadonly() || normalAttributes[i] == wrapper.getOptimisticLockAttribute())%> , exclusiveUntil<%if (normalAttributes[i].isPrimitive()) { %>,<%= normalAttributes[i].isNullable()%><% } %>);
        <% } else { %>
        MithraDataObject d = zSet<%= normalAttributes[i].getType().getJavaTypeStringPrimary()%>(<%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(), newValue, <%= ""+((normalAttributes[i].isPrimaryKey() )||normalAttributes[i].isReadonly() || normalAttributes[i] == wrapper.getOptimisticLockAttribute())%> , exclusiveUntil<%if (normalAttributes[i].isPrimitive()) { %>,<%= normalAttributes[i].isNullable()%><% } %>);
        if (d == null) return;
        <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) d;
        <%
            DependentRelationship[] relationshipsToSet = normalAttributes[i].getDependentRelationships();
            for(int r=0;r<relationshipsToSet.length;r++)
            {
                RelationshipAttribute relationshipAttribute = relationshipsToSet[ r ].getRelationshipAttribute();
                if (relationshipAttribute.isRelatedDependent())
                {
        %>
                <%=relationshipAttribute.getMithraTypeAsString()%> <%= relationshipAttribute.getName() %> =
                (<%=relationshipAttribute.getMithraTypeAsString()%>) data.<%=relationshipAttribute.getGetter()%>();
            if (<%= relationshipAttribute.getName() %> != null)
            {
                <%= relationshipAttribute.getName() %>.<%= relationshipsToSet[r].getAttributeToSet().getSetter() %>(newValue);
            }
        <%
                }
            }
           } %>
    }

    <% if (normalAttributes[i].isDoubleAttribute() || normalAttributes[i].isBigDecimalAttribute()) { %>
    <%= normalAttributes[i].getVisibility() %> void <%= normalAttributes[i].getIncrementer()%>(<%=normalAttributes[i].getTypeAsString()%> increment)
    {
        zIncrement(increment, <%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(), <%= ""+(normalAttributes[i].isPrimaryKey() || normalAttributes[i].isReadonly()) %>);
    }

    <%= normalAttributes[i].getVisibility() %> void <%= normalAttributes[i].getIncrementer()%>Until(<%=normalAttributes[i].getTypeAsString()%> increment, Timestamp exclusiveUntil)
    {
        zIncrementUntil(increment, exclusiveUntil, <%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(), <%= ""+(normalAttributes[i].isPrimaryKey() || normalAttributes[i].isReadonly()) %>);
    }
    <% } // double increment %>
    <% } // has business date attribute %>
    <% } // normal attributes %>

    <% for (EmbeddedValue evo : embeddedValueObjects) { %>
        <%= evo.getVisibility() %> <%=evo.isFinalGetter() ? "final " : ""%><%= evo.getType() %> <%= evo.getNestedGetter() %>()
        {
            <%= wrapper.getDataClassName() %> data = (<%= wrapper.getDataClassName() %>) this.zSynchronizedGetData();
            <%= evo.getType() %> evo = data.<%= evo.getNestedGetter() %>();
            if (evo == null)
            {
                evo = <%= evo.getType() %>.<%= evo.getFactoryMethodName() %>(this, <%= finderClassName %>.<%= evo.getNestedName() %>());
                data.<%= evo.getNestedSetter() %>(evo);
            }
            return evo;
        }

        <%= evo.getVisibility() %> void <%= evo.getNestedCopyValuesFrom() %>(<%= evo.getType() %> <%= evo.getName() %>)
        {
            <% for (EmbeddedValueMapping attribute : evo.getMappings()) { %>
                <% String type = attribute.getType().getJavaTypeString(); %>
                <%= type %> <%= attribute.getName() %> = <%= evo.getName() %>.<%= attribute.getShortNameGetter() %>();
                <% if (attribute.isPrimitive()) { %>
                    boolean is<%= StringUtility.firstLetterToUpper(attribute.getName()) %>Null = <%= evo.getName() %>.<%= attribute.getShortNameNullGetter() %>;
                <% } %>
                <% if ("DOUBLE".equalsIgnoreCase(attribute.getType().getTypeName())) { %>
                    zCheckDoubleValue(<%= attribute.getName() %>);
                <% } else if ("FLOAT".equalsIgnoreCase(attribute.getType().getTypeName())) { %>
                    zCheckFloatValue(<%= attribute.getName() %>);
                <% } %>
                <% if (attribute.mustTrim()) { %>
                    if (<%= attribute.getName() %> != null) <%= attribute.getName() %> = <%= attribute.getName() %>.trim();
                <% } %>
                <% if (attribute.getType() instanceof StringJavaType && attribute.hasMaxLength() ) { %>
                    if (<%= attribute.getName() %> != null && <%= attribute.getName() %>.length() > <%= attribute.getMaxLength() %>)
                        <% if (attribute.truncate()) { %>
                            <%= attribute.getName() %> = <%= attribute.getName() %>.substring(0, <%= attribute.getMaxLength() %> )<%if (attribute.mustTrim()) { %>.trim()<% } %>;
                        <% } else { %>
                            throw new MithraBusinessException("Attribute '<%= attribute.getName() %>' cannot exceed maximum length of <%= attribute.getMaxLength() %>: " + <%= attribute.getName() %>);
                        <% } %>
                <% } %>
            <% } %>
            <% for (EmbeddedValue nestedObject : evo.getDescendants()) { %>
                <% for (EmbeddedValueMapping attribute : nestedObject.getMappings()) { %>
                    <% String type = attribute.getType().getJavaTypeString(); %>
                    <%= type %> <%= attribute.getName() %> = <%= evo.getName() %>.<%= attribute.getChainedGetterAfterDepth(evo.getHierarchyDepth()) %>();
                    <% if (attribute.isPrimitive()) { %>
                        boolean is<%= StringUtility.firstLetterToUpper(attribute.getName()) %>Null = <%= evo.getName() %>.<%= attribute.getChainedNullGetterAfterDepth(evo.getHierarchyDepth()) %>;
                    <% } %>
                    <% if ("DOUBLE".equalsIgnoreCase(attribute.getType().getTypeName())) { %>
                        zCheckDoubleValue(<%= attribute.getName() %>);
                    <% } else if ("FLOAT".equalsIgnoreCase(attribute.getType().getTypeName())) { %>
                        zCheckFloatValue(<%= attribute.getName() %>);
                    <% } %>
                    <% if (attribute.mustTrim()) { %>
                        if (<%= attribute.getName() %> != null) <%= attribute.getName() %> = <%= attribute.getName() %>.trim();
                    <% } %>
                    <% if (attribute.getType() instanceof StringJavaType && attribute.hasMaxLength() ) { %>
                        if (<%= attribute.getName() %> != null && <%= attribute.getName() %>.length() > <%= attribute.getMaxLength() %>)
                            <% if (attribute.truncate()) { %>
                                <%= attribute.getName() %> = <%= attribute.getName() %>.substring(0, <%= attribute.getMaxLength() %> )<%if (attribute.mustTrim()) { %>.trim()<% } %>;
                            <% } else { %>
                                throw new MithraBusinessException("Attribute '<%= attribute.getName() %>' cannot exceed maximum length of <%= attribute.getMaxLength() %>: " + <%= attribute.getName() %>);
                            <% } %>
                    <% } %>
                <% } %>
            <% } %>
            DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
            <%= wrapper.getDataClassName() %> data = (<%= wrapper.getDataClassName() %>) behavior.getCurrentDataForRead(this);
            boolean dataChanged = true;
            AttributeUpdateWrapper updateWrapper;
            <% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
            Timestamp businessTo = data.get<%= StringUtility.firstLetterToUpper(wrapper.getBusinessDateAsOfAttributeName()) %>To();
            <% } %>
            <% for (EmbeddedValueMapping attribute : evo.getMappingsRecursively()) { %>
                <% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
                    if (businessTo != null && <%= wrapper.getFinderClassName() %>.<%= wrapper.getBusinessDateAsOfAttributeName() %>().getInfinityDate().getTime() == businessTo.getTime())
                <% } %>
                {
                    <% if (attribute.isPrimitive()) { %>
                        <% if (attribute.isNullablePrimitive()) { %>
                            if (data.<%= attribute.getNullGetter() %> && is<%= StringUtility.firstLetterToUpper(attribute.getName()) %>Null)
                            {
                                dataChanged = false;
                            }
                            else
                            {
                                dataChanged = data.<%= attribute.getGetter() %>() != <%= attribute.getName() %>;
                            }
                        <% } else { %>
                            dataChanged = data.<%= attribute.getGetter() %>() != <%= attribute.getName() %>;
                        <% } %>
                    <% } else { %>
                        if (data.<%= attribute.getGetter() %>() == null)
                        {
                            dataChanged = <%= attribute.getName() %> != null;
                        }
                        else
                        {
                            dataChanged = !data.<%= attribute.getGetter() %>().equals(<%= attribute.getName() %>);
                        }
                    <% } %>
                }
                if (dataChanged)
                {
                <% if (attribute.isPrimaryKey() && !attribute.isMutablePrimaryKey()) { %>
                    if (!behavior.maySetPrimaryKey()) throw new MithraBusinessException("cannot change the primary key");
                <% } %>
                    updateWrapper = new <%= attribute.getType().getJavaTypeStringPrimary() %>UpdateWrapper(
                        <%= wrapper.getFinderClassName() %>.<%= attribute.getName() %>(),
                        behavior.getCurrentDataForWrite(this),
                        <%= attribute.getName() %>);
                    behavior.update(this, updateWrapper, <%= (attribute.isReadonly() || attribute.isAsOfAttributeFrom() || attribute.isAsOfAttributeTo()) %>, true);
                }
            <% } %>
            this.zResetEmbeddedValueObjects(behavior);
        }

        <% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
            <%= evo.getVisibility() %> void <%= evo.getNestedCopyValuesFromUntil() %>(<%= evo.getType() %> <%= evo.getName() %>, Timestamp exclusiveUntil)
            {
                <% for (EmbeddedValueMapping attribute : evo.getMappings()) { %>
                    <% String type = attribute.getType().getJavaTypeString(); %>
                    <%= type %> <%= attribute.getName() %> = <%= evo.getName() %>.<%= attribute.getShortNameGetter() %>();
                    <% if (attribute.isPrimitive()) { %>
                        boolean is<%= StringUtility.firstLetterToUpper(attribute.getName()) %>Null = <%= evo.getName() %>.<%= attribute.getShortNameNullGetter() %>;
                    <% } %>
                    <% if ("DOUBLE".equalsIgnoreCase(attribute.getType().getTypeName())) { %>
                        this.zCheckDoubleValue(<%= attribute.getName() %>);
                    <% } else if ("FLOAT".equalsIgnoreCase(attribute.getType().getTypeName())) { %>
                        this.zCheckFloatValue(<%= attribute.getName() %>);
                    <% } %>
                    <% if (attribute.mustTrim()) { %>
                        if (<%= attribute.getName() %> != null) <%= attribute.getName() %> = <%= attribute.getName() %>.trim();
                    <% } %>
                    <% if (attribute.getType() instanceof StringJavaType && attribute.hasMaxLength() ) { %>
                        if (<%= attribute.getName() %> != null && <%= attribute.getName() %>.length() > <%= attribute.getMaxLength() %>)
                            <% if (attribute.truncate()) { %>
                                <%= attribute.getName() %> = <%= attribute.getName() %>.substring(0, <%= attribute.getMaxLength() %> )<%if (attribute.mustTrim()) { %>.trim()<% } %>;
                            <% } else { %>
                                throw new MithraBusinessException("Attribute '<%= attribute.getName() %>' cannot exceed maximum length of <%= attribute.getMaxLength() %>: " + <%= attribute.getName() %>);
                            <% } %>
                    <% } %>
                <% } %>
                <% for (EmbeddedValue nestedObject : evo.getDescendants()) { %>
                    <% for (EmbeddedValueMapping attribute : nestedObject.getMappings()) { %>
                        <% String type = attribute.getType().getJavaTypeString(); %>
                        <%= type %> <%= attribute.getName() %> = <%= evo.getName() %>.<%= attribute.getChainedGetterAfterDepth(evo.getHierarchyDepth()) %>();
                        <% if (attribute.isPrimitive()) { %>
                            boolean is<%= StringUtility.firstLetterToUpper(attribute.getName()) %>Null = <%= evo.getName() %>.<%= attribute.getChainedNullGetterAfterDepth(evo.getHierarchyDepth()) %>;
                        <% } %>
                        <% if ("DOUBLE".equalsIgnoreCase(attribute.getType().getTypeName())) { %>
                            this.zCheckDoubleValue(<%= attribute.getName() %>);
                        <% } else if ("FLOAT".equalsIgnoreCase(attribute.getType().getTypeName())) { %>
                            this.zCheckFloatValue(<%= attribute.getName() %>);
                        <% } %>
                        <% if (attribute.mustTrim()) { %>
                            if (<%= attribute.getName() %> != null) <%= attribute.getName() %> = <%= attribute.getName() %>.trim();
                        <% } %>
                        <% if (attribute.getType() instanceof StringJavaType && attribute.hasMaxLength() ) { %>
                            if (<%= attribute.getName() %> != null && <%= attribute.getName() %>.length() > <%= attribute.getMaxLength() %>)
                                <% if (attribute.truncate()) { %>
                                    <%= attribute.getName() %> = <%= attribute.getName() %>.substring(0, <%= attribute.getMaxLength() %> )<%if (attribute.mustTrim()) { %>.trim()<% } %>;
                                <% } else { %>
                                    throw new MithraBusinessException("Attribute '<%= attribute.getName() %>' cannot exceed maximum length of <%= attribute.getMaxLength() %>: " + <%= attribute.getName() %>);
                                <% } %>
                        <% } %>
                    <% } %>
                <% } %>
                DatedTransactionalBehavior behavior = zGetTransactionalBehaviorForWrite();
                <%= wrapper.getDataClassName() %> data = (<%= wrapper.getDataClassName() %>) behavior.getCurrentDataForRead(this);
                boolean dataChanged = true;
                AttributeUpdateWrapper updateWrapper;
                <% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
                Timestamp businessTo = data.get<%= StringUtility.firstLetterToUpper(wrapper.getBusinessDateAsOfAttributeName()) %>To();
                <% } %>
                <% for (EmbeddedValueMapping attribute : evo.getMappingsRecursively()) { %>
                    if (businessTo != null && businessTo.getTime() >= exclusiveUntil.getTime())
                    {
                        <% if (attribute.isPrimitive()) { %>
                            <% if (attribute.isNullablePrimitive()) { %>
                                if (data.<%= attribute.getNullGetter() %> && is<%= StringUtility.firstLetterToUpper(attribute.getName()) %>Null)
                                {
                                    dataChanged = false;
                                }
                                else
                                {
                                    dataChanged = data.<%= attribute.getGetter() %>() != <%= attribute.getName() %>;
                                }
                            <% } else { %>
                                dataChanged = data.<%= attribute.getGetter() %>() != <%= attribute.getName() %>;
                            <% } %>
                        <% } else { %>
                            if (data.<%= attribute.getGetter() %>() == null)
                            {
                                dataChanged = <%= attribute.getName() %> != null;
                            }
                            else
                            {
                                dataChanged = !data.<%= attribute.getGetter() %>().equals(<%= attribute.getName() %>);
                            }
                        <% } %>
                    }
                    if (dataChanged)
                    {
                    <% if (attribute.isPrimaryKey() && !attribute.isMutablePrimaryKey()) { %>
                        if (!behavior.maySetPrimaryKey()) throw new MithraBusinessException("cannot change the primary key");
                    <% } %>
                        updateWrapper = new <%= attribute.getType().getJavaTypeStringPrimary() %>UpdateWrapper(
                            <%= wrapper.getFinderClassName() %>.<%= attribute.getName() %>(),
                            behavior.getCurrentDataForWrite(this),
                            <%= attribute.getName() %>);
                        behavior.updateUntil(this, updateWrapper, <%= (attribute.isReadonly()) %>, exclusiveUntil, true);
                    }
                <% } %>
                this.zResetEmbeddedValueObjects(behavior);
            }
        <% } %>
    <% } %>

    protected void issuePrimitiveNullSetters(DatedTransactionalBehavior behavior, MithraDataObject data, boolean mustCheckCurrent)
    {
    <% if (wrapper.isTablePerClassSubClass()) { %>
        super.issuePrimitiveNullSetters(behavior, data, mustCheckCurrent);
    <% } %>
    <%for (int j = 0;j < nullablePrimitiveAttributes.length; j ++) {%>
        zNullify(behavior, data, <%= wrapper.getFinderClassName()%>.<%=nullablePrimitiveAttributes[j].getName()%>(), <%= (nullablePrimitiveAttributes[j].isReadonly() || nullablePrimitiveAttributes[j].isPrimaryKey())%>, mustCheckCurrent);
    <%}%>
    }

<% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
    protected boolean zCheckInfiniteBusinessDate(MithraDataObject data)
    {
        <%=wrapper.getDataClassName()%> data2 = (<%=wrapper.getDataClassName()%>) data;
        Timestamp businessTo = data2.get<%=StringUtility.firstLetterToUpper(wrapper.getBusinessDateAsOfAttributeName())%>To();
        return (businessTo != null && <%= wrapper.getFinderClassName() %>.<%= wrapper.getBusinessDateAsOfAttributeName() %>().getInfinityDate().getTime() ==
            businessTo.getTime());
    }
<% } %>


    <%for (int i = 0; i < nullablePrimitiveAttributes.length; i ++) {%>
    <%= nullablePrimitiveAttributes[i].getVisibility() %> void <%= nullablePrimitiveAttributes[i].getSetter() %>Null()
    {
        zNullify(<%= wrapper.getFinderClassName()%>.<%=nullablePrimitiveAttributes[i].getName()%>(), <%= nullablePrimitiveAttributes[i].isPrimaryKey() && !nullablePrimitiveAttributes[i].isMutablePrimaryKey()%>);
    }

    <% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
    <%= nullablePrimitiveAttributes[i].getVisibility() %> void <%= nullablePrimitiveAttributes[i].getSetter() %>NullUntil(Timestamp exclusiveUntil)
    {
        zNullifyUntil(<%= wrapper.getFinderClassName()%>.<%=nullablePrimitiveAttributes[i].getName()%>(), <%= nullablePrimitiveAttributes[i].isPrimaryKey() && !nullablePrimitiveAttributes[i].isMutablePrimaryKey()%>, exclusiveUntil);
    }
    <% } // has businessDate attribute %>
    <% } // nullable primitive attributes%>

    <% for (int i=0;i<asOfAttributes.length;i++) { %>
    public <%=asOfAttributes[i].isFinalGetter() ? "final " : ""%><%=asOfAttributes[i].getTypeAsString()%> <%=asOfAttributes[ i ].getGetter()%>()
    {
        return this.<%= asOfAttributes[i].getName()%>;
    }
    <% } %>
    <% if (wrapper.hasCascadableInPlaceUpdate()) { %>

    public void zCascadeUpdateInPlaceBeforeTerminate()
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.cascadeUpdateInPlaceBeforeTerminate(this);
    }

    public void zCascadeUpdateInPlaceBeforeTerminate(MithraDataObject _data)
    {
        <% if (wrapper.isTablePerClassSubClass()) { %>
            super.zCascadeUpdateInPlaceBeforeTerminate(_data);
        <% } %>
        <%=wrapper.getDataClassName()%> _newData = (<%=wrapper.getDataClassName()%>) _data;
        <% for (int i = 0; i < normalAttributes.length; i ++) {%>
            <% if (normalAttributes[i].isInPlaceUpdate()) { %>
                <% if (normalAttributes[i].isNullablePrimitive()) { %>
                if (_newData.<%= normalAttributes[i].getNullGetter()%>)
                {
                    this.<%= normalAttributes[i].getSetter()%>Null();
                }
                else
                {
                    this.<%= normalAttributes[i].getSetter()%>UsingInPlaceUpdate(_newData.<%= normalAttributes[i].getGetter()%>());
                }
                <% } else { %>
                this.<%= normalAttributes[i].getSetter()%>UsingInPlaceUpdate(_newData.<%= normalAttributes[i].getGetter()%>());
                <% } %>
            <% } %>
        <% } %>

    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
        <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent() && relationshipAttributes[i].getRelatedObject().hasCascadableInPlaceUpdate()) { %>
            <% if (!relationshipAttributes[i].isToMany()) { %>
            if (_newData.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation)
            {
                <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                    (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) ((NulledRelation)_newData.<%=relationshipAttributes[i].getGetter()%>()).getOriginal();
                if (<%= relationshipAttributes[i].getName()%> != null)
                {
                    <%= relationshipAttributes[i].getName()%>.zCascadeUpdateInPlaceBeforeTerminate();
                }
            }
            else
            <% } %>
            {
                <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                    (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _newData.<%=relationshipAttributes[i].getGetter()%>();
                if (<%= relationshipAttributes[i].getName()%> != null)
                {
                    <% if (relationshipAttributes[i].isToMany()) { %>
                    <%= relationshipAttributes[i].getName()%>.zCascadeUpdateInPlaceBeforeTerminate();
                    <% } else { %>
                    <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> _existing =
                        <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%= relationshipAttributes[i].getImplTypeAsString()%>)<%}%> this.<%=relationshipAttributes[i].getGetter()%>();
                    if (_existing != null)
                    {
                        _existing.zCascadeUpdateInPlaceBeforeTerminate(<%= relationshipAttributes[i].getName()%>.zGetTxDataForRead());
                    }
                    <%} %>
                }
            }

        <% } %>
    <% } %>
    }
    <% } %>

    public void zPersistDetachedRelationships(MithraDataObject _data)
    {
        <% if (wrapper.isTablePerClassSubClass()) { %>
            super.zPersistDetachedRelationships(_data);
        <% } %>
        <%=wrapper.getDataClassName()%> _newData = (<%=wrapper.getDataClassName()%>) _data;
    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
        <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
            <% if (!relationshipAttributes[i].isToMany()) { %>
            if (_newData.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation)
            {
                <% if (relationshipAttributes[i].getRelatedObject().hasCascadableInPlaceUpdate()) { %>
                <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> _detached =(<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) ((NulledRelation)_newData.<%=relationshipAttributes[i].getGetter()%>()).getOriginal();
                if (_detached != null)
                {
                    _detached.zCascadeUpdateInPlaceBeforeTerminate();
                }
                <% } %>
                <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                    <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%= relationshipAttributes[i].getImplTypeAsString()%>)<%}%> this.<%=relationshipAttributes[i].getGetter()%>();
                if (<%= relationshipAttributes[i].getName()%> != null)
                {
                    <%= relationshipAttributes[i].getName()%>.<%= relationshipAttributes[i].getRelatedObject().getCascadeDeleteOrTerminate()%>();
                }
            }
            else
            <% } %>
            {
                <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                    (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _newData.<%=relationshipAttributes[i].getGetter()%>();
                if (<%= relationshipAttributes[i].getName()%> != null)
                {
                    <% if (relationshipAttributes[i].isToMany()) { %>
                    <%= relationshipAttributes[i].getName()%>.copyDetachedValuesToOriginalOrInsertIfNewOr<%= StringUtility.firstLetterToUpper(relationshipAttributes[i].getRelatedObject().getDeleteOrTerminate())%>IfRemoved();
                    <% } else { %>
                    <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> _existing =
                        <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%= relationshipAttributes[i].getImplTypeAsString()%>)<%}%> this.<%=relationshipAttributes[i].getGetter()%>();
                    if (_existing == null)
                    {
                        <%= relationshipAttributes[i].getName()%>.copyDetachedValuesToOriginalOrInsertIfNew();
                    }
                    else
                    {
                        _existing.zCopyAttributesFrom(<%= relationshipAttributes[i].getName()%>.zGetTxDataForRead());
                        _existing.zPersistDetachedRelationships(<%= relationshipAttributes[i].getName()%>.zGetTxDataForRead());
                    }
                    <%} %>
                }
            }

        <% } %>
    <% } %>
    }

    public void zSetTxDetachedDeleted()
    {
        <% if (wrapper.isTablePerClassSubClass()) { %>
            super.zSetTxDetachedDeleted();
        <% } %>
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehavior();
        if (behavior.isDetached() && behavior.isDeleted()) return;
        <%=wrapper.getDataClassName()%> _newData = (<%=wrapper.getDataClassName()%>) behavior.getCurrentDataForRead(this);
    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
        <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
            if (_newData.<%=relationshipAttributes[i].getGetter()%>() != null && !(_newData.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation))
            {
                ((<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>)_newData.<%=relationshipAttributes[i].getGetter()%>()).zSetTxDetachedDeleted();
            }
        <% } %>
    <% } %>
        this.zSetTxPersistenceState(DatedPersistenceState.DETACHED_DELETED);
    }

    public void zSetNonTxDetachedDeleted()
    {
        <% if (wrapper.isTablePerClassSubClass()) { %>
            super.zSetNonTxDetachedDeleted();
        <% } %>
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehavior();
        <%=wrapper.getDataClassName()%> _newData = (<%=wrapper.getDataClassName()%>) behavior.getCurrentDataForRead(this);
    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
        <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
            if (_newData.<%=relationshipAttributes[i].getGetter()%>() != null && !(_newData.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation))
            {
                ((<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>)_newData.<%=relationshipAttributes[i].getGetter()%>()).zSetNonTxDetachedDeleted();
            }
        <% } %>
    <% } %>
        this.zSetNonTxPersistenceState(DatedPersistenceState.DETACHED_DELETED);
    }

    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
<%@ include file="../RelationshipJavaDoc.jspi" %>
	public <%=relationshipAttributes[i].isFinalGetter() ? "final " : ""%><%=relationshipAttributes[ i ].getTypeAsString()%> <%=relationshipAttributes[ i ].getGetter()%>(<%=relationshipAttributes[ i ].getParameters() %>)
    {
        <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> _result = null;
        Operation _op = null;
        DatedTransactionalBehavior _behavior = zGetTransactionalBehavior();
        <%=wrapper.getDataClassName()%> _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForRead(this);
        <% if (relationshipAttributes[i].needsPortal()) { %>
            MithraObjectPortal _portal = null;
        <% } %>
        <% if (relationshipAttributes[i].hasSetter()) { %>
        if (_behavior.isPersisted())
        <% } // has setter%>
        {
            <% String accessorFilters = relationshipAttributes[ i ].getFilterExpression(); %>
            <%=accessorFilters%>
            {
            <% if (relationshipAttributes[i].needsPortal()) { %>
                _portal = <%= relationshipAttributes[i].getRelatedObject().getFinderClassName()%>.getMithraObjectPortal();
            <% } %>
            <% if (relationshipAttributes[i].isToOneDirectReference()) { %>
                if (_behavior.isDirectReferenceAllowed())
                {
                    int currentCount = this.zGetPortal().getPerClassUpdateCountHolder().getNonTxUpdateCount();
                    if (this.classUpdateCount == currentCount)
                    {
                        Object o = <%=relationshipAttributes[i].getGetterForDirectRef()%>;
                        if (o instanceof NullPersistedRelation)
                        {
                            if (((NullPersistedRelation)o).isValid()) return null;
                        }
                        else
                        {
                            _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _portal.unwrapRelatedObject(<%=relationshipAttributes[i].getDirectRefHolder()%>, o, <%= relationshipAttributes[i].getDirectRefFromExtractorName()%>, <%= relationshipAttributes[i].getDirectRefToExtractorName()%>);
                            if (_result != null) return _result;
                        }
                    }
                    else
                    {
                        _data.clearAllDirectRefs();
                        this.zClearAllDirectRefs();
                        this.classUpdateCount = currentCount;
                    }
                }
            <% } // isToOneDirectReference%>
            <% if (relationshipAttributes[i].isToManyDirectReference()) { %>
                if (_behavior.isDirectReferenceAllowed())
                {
                    int currentCount = this.zGetPortal().getPerClassUpdateCountHolder().getNonTxUpdateCount();
                    if (this.classUpdateCount == currentCount)
                    {
                        Object o = <%=relationshipAttributes[i].getGetterForDirectRef()%>;
                        _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _portal.unwrapToManyRelatedObject(o);
                        if (_result != null)
                        {
                            <% if (relationshipAttributes[i].hasOrderBy()) { %>
                            _result.setOrderBy(<%= relationshipAttributes[i].getCompleteOrderByForRelationship() %>);
                            <% } // order by %>
                            return _result;
                        }
                    }
                    else
                    {
                        _data.clearAllDirectRefs();
                        this.zClearAllDirectRefs();
                        this.classUpdateCount = currentCount;
                    }
                }
            <% } // isToManyDirectReference%>
                <% if (relationshipAttributes[i].isFindByUnique()) { %>
                       _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) <%= relationshipAttributes[i].getRelatedObject().getFinderClassName()%>.<%= relationshipAttributes[i].getFindByUniqueMethodName() %>(<%= relationshipAttributes[i].getFindByUniqueParameters() %>);
                        <% if (relationshipAttributes[i].requiresOverSpecifiedParameterCheck()) { %>
                            if (_result != null && !(<%=relationshipAttributes[i].getOverSpecificationCheck()%>)) _result = null;
                        <% } %>
                <% } else { %>
                <% if (relationshipAttributes[i].isResolvableInCache()) { %>
                Object _related = _portal.<%= relationshipAttributes[i].getCacheLookupMethod()%>FromCache(this, _data, <%= relationshipAttributes[i].getRhs()%><%= relationshipAttributes[i].getCacheLookUpParameters()%>);
                if (!(_related instanceof NulledRelation)) _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _related;
                    <% if (relationshipAttributes[i].requiresOverSpecifiedParameterCheck()) { %>
                        if (_result != null && !(<%=relationshipAttributes[i].getOverSpecificationCheck()%>)) _result = null;
                    <% } %>
                if (_related == null)
                <% } %>
                {
                    _op = <%=relationshipAttributes[ i ].getOperationExpression()%>;
                }
                <% } // isFindByUnique %>
            }
            <%if ( (accessorFilters.length() > 0) && (relationshipAttributes[i].isListType()) )  {%>
            else {
                _result = new <%=relationshipAttributes[i].getListClassName()%>( new None (
                                 <%=relationshipAttributes[i].getRelatedObject().getClassName()+"Finder."+relationshipAttributes[i].getRelatedObject().getPrimaryKeyAttributes()[0].getName()+"()"%>));
            }
            <% } %>
            <%if ( (accessorFilters.length() > 0) && !relationshipAttributes[i].isListType() )  {%>
            else {
                return null;
            }
            <% } %>
        }
        <% if (relationshipAttributes[i].hasSetter()) { %>
        else if (_behavior.isDetached())
        {
            <% if (relationshipAttributes[i].isRelatedDependent() || relationshipAttributes[i].hasParentContainer()) { %>
            <% if (!relationshipAttributes[i].isToMany()) { %>
            if (_data.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation)
            {
                return null;
            }
            <% } %>
            _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _data.<%=relationshipAttributes[i].getGetter()%>();
            if (_result == null)
            <% } %>
            {
            <% accessorFilters = relationshipAttributes[ i ].getFilterExpression(); %>
            <%=accessorFilters%>
            {
            Operation detachedOp = <%=relationshipAttributes[ i ].getOperationExpression()%>;
            _result = <%=relationshipAttributes[ i ].getGetterExpressionForDetached("detachedOp")%>;

            <% if (relationshipAttributes[i].isToMany()) { %>
            _result.zSetForRelationship();
            <% } %>
            <%if(relationshipAttributes[ i ].mustDetach()){%>
            if(_result != null)
            {
               _result = _result.getDetachedCopy();
            }
            <%}%>

            <% if (relationshipAttributes[i].mustPersistRelationshipChanges() && relationshipAttributes[i].isToMany()) { %>
                _result.zSetAddHandler(new <%= relationshipAttributes[i].getAddHandlerName()%>InMemory());
            <% } %>
            }
            <%if ( (accessorFilters.length() > 0) && (relationshipAttributes[i].isListType()) )  {%>
            else {
                _result = new <%=relationshipAttributes[i].getListClassName()%>( new None (
                                 <%=relationshipAttributes[i].getRelatedObject().getClassName()+"Finder."+relationshipAttributes[i].getRelatedObject().getPrimaryKeyAttributes()[0].getName()+"()"%>));
            }
            <% } // initialize list type %>

            <% if (relationshipAttributes[i].hasOrderBy()) { %>
            _result.setOrderBy(<%= relationshipAttributes[i].getCompleteOrderByForRelationship() %>);
            <% } // order by %>
            <% if (relationshipAttributes[i].isRelatedDependent()) { %>
                _behavior = zGetTransactionalBehaviorForWrite();
                _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);
                _data.<%= relationshipAttributes[i].getSetter()%>(_result);
                <% if (relationshipAttributes[i].isBidirectional()) { %>
                if (_result != null) _result.zSetParentContainer<%=relationshipAttributes[i].getReverseName()%>(this);
                <% } %>
            <% } else if (relationshipAttributes[i].mustDetach()) {%>
                _behavior = zGetTransactionalBehaviorForWrite();
                _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);
                _data.<%= relationshipAttributes[i].getSetter()%>(_result);
            <% } %>
            }
        }
        else if (_behavior.isInMemory())
        {
            _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _data.<%=relationshipAttributes[i].getGetter()%>();
            <% if (relationshipAttributes[i].isRelatedDependent()) { %>
                <% if (relationshipAttributes[i].isToMany()) { %>
                if (_result == null)
                {
                    _result = <%=relationshipAttributes[ i ].getGetterExpressionForOperation("") %>;
                    _result.zSetAddHandler(new <%= relationshipAttributes[i].getAddHandlerName()%>InMemory());
                    _behavior = zGetTransactionalBehaviorForWrite();
                    _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);
                    _data.<%= relationshipAttributes[i].getSetter()%>(_result);
                }
                <% } %>
            <% } else { %>
            if (_result == null)
            {
                <% accessorFilters = relationshipAttributes[ i ].getFilterExpression(); %>
                <%=accessorFilters%>
                {
                    _op = <%=relationshipAttributes[ i ].getOperationExpression()%>;
                }
                <%if ( (accessorFilters.length() > 0) && (relationshipAttributes[i].isListType()) )  {%>
                else {
                    _result = new <%=relationshipAttributes[i].getListClassName()%>( new None (
                                     <%=relationshipAttributes[i].getRelatedObject().getClassName()+"Finder."+relationshipAttributes[i].getRelatedObject().getPrimaryKeyAttributes()[0].getName()+"()"%>));
                }
                <% } %>
            }
            <% } %>
        }
        <% } %>
        <% if (!relationshipAttributes[i].isFindByUnique()) { %>
        if (_op != null)
        {
            _result = <%=relationshipAttributes[ i ].getGetterExpressionForOperation("_op")%>;

            <% if (relationshipAttributes[i].getCardinality().isToMany()) { %>
            _result.zSetForRelationship();
                <% if (relationshipAttributes[i].mustPersistRelationshipChanges()) { %>
                    _result.zSetRemoveHandler(<%= relationshipAttributes[i].getRelatedObject().getRelationshipRemoveHandlerClass()%>.getInstance());
                    _result.zSetAddHandler(new <%= relationshipAttributes[i].getAddHandlerName()%>Persisted());
                <% } %>
            <% } %>
            <% if (relationshipAttributes[i].hasOrderBy()) { %>
            _result.setOrderBy(<%= relationshipAttributes[i].getCompleteOrderByForRelationship() %>);
            <% } // order by %>
        }
        <% } %>
        <% if (relationshipAttributes[i].isDirectReference()) { %>
        if (_behavior.isDirectReferenceAllowed())
        {
            <%= relationshipAttributes[i].getSetterForDirectRef()%>(_portal.wrapRelatedObject(_result));
        }
        <% } // isDirectReference%>
        return _result;
    }

    <% if (relationshipAttributes[i].hasSetter()) { %>
	public void <%=relationshipAttributes[ i ].getSetter()%>(<%=relationshipAttributes[ i ].getTypeAsString()%> <%= relationshipAttributes[i].getName()%>)
    {
        <% if (relationshipAttributes[i].hasParentContainer() && !relationshipAttributes[i].isFromMany()) { %>
        ((<%=relationshipAttributes[i].getMithraImplTypeAsString()%>)<%= relationshipAttributes[i].getName()%>).set<%= StringUtility.firstLetterToUpper(relationshipAttributes[i].getReverseName())%>((<%= wrapper.getClassName()%>)this);
        <% } else { %>
        <%=relationshipAttributes[i].getMithraImplTypeAsString()%> _<%= relationshipAttributes[i].getName()%> = (<%=relationshipAttributes[i].getMithraImplTypeAsString()%>) <%= relationshipAttributes[i].getName()%>;
        DatedTransactionalBehavior _behavior = zGetTransactionalBehaviorForWrite();
        <%=wrapper.getDataClassName()%> _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);
        if (_behavior.isInMemory())
        {
            <% if (relationshipAttributes[i].isRelatedDependent()) { %>
                <% if (relationshipAttributes[i].isToMany()) { %>
                if (_behavior.isDetached() && _<%= relationshipAttributes[i].getName()%> != null)
                {
                    _<%= relationshipAttributes[i].getName()%>.zMakeDetached(<%=relationshipAttributes[ i ].getOperationExpression()%>,
                        _data.<%= relationshipAttributes[i].getGetter()%>());
                }
                <% } else {%>
                Object _prev = _data.<%= relationshipAttributes[i].getGetter()%>();
                <% } %>
            <% } %>
        <% if (relationshipAttributes[i].hasParentContainer() && relationshipAttributes[i].isFromMany()) { %>
            Object _prev = _data.<%= relationshipAttributes[i].getGetter()%>();
            if (_behavior.isDetached() && _prev != null)
            {
                ((DelegatingList)((<%=relationshipAttributes[i].getMithraImplTypeAsString()%>)_prev).get<%= StringUtility.firstLetterToUpper(relationshipAttributes[i].getReverseName())%>()).zMarkMoved( (<%=wrapper.getImplClassName()%>) this);
            }
        <% } %>
            _data.<%=relationshipAttributes[ i ].getSetter()%>(_<%= relationshipAttributes[i].getName()%>);
        <% if (relationshipAttributes[i].hasParentContainer()) { %>
            <% if (relationshipAttributes[i].isFromMany()) { %>
                _<%= relationshipAttributes[i].getName()%>.get<%= StringUtility.firstLetterToUpper(relationshipAttributes[i].getReverseName())%>().add((<%=wrapper.getImplClassName()%>) this);
            <% } else { %>
            <% } %>
        <% } else if (relationshipAttributes[i].isRelatedDependent()) { %>
            if (_<%= relationshipAttributes[i].getName()%> != null)
            {
            <% Attribute[] attributesToSet = relationshipAttributes[i].getAttributesToSetOnRelatedObject(); %>
            <% for (int r = 0; r < attributesToSet.length ; r++) if (!attributesToSet[r].isAsOfAttribute()) { %>
            _<%= relationshipAttributes[i].getName() %>.<%= attributesToSet[r].getSetter()%>(_data.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getGetter() %>());
            <% } %>
            <% if (relationshipAttributes[i].isBidirectional()) { %>
                _<%= relationshipAttributes[i].getName() %>.zSetParentContainer<%=relationshipAttributes[i].getReverseName()%>(this);
            <% } %>
            <% if (relationshipAttributes[i].isToMany()) { %>
                _<%= relationshipAttributes[i].getName()%>.zSetAddHandler(new <%= relationshipAttributes[i].getAddHandlerName()%>InMemory());
            <% } %>
            }
            else if (_behavior.isDetached())
            {
                <% if (relationshipAttributes[i].isToMany()) { %>
                throw new MithraBusinessException("to-many relationships cannot be set to null. Use the clear() method on the list instead.");
                <% } else { %>
                _data.<%=relationshipAttributes[ i ].getSetter()%>(NulledRelation.create(_prev));
                if (_prev != null && !(_prev instanceof NulledRelation)
                    && (!((MithraTransactionalObject)_prev).isInMemoryAndNotInserted() || ((MithraTransactionalObject)_prev).zIsDetached()))
                {
                    <% if (relationshipAttributes[i].getRelatedObject().hasAsOfAttributes()) { %>
                    ((MithraDatedTransactionalObject)_prev).terminate();
                    <% } else { %>
                    ((MithraTransactionalObject)_prev).delete();
                    <% } %>
                }
                <% } %>
            }
        <% } else if (relationshipAttributes[i].canSetLocalAttributesFromRelationship()) { %>
            <% Attribute[] attributesToSet = relationshipAttributes[i].getAttributesToSetOnRelatedObject(); %>
            if (_<%= relationshipAttributes[i].getName()%> == null)
            {
            <% for (int r = 0; r < attributesToSet.length ; r++) if (!relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).isAsOfAttribute()) { %>
                <%if (relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).isNullablePrimitive()) {%>
                    this.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getSetter()%>Null();
                <%} else if (relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).isNullable()) {%>
                    this.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getSetter()%>(null);
                <%} else {%>
                    this.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getSetter() %>(<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getType().parseLiteralAndCast(relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getType().getDefaultInitialValue())%>);
                <%} %>
            <% } %>
            }
            else
            {
            <% for (int r = 0; r < attributesToSet.length ; r++) if (!relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).isAsOfAttribute()) { %>
                this.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getSetter() %>(<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getPrimitiveCastType(attributesToSet[r])%>
                    _<%= relationshipAttributes[i].getName() %>.<%= attributesToSet[r].getGetter()%>());
            <% } %>
            }
        <% } %>
        }
        else if (_behavior.isPersisted())
        {
        <% if (relationshipAttributes[i].isRelatedDependent()) { %>
            <% Attribute[] attributesToSet = relationshipAttributes[i].getAttributesToSetOnRelatedObject(); %>
            <% if (relationshipAttributes[i].isToMany()) { %>
                _<%= relationshipAttributes[i].getName()%>.zSetAddHandler(new <%= relationshipAttributes[i].getAddHandlerName()%>Persisted());
                <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%>ToDelete = new <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>();
                <%= relationshipAttributes[i].getName()%>ToDelete.addAll(this.<%= relationshipAttributes[i].getGetter()%>());
                for(int i=0;i < _<%= relationshipAttributes[i].getName()%>.size(); i++)
                {
                    <%=relationshipAttributes[i].getRelatedObject().getImplClassName()%> item = _<%= relationshipAttributes[i].getName()%>.get<%=relationshipAttributes[i].getRelatedObject().getClassName()%>At(i);
                    if (!<%= relationshipAttributes[i].getName()%>ToDelete.remove(item))
                    {
                        <% for (int r = 0; r < attributesToSet.length ; r++) if (!attributesToSet[r].isAsOfAttribute()) { %>
                        item.<%= attributesToSet[r].getSetter()%>(_data.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getGetter() %>());
                        <% } %>
                        item.cascadeInsert();
                    }
                }
                <%= relationshipAttributes[i].getName()%>ToDelete.<%= relationshipAttributes[i].getRelatedObject().getCascadeDeleteOrTerminate()%>All();
            <% } else { %>
                <%=relationshipAttributes[ i ].getImplTypeAsString()%> _existing = <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%= relationshipAttributes[i].getImplTypeAsString()%>)<%}%>this.<%= relationshipAttributes[i].getGetter()%>();
                if (_<%= relationshipAttributes[i].getName()%> != _existing)
                {
                    if (_existing != null)
                    {
                        _existing.<%= relationshipAttributes[i].getRelatedObject().getCascadeDeleteOrTerminate()%>();
                    }
                    if (_<%= relationshipAttributes[i].getName()%> != null)
                    {
                        <% for (int r = 0; r < attributesToSet.length ; r++) if (!attributesToSet[r].isAsOfAttribute()) { %>
                        _<%= relationshipAttributes[i].getName()%>.<%= attributesToSet[r].getSetter()%>(_data.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getGetter() %>());
                        <% } %>
                        _<%= relationshipAttributes[i].getName()%>.cascadeInsert();
                    }
                }
            <% } %>
        <% } else if (relationshipAttributes[i].canSetLocalAttributesFromRelationship()) { %>
            <% Attribute[] attributesToSet = relationshipAttributes[i].getAttributesToSetOnRelatedObject(); %>
            if (_<%= relationshipAttributes[i].getName()%> == null)
            {
            <% for (int r = 0; r < attributesToSet.length ; r++) if (!relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).isAsOfAttribute()) { %>
                <%if (relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).isNullablePrimitive()) {%>
                    this.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getSetter()%>Null();
                <%} else if (relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).isNullable()) {%>
                    this.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getSetter()%>(null);
                <%} else {%>
                    this.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getSetter() %>(<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getType().parseLiteralAndCast(relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getType().getDefaultInitialValue())%>);
                <%} %>
            <% } %>
            }
            else
            {
            <% for (int r = 0; r < attributesToSet.length ; r++) if (!relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).isAsOfAttribute()) { %>
                this.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getSetter() %>(<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getPrimitiveCastType(attributesToSet[r])%>
                    _<%= relationshipAttributes[i].getName() %>.<%= attributesToSet[r].getGetter()%>());
            <% } %>
            }
        <% } %>
        }
        else throw new RuntimeException("not implemented");
        <% } // set via parent%>
    }
    <% if (relationshipAttributes[i].hasParentContainer()) { %>
    public void zSetParentContainer<%=relationshipAttributes[i].getName()%>(<%= relationshipAttributes[i].getRelatedObject().getAbstractClassName()%> parent)
    {
        DatedTransactionalBehavior _behavior = zGetTransactionalBehaviorForWrite();
        <%=wrapper.getDataClassName()%> _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);
        if (_behavior.isInMemory())
        {
            _data.<%=relationshipAttributes[ i ].getSetter()%>(parent);
        }
    }
    <% } %>
    <% } // has setter %>
    <%}//accessors for relationship end%>

    public Cache zGetCache()
    {
        return <%= wrapper.getClassName()%>Finder.getMithraObjectPortal().getCache();
    }

    private Logger getLogger()
    {
        return logger;
    }

    public MithraDataObject zAllocateData()
    {
        return new <%= wrapper.getOnHeapDataClassName()%>();
    }

    public MithraDataObject zRefreshWithLock(boolean lock)
    {
        <%= wrapper.getDataClassName()%> data = (<%= wrapper.getDataClassName()%>) <%= wrapper.getClassName()%>Finder.getMithraObjectPortal().refreshDatedObject(this, lock);
        if (data == null)
        {
            throw new MithraDeletedException("<%= wrapper.getClassName()%> has been deleted.");
        }
        return data;
    }

    public void setFrom<%=wrapper.getDataClassName()%>( <%=wrapper.getDataClassName()%> data )
    {
        this.zSetData(data);
    }

    protected void zSetFrom<%=wrapper.getDataClassName()%>( <%=wrapper.getDataClassName()%> data )
    {
        this.zSetData(data);
        this.zSetNonTxPersistenceState(PERSISTED_STATE);
    }

    public MithraTransactionalDatabaseObject zGetDatabaseObject()
    {
        return (MithraTransactionalDatabaseObject) <%= wrapper.getClassName()%>Finder.getMithraObjectPortal().getDatabaseObject();
    }

    public MithraObjectPortal zGetPortal()
    {
        return <%= wrapper.getClassName()%>Finder.getMithraObjectPortal();
    }

    public <%=wrapper.getImplClassName()%> getOriginalPersistentObject()
    {
        return this.zFindOriginal();
    }

    public void copyNonPrimaryKeyAttributesFrom(<%= wrapper.getAbstractClassName() %> from) throws MithraBusinessException
    {
        this.copyNonPrimaryKeyAttributesFrom((MithraTransactionalObject)from);
    }

    protected void copyNonPrimaryKeyAttributesFromImpl(MithraTransactionalObject f, MithraTransaction tx) throws MithraBusinessException
    {
        <% if (wrapper.isTablePerClassSubClass()) { %>
            super.copyNonPrimaryKeyAttributesFromImpl(f, tx);
        <% } %>
        <%= wrapper.getAbstractClassName() %> from = (<%= wrapper.getAbstractClassName() %>) f;
        <%=wrapper.getDataClassName()%> newData = from.zSynchronizedGetData();
        <% for (int i = 0; i < normalAttributes.length; i ++) if (!normalAttributes[i].isPrimaryKey()
                && !normalAttributes[i].isSourceAttribute()
                && !normalAttributes[i].isAsOfAttributeFrom()
                && !normalAttributes[i].isAsOfAttributeTo()
        ) {%>
            <% if (normalAttributes[i].isNullablePrimitive()) { %>
            if (from.<%= normalAttributes[i].getNullGetter()%>)
            {
                this.<%= normalAttributes[i].getSetter()%>Null();
            }
            else
            {
                this.<%= normalAttributes[i].getSetter()%>(newData.<%= normalAttributes[i].getGetter()%>());
            }
            <% } else { %>
            this.<%= normalAttributes[i].getSetter()%>(newData.<%= normalAttributes[i].getGetter()%>());
            <% } %>
        <% } %>
    }

    <%if(wrapper.hasBusinessDateAsOfAttribute()){%>

    public void zPersistDetachedRelationshipsUntil(MithraDataObject _data, Timestamp exclusiveUntil)
    {
        <%=wrapper.getDataClassName()%> _newData = (<%=wrapper.getDataClassName()%>) _data;

    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
        <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
            <% if (!relationshipAttributes[i].isToMany()) { %>
            if (_newData.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation)
            {
                <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                    <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%= relationshipAttributes[i].getImplTypeAsString()%>)<%}%> this.<%=relationshipAttributes[i].getGetter()%>();
                if (<%= relationshipAttributes[i].getName()%> != null)
                {
                    <%= relationshipAttributes[i].getName()%>.<%= relationshipAttributes[i].getRelatedObject().getCascadeDeleteOrTerminate()%>();
                }
            }
            else
            {
            <% } %>

                <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                    (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _newData.<%=relationshipAttributes[i].getGetter()%>();
                if (<%= relationshipAttributes[i].getName()%> != null)
                {
                    <% if (relationshipAttributes[i].isToMany()) { %>
                        <%= relationshipAttributes[i].getName()%>.<%= relationshipAttributes[i].getRelatedObject().getCopyDetachedValueUntilMethodNameForList()%>;
                    <% } else { %>
                    <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> _existing =
                        <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%= relationshipAttributes[i].getImplTypeAsString()%>)<%}%> this.<%=relationshipAttributes[i].getGetter()%>();
                    if (_existing == null)
                    {
                        <%= relationshipAttributes[i].getName()%>.<%= relationshipAttributes[i].getRelatedObject().getCopyDetachedValueUntilMethodName()%>;
                    }
                    else
                    {
                        _existing.<%= relationshipAttributes[i].getRelatedObject().getCopyNonPrimaryKeyAttributesUntilMethodName(relationshipAttributes[i].getName())%>;
                    }
                    <%} %>
                }
            <% if (!relationshipAttributes[i].isToMany()) { %>
            }
            <% } %>

        <% } %>
    <% } %>
    }

    public void zInsertRelationshipsUntil(MithraDataObject _data, Timestamp exclusiveUntil)
    {
        <%=wrapper.getDataClassName()%> _newData = (<%=wrapper.getDataClassName()%>) _data;

    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
        <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
            <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _newData.<%=relationshipAttributes[i].getGetter()%>();
            if (<%= relationshipAttributes[i].getName()%> != null)
            {
                <% if (relationshipAttributes[i].isToMany()) { %>
                     <%= relationshipAttributes[i].getName()%>.<%= relationshipAttributes[i].getRelatedObject().getCopyDetachedValueUntilMethodNameForList()%>;
                <% } else { %>
                     <%= relationshipAttributes[i].getName()%>.<%= relationshipAttributes[i].getRelatedObject().getCopyDetachedValueUntilMethodName()%>;
                <%} %>
            }
        <% } %>
    <% } %>
    }

    public void copyNonPrimaryKeyAttributesUntilFrom(<%= wrapper.getAbstractClassName() %> from, Timestamp exclusiveUntil) throws MithraBusinessException
    {
        this.copyNonPrimaryKeyAttributesUntilFrom((MithraDatedTransactionalObject)from, exclusiveUntil);
    }

    protected void copyNonPrimaryKeyAttributesUntilFromImpl(MithraDatedTransactionalObject f, Timestamp exclusiveUntil, MithraTransaction tx) throws MithraBusinessException
    {
        <%= wrapper.getAbstractClassName() %> from = (<%= wrapper.getAbstractClassName() %>)f;
            <%=wrapper.getDataClassName()%> newData = from.zSynchronizedGetData();
            <% for (int i = 0; i < normalAttributes.length; i ++) if (!normalAttributes[i].isPrimaryKey()
                && !normalAttributes[i].isSourceAttribute()
                && !normalAttributes[i].isAsOfAttributeFrom()
                && !normalAttributes[i].isAsOfAttributeTo()
        ) {%>
                <% if (normalAttributes[i].isNullablePrimitive()) { %>
            if (from.<%= normalAttributes[i].getNullGetter()%>)
            {
                this.<%= normalAttributes[i].getSetter()%>NullUntil(exclusiveUntil);
            }
            else
            {
                this.<%= normalAttributes[i].getSetter()%>Until(newData.<%= normalAttributes[i].getGetter()%>(), exclusiveUntil);
            }
                <% } else { %>
            this.<%= normalAttributes[i].getSetter()%>Until(newData.<%= normalAttributes[i].getGetter()%>(), exclusiveUntil);
                <% } %>
            <% } %>
    }

    <%}%>

    <% for(int i=0;i<transactionalMethodSignatures.length;i++) { %>
    <%= transactionalMethodSignatures[i].getImplMethodSignature()%>

    <%= transactionalMethodSignatures[i].getOriginalMethodSignature()%>
    {
        <% if (!transactionalMethodSignatures[i].isVoid()) { %> return <% } %>
        this.<%= transactionalMethodSignatures[i].getMethodName()%>(<%= transactionalMethodSignatures[i].getMethodParametersNoType()%> 0);
    }

    <% if ((!transactionalMethodSignatures[i].getOriginalMethodSignature().contains("copyNonPrimaryKeyAttributesFrom")) ||
        (!transactionalMethodSignatures[i].getOriginalMethodSignature().contains("("+wrapper.getImplClassName()+" "))) { %>
    <%= transactionalMethodSignatures[i].getMethodSignatureWithRetryCount()%>
    {
        <% if (!transactionalMethodSignatures[i].isVoid()) { %>
        <%= transactionalMethodSignatures[i].getReturnType() %> _result = <%= transactionalMethodSignatures[i].getDefaultInitialValueForReturnType() %>;
        <% } %>
        MithraTransaction _tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean _nested = _tx != null;
        for(_retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES - _retryCount; _retryCount > 0; )
        {
            try
            {
                if (!_nested)
                {
                    _tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    _tx.setTransactionName("<%= wrapper.getImplClassName()+"."+transactionalMethodSignatures[i].getMethodName() %>");
                }
<%      if (transactionalMethodSignatures[i].isVoid()) { %>
                this.<%= transactionalMethodSignatures[i].getMethodName() %>Impl(<%= transactionalMethodSignatures[i].getMethodParametersNoType() %> _tx);
<%      } else {%>
                _result = <%= transactionalMethodSignatures[i].getMethodName() %>Impl(<%= transactionalMethodSignatures[i].getMethodParametersNoType() %> _tx);
<%      } %>
                if (!_nested) _tx.commit();
                break;
            }
<%      List exceptions = transactionalMethodSignatures[i].getExceptions();
        for (int count=0;count < exceptions.size();count++)
        {
         String exceptionName = (String) exceptions.get(count);
%>        catch(<%= exceptionName %> _mithra_e_<%= count %>)
        {
            this.getLogger().error("<%= transactionalMethodSignatures[i].getMethodName() %> rolled back tx ", _mithra_e_<%= count %>);
            if (!_nested && _tx != null)
            {
                _tx.rollback();
            }
            throw _mithra_e_<%= count %>;
        }
<%      } %>
            catch(Throwable _t)
            {
                if (_nested) MithraTransaction.handleNestedTransactionException(_tx, _t);
                _retryCount = MithraTransaction.handleTransactionException(_tx, _t, _retryCount);
            }
        }
<% if (!transactionalMethodSignatures[i].isVoid()) { %>
        return _result;
<% } %>
    }
<% } %>
<% } %>

<% if(wrapper.hasBusinessDateAsOfAttribute()){%>
    <% for(int i=0;i<datedTransactionalMethodSignatures.length;i++) { %>
        <%= datedTransactionalMethodSignatures[i].getImplMethodSignature()%>

        <%= datedTransactionalMethodSignatures[i].getOriginalMethodSignature()%>
    {
            <% if (!datedTransactionalMethodSignatures[i].isVoid()) { %> return <% } %>
        this.<%= datedTransactionalMethodSignatures[i].getMethodName()%>(<%= datedTransactionalMethodSignatures[i].getMethodParametersNoType()%> 0);
    }

    <% if ((!datedTransactionalMethodSignatures[i].getOriginalMethodSignature().contains("copyNonPrimaryKeyAttributesUntilFrom")) ||
        (!datedTransactionalMethodSignatures[i].getOriginalMethodSignature().contains("("+wrapper.getImplClassName()+" "))) { %>
        <%= datedTransactionalMethodSignatures[i].getMethodSignatureWithRetryCount()%>
    {
        <% if (!datedTransactionalMethodSignatures[i].isVoid()) { %>
        <%= datedTransactionalMethodSignatures[i].getReturnType() %> _result = <%= datedTransactionalMethodSignatures[i].getDefaultInitialValueForReturnType() %>;
        <% } %>
        MithraTransaction _tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean _nested = _tx != null;
        for(_retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES - _retryCount; _retryCount > 0; )
        {
            try
            {
                if (!_nested)
                {
                    _tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    _tx.setTransactionName("<%= wrapper.getImplClassName()+"."+datedTransactionalMethodSignatures[i].getMethodName() %>");
                }
<%      if (datedTransactionalMethodSignatures[i].isVoid()) { %>
                this.<%= datedTransactionalMethodSignatures[i].getMethodName() %>Impl(<%= datedTransactionalMethodSignatures[i].getMethodParametersNoType() %> _tx);
<%      } else {%>
                _result = <%= datedTransactionalMethodSignatures[i].getMethodName() %>Impl(<%= datedTransactionalMethodSignatures[i].getMethodParametersNoType() %> _tx);
<%      } %>
                if (!_nested) _tx.commit();
                break;
            }
<%      List exceptions = datedTransactionalMethodSignatures[i].getExceptions();
        for (int count=0;count < exceptions.size();count++)
        {
         String exceptionName = (String) exceptions.get(count);
%>        catch(<%= exceptionName %> _mithra_e_<%= count %>)
        {
            this.getLogger().error("<%= datedTransactionalMethodSignatures[i].getMethodName() %> rolled back tx ", _mithra_e_<%= count %>);
            if (!_nested && _tx != null)
            {
                _tx.rollback();
            }
            throw _mithra_e_<%= count %>;
        }
<%      } %>
            catch(Throwable _t)
            {
                if (_nested) MithraTransaction.handleNestedTransactionException(_tx, _t);
                _retryCount = MithraTransaction.handleTransactionException(_tx, _t, _retryCount);
            }
        }
<% if (!datedTransactionalMethodSignatures[i].isVoid()) { %>
        return _result;
<% } %>
     }
    <% } %>
<%}%>
<%}%>

<% if (wrapper.hasProcessingDate()) { %>
    protected void zCheckOptimisticLocking(MithraTransaction tx, MithraDataObject d, MithraDataObject nd)
    {
        <%=wrapper.getDataClassName()%> newData = (<%=wrapper.getDataClassName()%>) d;
        <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) nd;
        if (<%=wrapper.getFinderClassName()%>.getMithraObjectPortal().getTxParticipationMode(tx).isOptimisticLocking()
                && !tx.retryOnOptimisticLockFailure() && !newData.<%= wrapper.getProcessingDateAttribute().getGetter()%>From().equals(data.<%= wrapper.getProcessingDateAttribute().getGetter()%>From()))
        {
            throw new MithraOptimisticLockException("Optimistic lock failure. "+data.zGetPrintablePrimaryKey());
        }
    }
<% } %>

    protected boolean issueUpdates(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData)
    {
        boolean changed = false;
        <% if (wrapper.isTablePerClassSubClass()) { %>
            changed |= super.issueUpdates(behavior, data, newData);
        <% } %>
        <% for(int i=0;i < normalAttributes.length; i++) { %>
            <% if (!(normalAttributes[i].isAsOfAttributeFrom() || normalAttributes[i].isAsOfAttributeTo())){ %>
                changed |= zUpdate<%= normalAttributes[i].getType().getJavaTypeStringPrimary()%>(behavior, data, newData, <%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(), <%= (normalAttributes[i].isReadonly())%>);

            <% } %>
        <% } %>
        return changed;
    }

<% if (wrapper.hasBusinessDateAsOfAttribute()) { %>
    protected boolean issueUpdatesUntil(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, Timestamp until)
    {
        boolean changed = false;
        <% if (wrapper.isTablePerClassSubClass()) { %>
            changed |= super.issueUpdatesUntil(behavior, data, newData, until);
        <% } %>
        <% for(int i=0;i < normalAttributes.length; i++) { %>
            <% if (!(normalAttributes[i].isAsOfAttributeFrom() || normalAttributes[i].isAsOfAttributeTo())){ %>
                changed |= zUpdate<%= normalAttributes[i].getType().getJavaTypeStringPrimary()%>(behavior, data, newData, <%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(), <%= (normalAttributes[i].isReadonly())%>, until);

            <% } %>
        <% } %>
        return changed;
    }
<% } %>
    public void cascadeTerminate()
    {
      <% if (wrapper.hasDependentRelationships()) { %>
        <% for (int i=0;i<relationshipAttributes.length;i++)
            if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()
            && relationshipAttributes[i].getRelatedObject().isTransactional()) { %>
            <% if (relationshipAttributes[i].isToMany()) { %>
                <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>((<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>)<%}%>
                this.<%=relationshipAttributes[i].getGetter()%>()<%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>)<%}%>.<%= relationshipAttributes[i].getRelatedObject().getCascadeDeleteOrTerminate()%>All();
            <% } else { %>
                {
                    <%= relationshipAttributes[i].getImplTypeAsString()%> related = <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%= relationshipAttributes[i].getImplTypeAsString()%>)<%}%>this.<%=relationshipAttributes[i].getGetter()%>();
                    if (related != null)
                        related.<%= relationshipAttributes[i].getRelatedObject().getCascadeDeleteOrTerminate()%>();
                }
            <% } %>
        <% } %>
      <% } %>
        this.terminate();
    }

    public void cascadeTerminateUntil(Timestamp exclusiveUntil) throws MithraBusinessException
    {
      <% if (wrapper.hasDependentRelationships()) { %>
        <% for (int i=0;i<relationshipAttributes.length;i++)
            if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()
            && relationshipAttributes[i].getRelatedObject().isTransactional()
                    && relationshipAttributes[i].getRelatedObject().hasBusinessDateAsOfAttribute()) { %>
            <% if (relationshipAttributes[i].isToMany()) { %>
               <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>((<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>)<%}%>
               this.<%=relationshipAttributes[i].getGetter()%>()<%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>)<%}%>.cascadeTerminateAllUntil(exclusiveUntil);
            <% } else { %>
                {
                    <%= relationshipAttributes[i].getImplTypeAsString()%> related = <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%= relationshipAttributes[i].getImplTypeAsString()%>)<%}%>this.<%=relationshipAttributes[i].getGetter()%>();
                    if (related != null)
                        related.cascadeTerminateUntil(exclusiveUntil);
                }
            <% } %>
        <% } %>
      <% } %>
        this.terminateUntil(exclusiveUntil);
    }


    public void zSerializePrimaryKey(ObjectOutput out) throws IOException
    {
        <%=wrapper.getDataClassName()%> data = ((<%= wrapper.getDataClassName()%>)this.zGetCurrentOrTransactionalData());
        data.zSerializePrimaryKey(out);
        <% for (int i=0;i<asOfAttributes.length;i++) { %>
        <%= asOfAttributes[i].getSerializationStatement(asOfAttributes[i].getName())%>;
        <% } %>
    }

    public void zWriteDataClassName(ObjectOutput out) throws IOException
    {
<% if (wrapper.isTablePerClassSubClass() || wrapper.isTablePerClassSuperClass()) { %>
        out.writeObject("<%= wrapper.getPackageName()%>.<%= wrapper.getDataClassName()%>");
<% } %>
    }

    public void zSerializeFullData(ObjectOutput out) throws IOException
    {
        <%=wrapper.getDataClassName()%> data = ((<%= wrapper.getDataClassName()%>)this.zGetCurrentDataWithCheck());
        data.zSerializeFullData(out);
        <% for (int i=0;i<asOfAttributes.length;i++) { %>
        <%= asOfAttributes[i].getSerializationStatement(asOfAttributes[i].getName())%>;
        <% } %>
    }

    public void zSerializeFullTxData(ObjectOutput out) throws IOException
    {
        <%=wrapper.getDataClassName()%> data = ((<%= wrapper.getDataClassName()%>)this.zGetTxDataForRead());
        data.zSerializeFullData(out);
        <% for (int i=0;i<asOfAttributes.length;i++) { %>
        <%= asOfAttributes[i].getSerializationStatement(asOfAttributes[i].getName())%>;
        <% } %>
    }

    <%
       int size = pkAttributes.length;
       for(int i = 0; i < size; i++ )
       {
           if(pkAttributes[i].isSetPrimaryKeyGeneratorStrategy())
           {
               String type = pkAttributes[i].getType().getJavaTypeString();

               String sourceAttributeGetter = null;

               if(wrapper.hasSourceAttribute())
               {
                   if(wrapper.getSourceAttribute().isStringAttribute())
                   {
                       sourceAttributeGetter = wrapper.getSourceAttributeGetterForObject("this")+"()";
                   }
                   else
                   {
                       sourceAttributeGetter = "new Integer("+wrapper.getSourceAttributeGetterForObject("this")+"())";
                   }
               }
    %>
    public <%=type%> generateAndSet<%=StringUtility.firstLetterToUpper(pkAttributes[i].getName())%>()
    {
        <%=type%> nextValue =(<%=type%>) this.generate<%=StringUtility.firstLetterToUpper(pkAttributes[i].getName())%>();
        this.<%=pkAttributes[i].getSetter()%>(nextValue);
        return nextValue;
    }

    public boolean zGetIs<%=StringUtility.firstLetterToUpper(pkAttributes[i].getName())%>Set()
    {
        <%=wrapper.getDataClassName()%> data = this.zSynchronizedGetData();
        return data.zGetIs<%=StringUtility.firstLetterToUpper(pkAttributes[i].getName())%>Set();
    }
    <%      if(pkAttributes[i].isPrimaryKeyUsingSimulatedSequence())
           {
               boolean sequenceHasSourceAttribute = pkAttributes[i].hasSimulatedSequenceSourceAttribute();
    %>
    protected <%=type%> generate<%=StringUtility.firstLetterToUpper(pkAttributes[i].getName())%>()
    {
        Object sourceAttribute = <%=sourceAttributeGetter%>;
        <%= wrapper.getClassName()%>Finder.getMithraObjectPortal().getCache();
        <%if(sequenceHasSourceAttribute){%>
        SimulatedSequencePrimaryKeyGenerator primaryKeyGenerator =
                   MithraPrimaryKeyGenerator.getInstance().getSimulatedSequencePrimaryKeyGenerator("<%=pkAttributes[i].getSimulatedSequence().getSequenceName()%>", "<%=pkAttributes[i].getSimulatedSequence().getSequenceObjectFactoryName()%>",sourceAttribute);
        <%}else{%>
        SimulatedSequencePrimaryKeyGenerator primaryKeyGenerator =
                   MithraPrimaryKeyGenerator.getInstance().getSimulatedSequencePrimaryKeyGeneratorForNoSourceAttribute("<%=pkAttributes[i].getSimulatedSequence().getSequenceName()%>", "<%=pkAttributes[i].getSimulatedSequence().getSequenceObjectFactoryName()%>",sourceAttribute);
        <%}%>
        return (<%=type%>)primaryKeyGenerator.getNextId(sourceAttribute);
    }
    <%     }else{%>
    protected <%=type%> generate<%=StringUtility.firstLetterToUpper(pkAttributes[i].getName())%>()
    throws MithraBusinessException
    {

        MaxFromTablePrimaryKeyGenerator primaryKeyGenerator =
                    MithraPrimaryKeyGenerator.getInstance().getMaxFromTablePrimaryKeyGenerator(<%=wrapper.getFinderClassName()+"."+pkAttributes[i].getName()+"()"%>,<%=sourceAttributeGetter%>);
        return (<%=type%>)primaryKeyGenerator.getNextId();

    }
    <%     }
         }%>
       <%}%>

    <% if(wrapper.hasPkGeneratorStrategy()){%>
    private void checkAndGeneratePrimaryKeys()
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) behavior.getCurrentDataForWrite(this);
        <%for(int i = 0; i < size; i++){
          if(pkAttributes[i].isSetPrimaryKeyGeneratorStrategy()){%>
        if (!data.zGetIs<%=StringUtility.firstLetterToUpper(pkAttributes[i].getName())%>Set())
        {
            <%= pkAttributes[i].getTypeAsString()%> newValue = this.generate<%= StringUtility.firstLetterToUpper(pkAttributes[i].getName())%>();
            data.<%= pkAttributes[i].getSetter()%>(newValue);
        <% if (pkAttributes[i].mustSetRelatedObjectAttribute()) {

            DependentRelationship[] relationshipsToSet = pkAttributes[i].getDependentRelationships();
            for(int r=0;r<relationshipsToSet.length;r++)
            {
                RelationshipAttribute relationshipAttribute = relationshipsToSet[ r ].getRelationshipAttribute();
                if (relationshipAttribute.isRelatedDependent())
                {
        %>
                <%=relationshipAttribute.getMithraImplTypeAsString()%> <%= relationshipAttribute.getName() %> =
                (<%=relationshipAttribute.getMithraImplTypeAsString()%> ) data.<%=relationshipAttribute.getGetter()%>();
            if (<%= relationshipAttribute.getName() %> != null)
            {
                <%= relationshipAttribute.getName() %>.<%= relationshipsToSet[r].getAttributeToSet().getSetter() %>(newValue);
            }
        <%
                }
            }
           } %>

        }
        <%}
        }%>
    }
    <%}%>

    protected void zSerializeAsOfAttributes(java.io.ObjectOutputStream out) throws IOException
    {
        <% for (int i=0;i<asOfAttributes.length;i++) { %>
        <%= asOfAttributes[i].getSerializationStatement(asOfAttributes[i].getName())%>;
        <% } %>
    }

    protected void zDeserializeAsOfAttributes(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        <% for (int i=0;i<asOfAttributes.length;i++) { %>
        this.<%= asOfAttributes[i].getName()%> = <%= asOfAttributes[i].getDeserializationStatement()%>;
        <% } %>
    }

    public boolean zDataMatches(Object data, Timestamp[] asOfDates)
    {
        <%= wrapper.getDataClassName()%> localData = (<%= wrapper.getDataClassName()%>) data;
        MithraDataObject thisData = this.zGetCurrentOrTransactionalData();
        return thisData != null && localData.hasSamePrimaryKeyIgnoringAsOfAttributes(thisData)
        <% AsOfAttribute[] localAsOfAttributes = wrapper.getAsOfAttributes(); // sorted by canonical order, not name%>
        <% for (int i=0;i<localAsOfAttributes.length;i++) { %>
            && this.<%= localAsOfAttributes[i].getName()%>.equals(asOfDates[<%=i%>])
        <% } %>;
    }

<% if (wrapper.hasEmbeddedValueObjects()) { %>
    protected void zResetEmbeddedValueObjects(DatedTransactionalBehavior behavior)
    {
        <%= wrapper.getDataClassName() %> data = (<%= wrapper.getDataClassName() %>) behavior.getCurrentDataForWrite(this);
        <% for (EmbeddedValue evo : embeddedValueObjects) { %>
            data.<%= evo.getNestedSetter() %>(null);
        <% } %>
    }
<% } %>

<% if (wrapper.hasUpdateListener()) { %>
    public void triggerUpdateHook(UpdateInfo updateInfo)
    {
        mithraUpdateListener.handleUpdate(this, updateInfo);
    }

    public void triggerUpdateHookAfterCopy()
    {
        mithraUpdateListener.handleUpdateAfterCopy(this);
    }
<% }%>

    protected static void zConfigNonTx()
    {
        MEMORY_STATE = DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
        PERSISTED_STATE = DatedPersistenceState.PERSISTED_NON_TRANSACTIONAL;
    }

    protected static void zConfigFullTx()
    {
        MEMORY_STATE = DatedPersistenceState.IN_MEMORY;
        PERSISTED_STATE = DatedPersistenceState.PERSISTED;
    }

<%@  include file="../AddHandler.jspi" %>
}
