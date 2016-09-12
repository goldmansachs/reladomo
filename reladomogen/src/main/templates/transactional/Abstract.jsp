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
    String finderClassName = wrapper.getFinderClassName();
    MithraInterfaceType[] mithraInterfaceTypes = wrapper.getMithraInterfaces();
    String semiSpecificFinderTypeName = finderClassName + '.' + wrapper.getClassName() + "RelatedFinder";  // i.e. DomainFinder.DomainRelatedFinder
    TransactionalMethodSignature[] transactionalMethodSignatures = wrapper.getTransactionalMethodSignatures();
    String accessorFilters;
%>

package <%= wrapper.getPackageName() %>;

<%@  include file="../Import.jspi" %>
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.behavior.*;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.list.*;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.attribute.update.*;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;
import java.util.Arrays;
import java.util.HashSet;

<%@  include file="../DoNotModifyWarning.jspi" %>
// Generated from templates/transactional/Abstract.jsp

<% if (wrapper.hasSuperClass()) { %>
public abstract class <%=wrapper.getAbstractClassName()%> extends <%=wrapper.getFullyQualifiedSuperClassType()%> implements MithraTransactionalObject, Serializable
<% if (wrapper.hasMithraInterfaces()) { %>
      <% for (int i=0;i<mithraInterfaceTypes.length;i++) { %>
          , <%=mithraInterfaceTypes[i].getClassName()%>
        <% } %>
    <% } %>
<% } else { %>
public abstract class <%=wrapper.getAbstractClassName()%> extends com.gs.fw.common.mithra.superclassimpl.MithraTransactionalObjectImpl
<% if (wrapper.hasMithraInterfaces()) { %>
      implements
      <% for (int i=0;i<mithraInterfaceTypes.length-1;i++) { %>
           <%=mithraInterfaceTypes[i].getClassName()%>,
        <% } %>
       <%=mithraInterfaceTypes[mithraInterfaceTypes.length-1].getClassName()%>
    <% } %>
<% } %>
{
    private static byte MEMORY_STATE = PersistenceState.IN_MEMORY;
    private static byte PERSISTED_STATE = PersistenceState.PERSISTED;
<% if (wrapper.isTemporary()) { %>
    private MithraObjectPortal portal;
<% } %>
    <% for (int i=0;i<asOfAttributes.length;i++) { %>
    private Timestamp <%= asOfAttributes[i].getName()%>;
    <% } %>
    private static final Logger logger = LoggerFactory.getLogger(<%=wrapper.getImplClassName()%>.class.getName());
<% if (wrapper.hasUpdateListener()) { %>
    protected static final MithraUpdateListener mithraUpdateListener = new <%= wrapper.getUpdateListener()%>();
<% }%>

<%@  include file="../Relationships.jspi" %>
    <% if (wrapper.isPure()) { %>
        private static final List pkExtractors = Arrays.asList(<%= wrapper.getFinderClassName()%>.getPrimaryKeyAttributes());
    <% } %>
    public <%=wrapper.getAbstractClassName()%>()
    {
        this.persistenceState = MEMORY_STATE;
    }

    public <%= wrapper.getImplClassName() %> getDetachedCopy() throws MithraBusinessException
    {
        return (<%= wrapper.getImplClassName() %>) super.getDetachedCopy();
    }

    public <%= wrapper.getImplClassName() %> getNonPersistentCopy() throws MithraBusinessException
    {
        <%= wrapper.getImplClassName() %> result = (<%= wrapper.getImplClassName() %>) super.getNonPersistentCopy();
        result.persistenceState = MEMORY_STATE;
        return result;
    }
<% if (wrapper.isTablePerClassSubClass()) { %>
    public void delete() throws MithraBusinessException
    {
        MithraManagerProvider.getMithraManager().mustBeInTransaction("delete for inherited classed must be done in a transaction.");
        super.delete();
    }

    public void insert() throws MithraBusinessException
    {
        MithraManagerProvider.getMithraManager().mustBeInTransaction("insert for inherited classed must be done in a transaction.");
        super.insert();
    }
<% } %>
    <%if(wrapper.hasOptimisticLockAttribute()){%>
    protected MithraDataObject zCheckOptimisticDirty(MithraDataObject d, TransactionalBehavior behavior)
    {
        <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) d;
        if (data.zIsDirty())
        {
            this.zRefreshWithLockForRead(behavior);
            data = (<%=wrapper.getDataClassName()%>) behavior.getCurrentDataForRead(this);
        }
        return data;
    }
    <% } %>


<% if((!wrapper.isTablePerClassSubClass())&&(wrapper.hasMultipleLifeCycleParents() || wrapper.hasPkGeneratorStrategy() || wrapper.hasTimestampOptimisticLockAttribute() || wrapper.isPure())){%>
    public void insert() throws MithraBusinessException
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
      <% if(wrapper.hasMultipleLifeCycleParents()){%>
        if (behavior.isPersisted()) return;
      <% } %>
      <% if(wrapper.hasPkGeneratorStrategy()){%>
        this.checkAndGeneratePrimaryKeys();
      <%}%>
      <% if(wrapper.hasTimestampOptimisticLockAttribute()){%>
        ((<%= wrapper.getDataClassName()%>)behavior.getCurrentDataForWrite(this)).<%= wrapper.getOptimisticLockAttribute().getSetter()%>(new Timestamp(MithraManagerProvider.getMithraManager().getCurrentProcessingTime()));
      <%}%>
      <% if (wrapper.isPure()) { %>
        MithraDataObject data = behavior.getCurrentDataForWrite(this);
        if (this.zGetCache().getAsOne(data, pkExtractors) != null)
        {
            throw new MithraUniqueIndexViolationException("Cannot insert duplicate pure object "+data.zGetPrintablePrimaryKey());
        }
      <% } %>
        behavior.insert(this);
    }
<% } %>

<% if(wrapper.hasMultipleLifeCycleParents() || wrapper.hasMutablePk()){%>
    public <%= wrapper.getImplClassName() %> copyDetachedValuesToOriginalOrInsertIfNewImpl(MithraTransaction tx)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        <% if(wrapper.hasMultipleLifeCycleParents()){%>
        if (behavior.isPersisted()) return (<%=wrapper.getImplClassName()%>) this;
        <% } %>
        <% if(wrapper.hasMutablePk() && wrapper.hasDependentRelationships()){%>
        behavior.persistChildDelete(this);
        <% } %>
        <%=wrapper.getImplClassName()%> original = (<%= wrapper.getImplClassName() %>) behavior.updateOriginalOrInsert(this);
        <%if(wrapper.hasMutablePk()){%>
        behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        if (!behavior.isDeleted()) ((<%= wrapper.getDataClassName() %>)this.transactionalState.getTxData()).zSetUpdated();
        <%}%>
        return original;
    }
<% } %>

    public <%= wrapper.getImplClassName() %> copyDetachedValuesToOriginalOrInsertIfNew()
    {
        return (<%=wrapper.getImplClassName()%>) this.zCopyDetachedValuesToOriginalOrInsertIfNew();
    }

<%if(wrapper.hasOptimisticLockAttribute() || wrapper.hasShadowAttributes()){%>
    public void zSetInserted()
    {
        MithraDataObject data;
        boolean validTransactionalState = this.transactionalState != null;
        if(validTransactionalState)
        {
            data = this.transactionalState.getTxData();
            if (data == null)
            {
                data = this.currentData.copy();
                this.transactionalState.setTxData(data);
            }
        }
        else
        {
            data = this.currentData;
        }
        ((<%= wrapper.getDataClassName()%>)data).zSetUpdated();
    }

    public void zSetUpdated(List<AttributeUpdateWrapper> updates)
    {
        ((<%= wrapper.getDataClassName()%>)this.transactionalState.getTxData()).zSetUpdated();
    }
<% } %>

<%if(wrapper.hasOptimisticLockAttribute()){%>
    protected void zIncrementOptimiticAttribute(TransactionalBehavior behavior, MithraDataObject d)
    {
        <%= wrapper.getDataClassName()%> data = (<%= wrapper.getDataClassName()%>) d;
        if (data.mustIncrementVersion())
        {
            AttributeUpdateWrapper versionUpdateWrapper =
                new <%= wrapper.getOptimisticLockAttribute().getType().getJavaTypeStringPrimary()%>UpdateWrapper(
                    <%= wrapper.getFinderClassName()%>.<%=wrapper.getOptimisticLockAttribute().getName()%>(),
                    data, <%= wrapper.getOptimisticLockAttribute().getIncrementedVersion()%>);
            behavior.update(this, versionUpdateWrapper, false, true);
        }
    }
<%}%>

<%if(wrapper.hasOptimisticLockAttribute()){%>
    public void zApplyUpdateWrappers(List updateWrappers)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        ((<%= wrapper.getDataClassName()%>)behavior.getCurrentDataForWrite(this)).mustIncrementVersion();
        behavior.remoteUpdate(this, updateWrappers);
    }

    public void zApplyUpdateWrappersForBatch(List updateWrappers)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        behavior.remoteUpdateForBatch(this, updateWrappers);
        ((<%= wrapper.getDataClassName()%>)this.transactionalState.getTxData()).mustIncrementVersion();
    }

    public void zMarkDirty()
    {
        if (this.currentData != null) ((<%=wrapper.getDataClassName()%>)this.currentData).zMarkDirty();
    }
<% } %>

<%if(wrapper.hasNullablePrimaryKeys()){%>
    public boolean zHasSameNullPrimaryKeyAttributes(MithraTransactionalObject other)
    {
        <%=wrapper.getDataClassName()%> otherData = (<%=wrapper.getDataClassName()%>)other.zGetTxDataForRead();
        return ((<%=wrapper.getDataClassName()%>) this.zGetTxDataForRead()).zHasSameNullPrimaryKeyAttributes(otherData);
    }
<% } %>

    public <%= wrapper.getImplClassName() %> zFindOriginal()
    {
        <% if (wrapper.hasMutablePk()) { %>
        <%= wrapper.getDataClassName() %> data = (<%= wrapper.getDataClassName() %>) this.zSynchronizedGetDataForPrimaryKey();
        <% } else { %>
        <%= wrapper.getDataClassName() %> data = (<%= wrapper.getDataClassName() %>) this.currentData;
        <% } %>
        <%= wrapper.getPrimaryKeyOperation()%>
        return <%if(wrapper.isGenerateInterfaces()){%>(<%=wrapper.getImplClassName()%>)<%}%> <%= wrapper.getFinderClassName()%>.findOne(op);
    }

    public boolean isModifiedSinceDetachmentByDependentRelationships()
	{
        if(this.isModifiedSinceDetachment()) return true;
        <%for(int i = 0 ; i < relationshipAttributes.length; i++){
            if(relationshipAttributes[i].isRelatedDependent()){%>
        if(is<%=StringUtility.firstLetterToUpper(relationshipAttributes[i].getName())%>ModifiedSinceDetachment()) return true;
        <%}  }%>
        return false;
	}

    private Logger getLogger()
    {
        return logger;
    }

    public MithraDataObject zAllocateData()
    {
        return new <%= wrapper.getOnHeapDataClassName() %>();
    }

    <%-- called from database object --%>
    protected void zSetFrom<%=wrapper.getDataClassName()%>( <%=wrapper.getDataClassName()%> data )
    {
        super.zSetData(data);
        this.persistenceState = PERSISTED_STATE;
    }

    public void setFrom<%=wrapper.getDataClassName()%>( <%=wrapper.getDataClassName()%> data )
    {
        super.zSetData(data);
    }

    public void zWriteDataClassName(ObjectOutput out) throws IOException
    {
        <% if (wrapper.isTablePerClassSubClass() || wrapper.isTablePerClassSuperClass()) { %>
                out.writeObject("<%= wrapper.getPackageName()%>.<%= wrapper.getDataClassName()%>");
        <% } %>
    }

    <% for (AbstractAttribute attribute : normalAttributes) { %>
        <%= attribute.getVisibility() %> <%=attribute.isFinalGetter() ? "final " : ""%> boolean <%= attribute.getNullGetter() %>
        {
            return ((<%= wrapper.getDataClassName() %>) this.zSynchronizedGetData()).<%= attribute.getNullGetter() %>;
        }

        <%= attribute.getVisibility() %> <%=attribute.isFinalGetter() ? "final " : ""%><%=attribute.getTypeAsString()%> <%=attribute.getGetter()%>()
        {
            <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) this.zSynchronizedGetData();
            <%if(attribute.isNullablePrimitive() && (attribute).getDefaultIfNull() == null){%>
            if (data.<%=attribute.getNullGetter()%>) MithraNullPrimitiveException.throwNew("<%=attribute.getName()%>", data);
            <%}//nullable primitive%>
            return data.<%=attribute.getGetter()%>();
        }
    <%= attribute.getVisibility() %> void <%=attribute.getSetter()%>(<%=attribute.getTypeAsString()%> newValue)
    {
        <%if (attribute.mustTrim()) { %>
        if (newValue != null) newValue = newValue.trim();
        <% } %>

        <% if (attribute.getType() instanceof StringJavaType && attribute.hasMaxLength() ) { %>
          if (newValue != null && newValue.length() > <%=attribute.getMaxLength()%>)
              <% if (attribute.truncate()) { %>
              newValue = newValue.substring(0, <%= attribute.getMaxLength() %> )<%if (attribute.mustTrim()) { %>.trim()<% } %>;
              <% } else { %>
              throw new MithraBusinessException("Attribute '<%=attribute.getName()%>' cannot exceed maximum length of <%=attribute.getMaxLength()%>: " + newValue);
              <% } %>
        <% } %>
        <% if (attribute.isTimeAttribute() && attribute.hasModifyTimePrecisionOnSet()) { %>
            <% if (attribute.getModifyTimePrecisionOnSet().isSybase()) { %>
            if(newValue != null)
                newValue = newValue.createOrReturnTimeWithSybaseMillis();
            <% } else if (attribute.getModifyTimePrecisionOnSet().isTenMillisecond()) { %>
            if(newValue != null)
                newValue = newValue.createOrReturnTimeTenMillisecond();
            <% } %>
        <% } %>
        <% if (!attribute.mustSetRelatedObjectAttribute()) { %>
        zSet<%= attribute.getType().getJavaTypeStringPrimary()%>(<%= wrapper.getFinderClassName()%>.<%=attribute.getName()%>(), newValue, <%= ""+((attribute.isPrimaryKey() && !attribute.isMutablePrimaryKey())||attribute.isReadonly() || attribute == wrapper.getOptimisticLockAttribute())%>, <%= ""+(wrapper.hasOptimisticLockAttribute() && attribute != wrapper.getOptimisticLockAttribute())%> <%if (attribute.isPrimitive()) { %>,<%= attribute.isNullable()%><% } %>);
        <% } else { %>
            <% if (attribute.isMutablePrimaryKey()) { %>
              <% if (wrapper.hasDependentRelationships()) { %>
                <%  DependentRelationship[] relationshipsToSet = attribute.getDependentRelationships();
                    for(int r=0;r<relationshipsToSet.length;r++)
                    {
                        RelationshipAttribute relationshipAttribute = relationshipsToSet[ r ].getRelationshipAttribute();
                        if (relationshipAttribute.isRelatedDependent())
                        {
                %>
                        <%=relationshipAttribute.getMithraTypeAsString()%> <%= relationshipAttribute.getName() %> =
                        (<%=relationshipAttribute.getMithraTypeAsString()%> ) this.<%=relationshipAttribute.getGetter()%>();
                    if (<%= relationshipAttribute.getName() %> != null)
                    {
                        <%= relationshipAttribute.getName() %>.<%= relationshipsToSet[r].getAttributeToSet().getSetter() %>(newValue);
                    }
                <%
                        }
                    } } %>
                zSet<%= attribute.getType().getJavaTypeStringPrimary()%>(<%= wrapper.getFinderClassName()%>.<%=attribute.getName()%>(), newValue, <%= ""+((attribute.isPrimaryKey() && !attribute.isMutablePrimaryKey())||attribute.isReadonly() || attribute == wrapper.getOptimisticLockAttribute())%>, <%= ""+(wrapper.hasOptimisticLockAttribute() && attribute != wrapper.getOptimisticLockAttribute())%> <%if (attribute.isPrimitive()) { %>,<%= attribute.isNullable()%><% } %>);
            <% } else { %>
                MithraDataObject d = zSet<%= attribute.getType().getJavaTypeStringPrimary()%>(<%= wrapper.getFinderClassName()%>.<%=attribute.getName()%>(), newValue, <%= ""+((attribute.isPrimaryKey() && !attribute.isMutablePrimaryKey())||attribute.isReadonly() || attribute == wrapper.getOptimisticLockAttribute())%>, <%= ""+(wrapper.hasOptimisticLockAttribute() && attribute != wrapper.getOptimisticLockAttribute())%> <%if (attribute.isPrimitive()) { %>,<%= attribute.isNullable()%><% } %>);
                if (d == null) return;
                <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) d;
        		TransactionalBehavior _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
                if (!_behavior.isPersisted())
                {
                <%  DependentRelationship[] relationshipsToSet = attribute.getDependentRelationships();
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
                    } %>
                }
                 <% } %>
            <% } %>
    }
    <%}%>

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
            TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
            try
            {
                <%= wrapper.getDataClassName() %> data = null;
                boolean dataChanged = false;
                <% if (wrapper.hasOptimisticLockAttribute()) { %>
                    boolean optimisticLockingDataChanged = false;
                <% } %>
                AttributeUpdateWrapper updateWrapper;
                <% for (EmbeddedValueMapping attribute : evo.getMappingsRecursively()) { %>
                    data = (<%= wrapper.getDataClassName() %>) behavior.getCurrentDataForRead(this);
                    <% if (attribute.isPrimitive()) { %>
                        <% if (attribute.isNullablePrimitive()) { %>
                            if (data.<%= attribute.getNullGetter() %> != is<%= StringUtility.firstLetterToUpper(attribute.getName()) %>Null)
                            {
                                dataChanged = true;
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
                    if (dataChanged)
                    {
                    <% if (attribute.isPrimaryKey() && !attribute.isMutablePrimaryKey()) { %>
                        if (!behavior.maySetPrimaryKey()) throw new MithraBusinessException("cannot change the primary key");
                    <% } %>
                        <% if (wrapper.hasOptimisticLockAttribute() && attribute == wrapper.getOptimisticLockAttribute()) { %>
                            optimisticLockingDataChanged = true;
                        <% } %>
                        data = (<%= wrapper.getDataClassName() %>) behavior.getCurrentDataForWrite(this);
                        updateWrapper = new <%= attribute.getType().getJavaTypeStringPrimary() %>UpdateWrapper(
                            <%= wrapper.getFinderClassName() %>.<%= attribute.getName() %>(),
                            data,
                            <%= attribute.getName() %>);
                        behavior.update(this, updateWrapper, <%= (attribute.isReadonly() || attribute == wrapper.getOptimisticLockAttribute()) %>, true);
                    }
                <% } %>
                <% if (wrapper.hasOptimisticLockAttribute()) { %>
                    if (!optimisticLockingDataChanged)
                    {
                        data = (<%= wrapper.getDataClassName() %>) behavior.getCurrentDataForWrite(this);
                        if (data.mustIncrementVersion())
                        {
                            AttributeUpdateWrapper versionUpdateWrapper =
                                new <%= wrapper.getOptimisticLockAttribute().getType().getJavaTypeStringPrimary() %>UpdateWrapper(
                                    <%= wrapper.getFinderClassName() %>.<%= wrapper.getOptimisticLockAttribute().getName() %>(),
                                    data, <%= wrapper.getOptimisticLockAttribute().getIncrementedVersion() %>);
                            behavior.update(this, versionUpdateWrapper, false, true);
                        }
                    }
                <% } %>
                this.zResetEmbeddedValueObjects(behavior);
            }
            finally
            {
                behavior.clearTempTransaction(this);
            }
        }
    <% } %>

    protected void issuePrimitiveNullSetters(TransactionalBehavior behavior, MithraDataObject data)
    {
    <% if (wrapper.isTablePerClassSubClass()) { %>
        super.issuePrimitiveNullSetters(behavior, data);
    <% } %>
    <%for (int j = 0;j < nullablePrimitiveAttributes.length; j ++) {%>
        zNullify(behavior, data, <%= wrapper.getFinderClassName()%>.<%=nullablePrimitiveAttributes[j].getName()%>(), <%= (nullablePrimitiveAttributes[j].isReadonly())%>);
    <%}%>
    }
    <% for (int i = 0; i < nullablePrimitiveAttributes.length; i ++) { %>
    <%= nullablePrimitiveAttributes[i].getVisibility() %> void <%= nullablePrimitiveAttributes[i].getSetter() %>Null()
    {
        zNullify(<%= wrapper.getFinderClassName()%>.<%=nullablePrimitiveAttributes[i].getName()%>(), <%= (nullablePrimitiveAttributes[i].isPrimaryKey() && !nullablePrimitiveAttributes[i].isMutablePrimaryKey()) || (nullablePrimitiveAttributes[i].isReadonly())%>);
    }
    <%}%>


    <% for (int i=0;i<asOfAttributes.length;i++) { %>
    public <%=asOfAttributes[i].getTypeAsString()%> <%=asOfAttributes[ i ].getGetter()%>()
    {
        return this.<%= asOfAttributes[i].getName()%>;
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
                    <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%=relationshipAttributes[i].getMithraImplTypeAsString()%>)<%}%> this.<%=relationshipAttributes[i].getGetter()%>();
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
                      <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%=relationshipAttributes[i].getMithraImplTypeAsString()%>)<%}%> this.<%=relationshipAttributes[i].getGetter()%>();
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
        TransactionalBehavior _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        if (_behavior.isDetached() && _behavior.isDeleted()) return;
        <%=wrapper.getDataClassName()%> _newData = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForRead(this);
    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
        <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
            if (_newData.<%=relationshipAttributes[i].getGetter()%>() != null && !(_newData.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation))
            {
                ((<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>)_newData.<%=relationshipAttributes[i].getGetter()%>()).zSetTxDetachedDeleted();
            }
        <% } %>
    <% } %>
        this.zSetTxPersistenceState(PersistenceState.DETACHED_DELETED);
    }

    public void zSetNonTxDetachedDeleted()
    {
        <% if (wrapper.isTablePerClassSubClass()) { %>
            super.zSetNonTxDetachedDeleted();
        <% } %>
        TransactionalBehavior _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        <%=wrapper.getDataClassName()%> _newData = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForRead(this);
    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
        <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
            if (_newData.<%=relationshipAttributes[i].getGetter()%>() != null && !(_newData.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation))
            {
                ((<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>)_newData.<%=relationshipAttributes[i].getGetter()%>()).zSetNonTxDetachedDeleted();
            }
        <% } %>
    <% } %>
        this.zSetNonTxPersistenceState(PersistenceState.DETACHED_DELETED);
    }


<% if (wrapper.hasMutablePk() && wrapper.hasDependentRelationships()) { %>
    public void zPersistDetachedChildDelete(MithraDataObject _data)
    {
    <% if (wrapper.isTablePerClassSubClass()) { %>
        super.zPersistDetachedChildDelete(_data);
    <% } %>
        <%=wrapper.getDataClassName()%> _newData = (<%=wrapper.getDataClassName()%>) _data;
    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
        <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
            <% if (!relationshipAttributes[i].isToMany()) { %>
            if (_newData.<%=relationshipAttributes[i].getGetter()%>() instanceof NulledRelation)
            {
                <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                    <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%=relationshipAttributes[i].getMithraImplTypeAsString()%>)<%}%> this.<%=relationshipAttributes[i].getGetter()%>();
                if (<%= relationshipAttributes[i].getName()%> != null)
                {
                    <%= relationshipAttributes[i].getName()%>.<%= relationshipAttributes[i].getRelatedObject().getCascadeDeleteOrTerminate()%>();
                }
            }
            <% } else { %>
            <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName()%> =
                (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _newData.<%=relationshipAttributes[i].getGetter()%>();
            if (<%= relationshipAttributes[i].getName()%> != null)
            {
                <%= relationshipAttributes[i].getName()%>.zCopyDetachedValuesDeleteIfRemovedOnly();
            }
            <% } %>
        <% } %>
    <% } %>
    }
<% } %>



    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
<%@ include file="../RelationshipJavaDoc.jspi" %>
	public <%=relationshipAttributes[i].isFinalGetter() ? "final " : ""%><%=relationshipAttributes[ i ].getTypeAsString()%> <%=relationshipAttributes[ i ].getGetter()%>(<%=relationshipAttributes[ i ].getParameters() %>)
    {
        <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> _result = null;
        Operation _op = null;
        TransactionalBehavior _behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        <%=wrapper.getDataClassName()%> _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForRead(this);
        <% if (relationshipAttributes[i].needsPortal()) { %>
            MithraObjectPortal _portal = null;
        <% } %>
        <% if (relationshipAttributes[i].hasSetter()) { %>
        if (_behavior.isPersisted())
        <% } // has setter%>
        {
            <% accessorFilters = relationshipAttributes[ i ].getFilterExpression(); %>
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
                        Object o = _data.<%=relationshipAttributes[i].getGetter()%>();
                        if (o instanceof NullPersistedRelation)
                        {
                            if (((NullPersistedRelation)o).isValid()) return null;
                        }
                        else
                        {
                            _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _portal.unwrapRelatedObject(_data, o, <%= relationshipAttributes[i].getDirectRefFromExtractorName()%>, <%= relationshipAttributes[i].getDirectRefToExtractorName()%>);
                            if (_result != null) return _result;
                        }
                    }
                    else
                    {
                        _data.clearAllDirectRefs();
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
                        Object o = _data.<%=relationshipAttributes[i].getGetter()%>();
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
                        this.classUpdateCount = currentCount;
                    }
                }
            <% } // isToManyDirectReference%>
                <% if (relationshipAttributes[i].isFindByUnique()) { %>
                       _result = (<%=relationshipAttributes[i].getMithraImplTypeAsString()%>) <%= relationshipAttributes[i].getRelatedObject().getFinderClassName()%>.<%= relationshipAttributes[i].getFindByUniqueMethodName() %>(<%= relationshipAttributes[i].getFindByUniqueParameters() %>);
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
                _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
                _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);
                _data.<%= relationshipAttributes[i].getSetter()%>(_result);
                <% if (relationshipAttributes[i].isBidirectional()) { %>
                if (_result != null) _result.zSetParentContainer<%=relationshipAttributes[i].getReverseName()%>(this);
                <% } %>
            <% } else if (relationshipAttributes[i].mustDetach()) {%>
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
                    _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
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
            _data.<%= relationshipAttributes[i].getSetter()%>(_portal.wrapRelatedObject(_result));
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
        TransactionalBehavior _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
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
                _<%= relationshipAttributes[i].getName()%>.get<%= StringUtility.firstLetterToUpper(relationshipAttributes[i].getReverseName())%>().add( (<%=wrapper.getImplClassName()%>) this);
            <% } else { %>
            <% } %>
        <% } else if (relationshipAttributes[i].isRelatedDependent()) { %>
            if (_<%= relationshipAttributes[i].getName()%> != null)
            {
            <% Attribute[] attributesToSet = relationshipAttributes[i].getAttributesToSetOnRelatedObject(); %>
            <% for (int r = 0; r < attributesToSet.length ; r++)  if (!attributesToSet[r].isAsOfAttribute()) { %>
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
                this.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getSetter() %>(<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getPrimitiveCastType(attributesToSet[r])%>_<%= relationshipAttributes[i].getName() %>.<%= attributesToSet[r].getGetter()%>());
            <% } %>
            }
        <% } %>
        }
        else if (_behavior.isPersisted())
        {
            _behavior.clearTempTransaction(this);
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
                        <% for (int r = 0; r < attributesToSet.length ; r++)  if (!attributesToSet[r].isAsOfAttribute()) { %>
                        item.<%= attributesToSet[r].getSetter()%>(_data.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getGetter() %>());
                        <% } %>
                        item.cascadeInsert();
                    }
                }
                <%= relationshipAttributes[i].getName()%>ToDelete.<%= relationshipAttributes[i].getRelatedObject().getCascadeDeleteOrTerminate()%>All();
            <% } else { %>
                <%=relationshipAttributes[ i ].getImplTypeAsString()%> _existing = <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%=relationshipAttributes[i].getMithraImplTypeAsString()%>)<%}%> this.<%= relationshipAttributes[i].getGetter()%>();
                if (_<%= relationshipAttributes[i].getName()%> != _existing)
                {
                    if (_existing != null)
                    {
                        _existing.<%= relationshipAttributes[i].getRelatedObject().getCascadeDeleteOrTerminate()%>();
                    }
                    if (_<%= relationshipAttributes[i].getName()%> != null)
                    {
                        <% for (int r = 0; r < attributesToSet.length ; r++)  if (!attributesToSet[r].isAsOfAttribute()) { %>
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
    <% } // has setter %>
    <% if (relationshipAttributes[i].hasParentContainer()) { %>
    public void zSetParentContainer<%=relationshipAttributes[i].getName()%>(<%= relationshipAttributes[i].getRelatedObject().getAbstractClassName()%> parent)
    {
        TransactionalBehavior _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        <%=wrapper.getDataClassName()%> _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);
        _behavior.clearTempTransaction(this);
        if (_behavior.isInMemory())
        {
            _data.<%=relationshipAttributes[ i ].getSetter()%>(parent);
        }
    }
    <% } %>

    <% if (wrapper.isTransactional() && relationshipAttributes[i].mustPersistRelationshipChanges()){ %>
    public boolean is<%=StringUtility.firstLetterToUpper(relationshipAttributes[i].getName())%>ModifiedSinceDetachment()
    {
        TransactionalBehavior _behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
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

    <%}//accessors for relationship end%>

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

    protected void cascadeInsertImpl() throws MithraBusinessException
    {
        TransactionalBehavior _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
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
               (<%= relationshipAttributes[i].getImplTypeAsString()%>) _data.<%=relationshipAttributes[i].getGetter()%>();
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
        TransactionalBehavior _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        _behavior.addNavigatedRelationshipsStats(this, finder, navigationStats);
        return navigationStats;
    }

    @Override
    public Map< RelatedFinder, StatisticCounter > zAddNavigatedRelationshipsStatsForUpdate(RelatedFinder parentFinderGeneric, Map< RelatedFinder, StatisticCounter > navigationStats)
    {
        <% if (wrapper.hasDependentRelationships()) { %>
          <%=semiSpecificFinderTypeName%> parentFinder = (<%=semiSpecificFinderTypeName%>) parentFinderGeneric;
          TransactionalBehavior _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
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

    @Override
    public <%= wrapper.getImplClassName() %> zCascadeCopyThenInsert() throws MithraBusinessException
    {
        TransactionalBehavior _behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        <% if(wrapper.hasMultipleLifeCycleParents()){%>
        if (_behavior.isPersisted()) return (<%= wrapper.getImplClassName() %>) this;
        <% } %>
      <% if(wrapper.hasPkGeneratorStrategy()){%>
        this.checkAndGeneratePrimaryKeys();
      <%}%>
      <% if (wrapper.hasDependentRelationships()) { %>
        <%=wrapper.getDataClassName()%> _data = (<%=wrapper.getDataClassName()%>) _behavior.getCurrentDataForWrite(this);
        <% for (int i=0;i<relationshipAttributes.length;i++)
            if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()) { %>
        <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> <%= relationshipAttributes[i].getName() %> =
               (<%= relationshipAttributes[i].getImplTypeAsString()%>) _data.<%=relationshipAttributes[i].getGetter()%>();
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

    protected void cascadeDeleteImpl() throws MithraBusinessException
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
                    <%= relationshipAttributes[i].getImplTypeAsString()%> related = <%if(relationshipAttributes[i].getRelatedObject().isGenerateInterfaces()){%>(<%=relationshipAttributes[i].getMithraImplTypeAsString()%>) <%}%>this.<%=relationshipAttributes[i].getGetter()%>();
                    if (related != null)
                        related.<%= relationshipAttributes[i].getRelatedObject().getCascadeDeleteOrTerminate()%>();
                }
            <% } %>
        <% } %>
      <% } %>
    <% if (wrapper.isTablePerClassSubClass()) { %>
        super.cascadeDeleteImpl();
    <% } else { %>
        this.delete();
    <% } %>
    }

    public Cache zGetCache()
    {
        <% if (wrapper.isTemporary()) { %>
        if (this.portal == null)
        {
            this.portal = <%= wrapper.getClassName()%>Finder.getMithraObjectPortal();
        }
        return this.portal.getCache();
        <% } else { %>
        return <%= wrapper.getClassName()%>Finder.getMithraObjectPortal().getCache();
        <% } %>
    }

    public MithraObjectPortal zGetPortal()
    {
        <% if (wrapper.isTemporary()) { %>
        if (this.portal == null)
        {
            this.portal = <%= wrapper.getClassName()%>Finder.getMithraObjectPortal();
        }
        return this.portal;
        <% } else { %>
        return <%= wrapper.getClassName()%>Finder.getMithraObjectPortal();
        <% } %>
    }

    public <%= wrapper.getImplClassName() %> getOriginalPersistentObject()
    {
        return this.zFindOriginal();
    }

    protected boolean issueUpdatesForNonPrimaryKeys(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData)
    {
        boolean changed = false;
    <% if (wrapper.isTablePerClassSubClass()) { %>
        changed |= super.issueUpdatesForNonPrimaryKeys(behavior, data, newData);
    <% } %>
            <% for(int i=0;i < normalAttributes.length; i++) { %>

                <% if (!normalAttributes[i].isPrimaryKey()
                                    && !normalAttributes[i].isSourceAttribute()
                                    && !normalAttributes[i].isAsOfAttributeFrom()
                                    && !normalAttributes[i].isAsOfAttributeTo() && normalAttributes[i] != wrapper.getOptimisticLockAttribute()
                            ) {%>
                changed |= zUpdate<%= normalAttributes[i].getType().getJavaTypeStringPrimary()%>(behavior, data, newData, <%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(), <%= (normalAttributes[i].isReadonly())%>);
                    <% } %>
            <% } %>
            <% if (wrapper.hasOptimisticLockAttribute()) { %>
                if (changed) zIncrementOptimiticAttribute(behavior, data);
            <% } %>
        return changed;
    }

    protected boolean issueUpdatesForPrimaryKeys(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData)
    {
        boolean changed = false;
    <% if (wrapper.isTablePerClassSubClass()) { %>
        changed |= super.issueUpdatesForPrimaryKeys(behavior, data, newData);
    <% } %>
            <% for(int i=0;i < normalAttributes.length; i++) { %>

                <% if (normalAttributes[i].isPrimaryKey()
                                    || normalAttributes[i].isSourceAttribute()
                                    || normalAttributes[i].isAsOfAttributeFrom()
                                    || normalAttributes[i].isAsOfAttributeTo()
                            ) {%>
                changed |= zUpdate<%= normalAttributes[i].getType().getJavaTypeStringPrimary()%>(behavior, data, newData, <%= wrapper.getFinderClassName()%>.<%=normalAttributes[i].getName()%>(), <%= (normalAttributes[i].isReadonly())%>);
                    <% } %>
                <% } %>
        return changed;
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
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) behavior.getCurrentDataForRead(this);
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

    <%if(wrapper.hasPkGeneratorStrategy()){%>
    private void checkAndGeneratePrimaryKeys()
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        <%=wrapper.getDataClassName()%> data = (<%=wrapper.getDataClassName()%>) behavior.getCurrentDataForWrite(this);
        <%for(int i = 0; i < size; i++){
          if(pkAttributes[i].isSetPrimaryKeyGeneratorStrategy()){%>
        if (!data.zGetIs<%=StringUtility.firstLetterToUpper(pkAttributes[i].getName())%>Set())
        {
            data.<%= pkAttributes[i].getSetter()%>(generate<%= StringUtility.firstLetterToUpper(pkAttributes[i].getName())%>());
            <% if (pkAttributes[i].mustSetRelatedObjectAttribute()) {

                DependentRelationship[] relationshipsToSet = pkAttributes[i].getDependentRelationships();
                for(int r=0;r<relationshipsToSet.length;r++)
                {
                    RelationshipAttribute relationshipAttribute = relationshipsToSet[ r ].getRelationshipAttribute();
            %>
                    <%=relationshipAttribute.getMithraImplTypeAsString()%> <%= relationshipAttribute.getName() %> =
                    (<%=relationshipAttribute.getMithraImplTypeAsString()%> ) data.<%=relationshipAttribute.getGetter()%>();
                    if (<%= relationshipAttribute.getName() %> != null)
                    {
                        <%= relationshipAttribute.getName() %>.<%= relationshipsToSet[r].getAttributeToSet().getSetter() %>(data.<%= pkAttributes[i].getGetter()%>());
                    }
            <%  }
               } %>
        }
        <%}
        }%>
    }
    <%}%>

<% if (wrapper.hasEmbeddedValueObjects()) { %>
    protected void zResetEmbeddedValueObjects(TransactionalBehavior behavior)
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
    public Object readResolve() throws ObjectStreamException
    {
        <%=wrapper.getAbstractClassName()%> result = (<%=wrapper.getAbstractClassName()%>) super.readResolve();
        if (result.persistenceState == PersistenceState.PERSISTED)
        {
            result.persistenceState = PERSISTED_STATE;
        }
        else if (result.persistenceState == PersistenceState.IN_MEMORY)
        {
            result.persistenceState = MEMORY_STATE;
        }
        return result;
    }

    protected static void zConfigNonTx()
    {
        MEMORY_STATE = PersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
        PERSISTED_STATE = PersistenceState.PERSISTED_NON_TRANSACTIONAL;
    }

    protected static void zConfigFullTx()
    {
        MEMORY_STATE = PersistenceState.IN_MEMORY;
        PERSISTED_STATE = PersistenceState.PERSISTED;
    }

<%@  include file="../AddHandler.jspi" %>
}
