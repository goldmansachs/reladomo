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
<%@ page import="com.gs.fw.common.mithra.generator.util.StringUtility" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraObjectType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AttributeType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.RelationshipType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
	AbstractAttribute[] normalAttributes = wrapper.getSortedNormalAndSourceAttributes();
    EmbeddedValue[] embeddedValueObjects = wrapper.getEmbeddedValueObjects();
    RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
    AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
    String finderClassName = wrapper.getFinderClassName();
    MithraInterfaceType[] mithraInterfaceTypes = wrapper.getMithraInterfaces();
    String accessorFilters = "";
%>

package <%= wrapper.getPackageName() %>;

<%@  include file="../Import.jspi" %>
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.list.*;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;
import java.util.HashSet;
import java.util.Arrays;

<%@  include file="../DoNotModifyWarning.jspi" %>
// Generated from templates/datedreadonly/Abstract.jsp

<% if (!wrapper.hasSuperClass()) { %>
public abstract class <%=wrapper.getAbstractClassName()%>
<% } else { %>
public abstract class <%=wrapper.getAbstractClassName()%> extends <%=wrapper.getFullyQualifiedSuperClassType()%>
<% } %>
implements MithraDatedObject, Serializable
<% if (wrapper.hasMithraInterfaces()) { %>
    <% for (int i=0;i<mithraInterfaceTypes.length;i++) { %>
        , <%=mithraInterfaceTypes[i].getClassName()%>
    <% } %>
<% } %>
{
    <% if (!wrapper.isTablePerClassSubClass()) { %>
        protected <%=wrapper.getDataClassName()%> currentData;
        protected byte dataVersion;
        protected byte persistenceState = DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
    <% if (wrapper.hasDirectRefs()) { %>
        private int _classUpdateCount = 1;
    <% } %>
        <% for (int i=0;i<asOfAttributes.length;i++) { %>
            protected Timestamp <%= asOfAttributes[i].getName()%>;
        <% } %>
    <% } %>
<%@  include file="../Relationships.jspi" %>
<% for (RelationshipAttribute rel : relationshipAttributes) { %>
    <% if (rel.isDirectReference()) { %>
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
            super(<%= asOfAttributes[0].getName()%>
            <% for (int i=1;i<asOfAttributes.length;i++) { %>
                ,<%= asOfAttributes[i].getName()%>
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
    }

    <% if (wrapper.hasBusinessDateAsOfAttribute() && wrapper.hasProcessingDate()) { %>
    public <%=wrapper.getAbstractClassName()%>(Timestamp <%= wrapper.getBusinessDateAsOfAttribute().getName()%>)
    {
        this(<%= wrapper.getBusinessDateAsOfAttribute().getName() %>, <%= wrapper.getProcessingDateAttribute().getInfinityExpression() %>);
    }
    <% } %>

    protected Object zGetLock()
    {
        return this;
    }

    public void zSetNonTxPersistenceState(int state)
    {
        this.persistenceState = (byte) state;
    }

    public boolean isDeletedOrMarkForDeletion()
    {
        return this.persistenceState == DatedPersistenceState.DELETED || this.persistenceState == DatedPersistenceState.PERSISTED_NON_TRANSACTIONAL && zIsDataDeleted();
    }

    public void setFrom<%=wrapper.getDataClassName()%>( <%=wrapper.getDataClassName()%> data )
    {
        synchronized(this.zGetLock())
        {
            <% if (!wrapper.isTablePerClassSubClass()) { %>
                this.currentData = data;
                this.dataVersion = data.zGetDataVersion();
                this.persistenceState = DatedPersistenceState.PERSISTED_NON_TRANSACTIONAL;
            <% } else { %>
                super.currentData = data;
                super.dataVersion = data.zGetDataVersion();
                super.persistenceState = DatedPersistenceState.PERSISTED_NON_TRANSACTIONAL;
            <% } %>
        }
    }

    protected void zSetFrom<%=wrapper.getDataClassName()%>( <%=wrapper.getDataClassName()%> data )
    {
        this.setFrom<%=wrapper.getDataClassName()%>(data);
    }

    public void zSetCurrentData(MithraDataObject data)
    {
        this.setFrom<%=wrapper.getDataClassName()%>( (<%=wrapper.getDataClassName()%>) data );
    }

    public <%= wrapper.getImplClassName() %> getNonPersistentCopy() throws MithraBusinessException
    {
        MithraDataObject newData = this.currentData.copy();
        Timestamp[] asOfAttributes = new Timestamp[<%= asOfAttributes.length %>];
        <% for(int i=0;i<asOfAttributes.length;i++) { %>
        asOfAttributes[<%= i %>] = this.<%= asOfAttributes[i].getName()%>;
        <% } %>
        <%= wrapper.getAbstractClassName() %> copy = (<%= wrapper.getAbstractClassName() %>) 
        <%= wrapper.getClassName()%>Finder.getMithraObjectPortal().getMithraDatedObjectFactory().createObject(newData, asOfAttributes);
        copy.persistenceState = DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
        return (<%= wrapper.getImplClassName() %>)copy;
    }


    <% if (!wrapper.isTablePerClassSubClass()) { %>
        public MithraDataObject zGetCurrentData()
        {
            return this.currentData;
        }

        public MithraDataObject zGetCurrentOrTransactionalData()
        {
            MithraDataObject result = this.currentData;
            if (result == null)
            {
                synchronized(this.zGetLock())
                {
                    result = this.currentData;
                }
            }
            return result;
        }
    <% } %>

    public void zReindexAndSetDataIfChanged(MithraDataObject data, Cache cache)
    {
        throw new RuntimeException("should never be called");
    }

    public void zSetData(MithraDataObject data, Object optional)
    {
        throw new RuntimeException("should never be called");
    }

    public MithraDataObject zRefreshWithLock(boolean lock)
    {
        <%= wrapper.getDataClassName()%> data = (<%= wrapper.getDataClassName()%>) <%= wrapper.getClassName()%>Finder.getMithraObjectPortal().refreshDatedObject(this, lock);
        if (data == null)
        {
            throw new MithraDeletedException("<%= wrapper.getClassName()%> has been deleted: " + this.zGetCurrentData().zGetPrintablePrimaryKey());
        }
        return data;
    }

    public Cache zGetCache()
    {
        return <%= wrapper.getClassName()%>Finder.getMithraObjectPortal().getCache();
    }

	public <%=wrapper.getDataClassName()%> zGetCurrentDataWithCheck()
	{
		<%=wrapper.getDataClassName()%> current = currentData;
		if (current == null && persistenceState == DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL)
		{
            return zGetOrInitializeInMemoryData();
        }
        if (persistenceState != DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL)
        {
            boolean refresh = current.zGetDataVersion() < 0;
            if (!refresh && dataVersion != current.zGetDataVersion())
            {
                refresh = zCheckForRefresh(current);
            }

            if (refresh)
            {
                current = zRefreshData(current);
            }
        }
        return current;
	}

    private <%=wrapper.getDataClassName()%> zRefreshData(<%=wrapper.getDataClassName()%> current)
    {
        <%=wrapper.getDataClassName()%> newData = (<%=wrapper.getDataClassName()%>) this.zGetCache().refreshOutsideTransaction(this, current);
        if (newData == null)
        {
            throw new MithraDeletedException("<%= wrapper.getClassName() %> has been deleted: " + current.zGetPrintablePrimaryKey());
        }

        synchronized(this.zGetLock())
        {
            if (currentData == current)
            {
                currentData = newData;
                <% if (wrapper.hasDirectRefs()) { %>
                zClearAllDirectRefs();
                <% } %>
            }
        }

        return newData;
    }

    private boolean zCheckForRefresh(<%=wrapper.getDataClassName()%> current)
    {
        boolean refresh = !<%= wrapper.getClassName() %>Finder.<%= asOfAttributes[0].getName()%>().dataMatches(current, <%= asOfAttributes[0].getName()%>);
        <% for(int i=1;i<asOfAttributes.length;i++) { %>
        if (!refresh)
        {
            refresh = !<%= wrapper.getClassName() %>Finder.<%= asOfAttributes[i].getName()%>().dataMatches(current, <%= asOfAttributes[i].getName()%>);
        }
        <% } %>
        return refresh;
    }

    private <%=wrapper.getDataClassName()%> zGetOrInitializeInMemoryData()
    {
        <%=wrapper.getDataClassName()%> current;
        synchronized(this.zGetLock())
        {
            if (currentData == null)
            {
                currentData = new <%=wrapper.getOnHeapDataClassName()%>();
            }

            current = this.currentData;
        }
        return current;
    }

    private boolean zIsDataDeleted()
	{
		<%=wrapper.getDataClassName()%> current = currentData;
		boolean refresh = current.zGetDataVersion() < 0;
		if (!refresh && dataVersion != current.zGetDataVersion())
		{
            refresh = zCheckForRefresh(current);
		}

		if (refresh)
		{
			<%=wrapper.getDataClassName()%> newData = (<%=wrapper.getDataClassName()%>) this.zGetCache().refreshOutsideTransaction(this, current);
			if (newData == null) return true;
		}

		return false;
	}

    <% for (int i = 0; i < normalAttributes.length; i ++) {%>
    <%= normalAttributes[i].getVisibility() %> <%=normalAttributes[i].isFinalGetter() ? "final " : ""%> boolean <%=normalAttributes[i].getNullGetter()%>
    {
        <% if (normalAttributes[i].isPrimaryKey()) { %>
        return this.currentData.<%=normalAttributes[i].getNullGetter()%>;
        <% } else { %>
        return this.zGetCurrentDataWithCheck().<%=normalAttributes[i].getNullGetter()%>;
        <% } %>
    }

    <%= normalAttributes[i].getVisibility() %> <%=normalAttributes[i].isFinalGetter() ? "final " : ""%><%=normalAttributes[i].getTypeAsString()%> <%=normalAttributes[ i ].getGetter()%>()
    {
        <% if (normalAttributes[i].isPrimaryKey()) { %>
            <%=wrapper.getDataClassName()%> data = this.currentData;
        <% } else { %>
            <%=wrapper.getDataClassName()%> data = this.zGetCurrentDataWithCheck();
        <% } %>
        <%if (normalAttributes[i].isNullablePrimitive() && normalAttributes[i].getDefaultIfNull() == null) {%>
            if (data.<%=normalAttributes[i].getNullGetter()%>)
                MithraNullPrimitiveException.throwNew("<%=normalAttributes[i].getName()%>", data);
        <%}//nullable primitive%>
            return data.<%=normalAttributes[ i ].getGetter()%>();
    }

    <%= normalAttributes[i].getVisibility() %> void <%=normalAttributes[ i ].getSetter()%>(<%=normalAttributes[i].getTypeAsString()%> newValue)
    {
        this.ensureSetAttributeAllowed();
        this.zGetCurrentDataWithCheck().<%=normalAttributes[ i ].getSetter()%>(newValue);
        <% if (wrapper.hasEmbeddedValueObjects()) { %>this.resetEmbeddedValueObjects(); <% } %>
    }

    <%}%>

    <% for (EmbeddedValue evo : embeddedValueObjects) { %>
        <%= evo.getVisibility() %> <%=evo.isFinalGetter() ? "final " : ""%><%= evo.getType() %> <%= evo.getNestedGetter() %>()
        {
            <%= wrapper.getDataClassName() %> data = (<%= wrapper.getDataClassName() %>) this.zGetCurrentDataWithCheck();
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
            this.ensureSetAttributeAllowed();
            <% for (EmbeddedValueMapping attribute : evo.getMappings()) { %>
                    this.<%= attribute.getSetter() %>(<%= evo.getName() %>.<%= attribute.getShortNameGetter() %>());
            <% } %>
            <% for (EmbeddedValue nestedObject : evo.getDescendants()) { %>
                <% for (EmbeddedValueMapping attribute : nestedObject.getMappings()) { %>
                    this.<%= attribute.getSetter() %>(<%= evo.getName() %>.<%= attribute.getChainedGetterAfterDepth(evo.getHierarchyDepth()) %>());
                <% } %>
            <% } %>
        }

        <%= evo.getVisibility() %> void <%= evo.getNestedCopyValuesFromUntil() %>(<%= evo.getType() %> <%= evo.getName() %>, Timestamp exclusiveUntil)
        {
            throw new UnsupportedOperationException("<%= evo.getNestedCopyValuesFromUntil() %> is only implemented for dated transactional objects.");
        }
    <% } %>

    <% if (!wrapper.isTablePerClassSubClass()) { %>
        protected void ensureSetAttributeAllowed()
        {
            if (this.persistenceState != DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL)
            {
                throw new MithraBusinessException("Cannot set an attribute of a persisted read-only object");
            }
        }

        <% for (int i=0;i<asOfAttributes.length;i++) { %>
            public <%=asOfAttributes[i].isFinalGetter() ? "final " : ""%><%=asOfAttributes[i].getTypeAsString()%> <%=asOfAttributes[ i ].getGetter()%>()
            {
                return this.<%= asOfAttributes[i].getName()%>;
            }
        <% } %>
    <% } %>

    <%  Attribute[] nullablePrimitiveAttributes = wrapper.getNullablePrimitiveAttributes();
        for (int i = 0; i < nullablePrimitiveAttributes.length; i ++) {%>
    <%= nullablePrimitiveAttributes[i].getVisibility() %> void <%= nullablePrimitiveAttributes[i].getSetter() %>Null()
    {
        this.ensureSetAttributeAllowed();
        this.zGetCurrentDataWithCheck().<%=nullablePrimitiveAttributes[ i ].getSetter()%>Null();
        <% if (wrapper.hasEmbeddedValueObjects()) { %>this.resetEmbeddedValueObjects(); <% } %>
    }
    <% } %>

<% if (wrapper.hasDirectRefs()) { %>
    protected void zClearAllDirectRefs()
    {
    <% for (RelationshipAttribute rel : relationshipAttributes) { %>
        <% if (rel.isDirectReferenceInBusinessObject()) { %>
        <%= rel.getDirectRefVariableName() %> = null;
        <% } %>
    <% } %>
    }
<% } %>

    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship start %>
<%@ include file="../RelationshipJavaDoc.jspi" %>
	public <%=relationshipAttributes[i].isFinalGetter() ? "final " : ""%><%=relationshipAttributes[ i ].getTypeAsString()%> <%=relationshipAttributes[ i ].getGetter()%>(<%= relationshipAttributes[i].getParameters()%>)
    {
        <%=wrapper.getDataClassName()%> _data = ((<%= wrapper.getDataClassName()%>)this.zGetCurrentDataWithCheck());
        <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> _result = null;

        <% if (relationshipAttributes[ i ].isStorableInArray()) { %>
        if (this.persistenceState == DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL)
        {
            _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _data.<%=relationshipAttributes[i].getGetter()%>();
        }
        <% } %>


        if (_result == null)
        {
                <% if (relationshipAttributes[i].needsPortal()) { %>
                    MithraObjectPortal _portal = null;
                <% } %>
                Operation _op = null;

                <% accessorFilters = relationshipAttributes[ i ].getFilterExpression(); %>
                <%=accessorFilters%>
                {
                    <% if (relationshipAttributes[i].needsPortal()) { %>
                        _portal = <%= relationshipAttributes[i].getRelatedObject().getFinderClassName()%>.getMithraObjectPortal();
                    <% } %>
                    <% if (relationshipAttributes[i].isToOneDirectReference()) { %>
                    if (this.persistenceState == DatedPersistenceState.PERSISTED_NON_TRANSACTIONAL)
                    {
                        int _currentCount = <%= wrapper.getFinderClassName() %>.getMithraObjectPortal().getPerClassUpdateCountHolder().getNonTxUpdateCount();
                        if (this._classUpdateCount == _currentCount)
                        {
                            Object o = <%=relationshipAttributes[i].getGetterForDirectRef()%>;
                            if (o instanceof NullPersistedRelation)
                            {
                                if (((NullPersistedRelation)o).isValid()) return null;
                                _result = null;
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
                            this._classUpdateCount = _currentCount;
                        }
                    }
                    <% } %>
                    <% if (relationshipAttributes[i].isToManyDirectReference()) { %>
                    if (this.persistenceState == DatedPersistenceState.PERSISTED_NON_TRANSACTIONAL)
                    {
                        int _currentCount = <%= wrapper.getFinderClassName() %>.getMithraObjectPortal().getPerClassUpdateCountHolder().getNonTxUpdateCount();
                        if (this._classUpdateCount == _currentCount)
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
                            this._classUpdateCount = _currentCount;
                        }
                    }
                    <% } %>
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
                <%if ( accessorFilters.length() > 0 && relationshipAttributes[i].isListType() )  {%>
                else {
                    _result = new <%=relationshipAttributes[i].getListClassName()%>( new None (
                                     <%=relationshipAttributes[i].getRelatedObject().getClassName()+"Finder."+relationshipAttributes[i].getRelatedObject().getPrimaryKeyAttributes()[0].getName()+"()"%>));
                }
                <% } %>
                <%if ( accessorFilters.length() > 0 && !relationshipAttributes[i].isListType() )  {%>
                else {
                    return null;
                }
                <% } %>

                <% if (!relationshipAttributes[i].isFindByUnique()) { %>
                if (_op != null)
                {
                    _result = <%=relationshipAttributes[ i ].getGetterExpressionForOperation("_op")%>;

                    <% if (relationshipAttributes[i].getCardinality().isToMany()) { %>
                    _result.zSetForRelationship();
                        <% if (relationshipAttributes[i].mustPersistRelationshipChanges()) { %>
                        _result.zSetRemoveHandler(DeleteOnRemoveHandler.getInstance());
                        <% } %>
                    <% } %>
                    <% if (relationshipAttributes[i].hasOrderBy()) { %>
                    _result.setOrderBy(<%= relationshipAttributes[i].getCompleteOrderByForRelationship() %>);
                    <% } // order by %>
                }
                <% } %>
                <% if (relationshipAttributes[i].isDirectReference()) { %>
                if (this.persistenceState == DatedPersistenceState.PERSISTED_NON_TRANSACTIONAL)
                {
                    <%= relationshipAttributes[i].getSetterForDirectRef()%>(_portal.wrapRelatedObject(_result));
                }
                <% } // isDirectReference%>
        }
        return _result;
    }
    <%}//accessors for relationship end%>

    <% if (wrapper.hasArraySettableRelationships())
    {
        for (int i = 0; i < relationshipAttributes.length; i ++)
        {
        if (relationshipAttributes[ i ].isStorableInArray())
        {//operation for relationship mutators start %>

           public void <%=relationshipAttributes[ i ].getSetter()%>(<%=relationshipAttributes[ i ].getTypeAsString()%> <%= relationshipAttributes[i].getName()%>)
           {
               this.ensureSetAttributeAllowed();
               <%=wrapper.getDataClassName()%> _data = this.zGetCurrentDataWithCheck();
               _data.<%=relationshipAttributes[ i ].getSetter()%>(<%= relationshipAttributes[i].getName()%>);

               <% if (relationshipAttributes[i].canSetLocalAttributesFromRelationship()) { %>
                   <% Attribute[] attributesToSet = relationshipAttributes[i].getAttributesToSetOnRelatedObject(); %>
                    if (<%= relationshipAttributes[i].getName()%> == null)
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
                             <%= relationshipAttributes[i].getName() %>.<%= attributesToSet[r].getGetter()%>());
                    <% } %>
                    }
               <% } %>
               }
           <%}}}%>
   


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

    public void zSerializePrimaryKey(ObjectOutput out) throws IOException
    {
        <%=wrapper.getDataClassName()%> data = ((<%= wrapper.getDataClassName()%>)this.zGetCurrentOrTransactionalData());
        data.zSerializePrimaryKey(out);
        <% for (int i=0;i<asOfAttributes.length;i++) { %>
        <%= asOfAttributes[i].getSerializationStatement(asOfAttributes[i].getName())%>;
        <% } %>
    }

    public void zSerializeFullTxData(ObjectOutput out) throws IOException
    {
        this.zSerializeFullData(out);
    }

    private void writeObject(java.io.ObjectOutputStream out)
        throws IOException
    {
        boolean writeFullData = (persistenceState == DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL);
        MithraDataObject dataToWrite = this.zGetCurrentDataWithCheck();
        out.writeBoolean(writeFullData);
        if (writeFullData)
        {
            dataToWrite.zSerializeFullData(out);
            dataToWrite.zSerializeRelationships(out);
        }
        else
        {
            dataToWrite.zSerializePrimaryKey(out);
        }
        <% for (int i=0;i<asOfAttributes.length;i++) { %>
        <%= asOfAttributes[i].getSerializationStatement(asOfAttributes[i].getName())%>;
        <% } %>
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException
    {
        boolean fullData = in.readBoolean();
        this.currentData = new <%= wrapper.getOnHeapDataClassName() %>();
        if (fullData)
        {
            currentData.zDeserializeFullData(in);
            persistenceState = DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
            this.currentData.zDeserializeRelationships(in);
        }
        else
        {
            currentData.zDeserializePrimaryKey(in);
            persistenceState = DatedPersistenceState.PERSISTED_NON_TRANSACTIONAL;
        }
        <% for (int i=0;i<asOfAttributes.length;i++) { %>
            <%= asOfAttributes[i].getName()%> = <%= asOfAttributes[i].getDeserializationStatement()%>;
        <% } %>
    }

    public Object readResolve() throws ObjectStreamException
    {
        if (persistenceState == DatedPersistenceState.PERSISTED_NON_TRANSACTIONAL)
        {
            Operation op = <%= wrapper.getFinderClassName() %>.<%= asOfAttributes[0].getName()%>().eq(<%= asOfAttributes[0].getName()%>);
            <% for (int i=1;i < asOfAttributes.length;i++) { %>
            op = op.and(<%= wrapper.getFinderClassName() %>.<%= asOfAttributes[i].getName()%>().eq(<%= asOfAttributes[i].getName()%>));
            <% } %>
            <% for (int i=0; i < normalAttributes.length ; i++) if (normalAttributes[i].isPrimaryKey()) { %>
            op = op.and(<%= wrapper.getFinderClassName() %>.<%= normalAttributes[i].getName()%>().eq(currentData.<%= normalAttributes[i].getGetter()%>()));
            <% } %>
            <% if (wrapper.hasSourceAttribute()) { %>
            op = op.and(<%= wrapper.getFinderClassName() %>.<%= wrapper.getSourceAttribute().getName()%>().eq(currentData.<%= wrapper.getSourceAttribute().getGetter()%>()));
            <% } %>
            return <%= wrapper.getFinderClassName() %>.findOne(op);
        }
        return this;
    }

    public void zMarkDirty()
    {
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
    public void resetEmbeddedValueObjects()
    {
        <%= wrapper.getDataClassName()%> data = ((<%= wrapper.getDataClassName() %>) this.zGetCurrentDataWithCheck());
        <% for (EmbeddedValue evo : embeddedValueObjects) { %>
            data.<%= evo.getNestedSetter() %>(null);
        <% } %>
    }
<% } %>
}

