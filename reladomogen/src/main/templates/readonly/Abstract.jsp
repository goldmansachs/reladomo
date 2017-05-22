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
<%@ page import="com.gs.fw.common.mithra.generator.type.DateJavaType" %>
<%@ page import="com.gs.fw.common.mithra.generator.util.StringUtility" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AttributeType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraObjectType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.RelationshipType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
    AbstractAttribute[] attributes = wrapper.getSortedNormalAndSourceAttributes();
    EmbeddedValue[] embeddedValueObjects = wrapper.getEmbeddedValueObjects();
    Attribute[] shadowAttributes = wrapper.getShadowAttributes();
	RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
    String finderClassName = wrapper.getFinderClassName();
    MithraInterfaceType[] mithraInterfaceTypes = wrapper.getMithraInterfaces();
    String accessorFilters = "";
%>

package <%= wrapper.getPackageName() %>;

<%@  include file="../Import.jspi" %>
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.list.*;
import java.util.HashSet;
import java.util.Arrays;

<%@  include file="../DoNotModifyWarning.jspi" %>
// Generated from templates/readonly/Abstract.jsp
<% if (wrapper.hasSuperClass()) { %>
public abstract class <%=wrapper.getAbstractClassName()%> extends <%=wrapper.getFullyQualifiedSuperClassType()%>
<% } else { %>
public abstract class <%=wrapper.getAbstractClassName()%>
<% } %>
implements MithraObject, Serializable
<% if (wrapper.hasMithraInterfaces()) { %>
    <% for (int i=0;i<mithraInterfaceTypes.length;i++) { %>
        , <%=mithraInterfaceTypes[i].getClassName()%>
    <% } %>
<% } %>
{
    <% if (wrapper.hasArraySettableRelationships()) { %>
    private Object[] _relationships;
    <% } %>
    <% if (!wrapper.isTablePerClassSubClass()) { %>
        <%= wrapper.getCommonDataModifier() %> byte persistenceState = PersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
    <% } %>

<%@  include file="../Relationships.jspi" %>

    <%@ include file="common/AttributesAndGetters.jspi" %>

<% if (wrapper.hasDirectRefs()) { %>
    private int _classUpdateCount;
<% } %>

<% for (RelationshipAttribute rel : relationshipAttributes) { %>
    <% if (rel.isDirectReference()) { %>
    private Object <%= rel.getDirectRefVariableName() %>;
    <% } %>
<% } %>
    <% for (EmbeddedValue evo : embeddedValueObjects) { %>
        <%= evo.getVisibility() %> <%=evo.isFinalGetter() ? "final " : ""%><%= evo.getType() %> <%= evo.getNestedGetter() %>()
        {
            <%= evo.getType() %> evo = this.<%= evo.getNestedName() %>;
            if (evo == null)
            {
                evo = <%= evo.getType() %>.<%= evo.getFactoryMethodName() %>(this, <%= finderClassName %>.<%= evo.getNestedName() %>());
                this.<%= evo.getNestedName() %> = evo;
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
    <% } %>

    public MithraDataObject zGetCurrentData()
    {
        <%= wrapper.getDataClassName() %> data = new <%= wrapper.getOnHeapDataClassName() %>();
        <% if (wrapper.isTablePerClassSubClass()) { %>
                ((<%= wrapper.getSuperClassWrapper().getDataClassName() %>)super.zGetCurrentData()).copyInto(data, false);
        <% } %>
        <% for (int i = 0; i < attributes.length; i ++) { %>
            data.<%= attributes[i].getPrivateSetter() %>(<%= attributes[ i ].getName() %>);
        <% } %>
        <% if (nullBitsHolders != null) { %>
            <% for (int i = 0; i < nullBitsHolders.length; i++) { %>
                data.zSet<%= StringUtility.firstLetterToUpper(nullBitsHolders[i].getName()) %>(this.<%= nullBitsHolders[i].getName() %>);
            <% } %>
        <% } %>
        return data;
    }

    public <%= wrapper.getImplClassName() %> getNonPersistentCopy() throws MithraBusinessException
    {
        MithraDataObject newData = this.zGetCurrentData();
        <%= wrapper.getAbstractClassName() %> copy = (<%= wrapper.getAbstractClassName() %>) <%= wrapper.getClassName()%>Finder.getMithraObjectPortal().getMithraObjectFactory().createObject(newData);
        copy.persistenceState = PersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
        return (<%= wrapper.getImplClassName() %>)copy; 
    }

    protected void zSetFrom<%= wrapper.getDataClassName() %>(<%= wrapper.getDataClassName() %> data)
    {
        this.setFrom<%= wrapper.getDataClassName() %>(data);
    }

    public void setFrom<%= wrapper.getDataClassName() %>(<%= wrapper.getDataClassName() %> data)
    {
        <% if (wrapper.isTablePerClassSubClass()) { %>
                super.setFrom<%= wrapper.getSuperClassWrapper().getDataClassName() %>(data);
        <% } else { %>
                this.persistenceState = PersistenceState.PERSISTED_NON_TRANSACTIONAL;
        <% } %>
        <% for (int i = 0; i < attributes.length; i ++) { %>
   		    <%= attributes[i].getName() %> = data.<%= attributes[i].getPrivateGetter() %>();
        <% } %>
        <% if (nullBitsHolders != null) { %>
            <% for (int i = 0; i < nullBitsHolders.length; i++) { %>
                this.<%= nullBitsHolders[i].getName() %> = data.zGet<%= StringUtility.firstLetterToUpper(nullBitsHolders[i].getName()) %>();
            <% } %>
        <% } %>
        <% if (wrapper.hasEmbeddedValueObjects()) { %>this.resetEmbeddedValueObjects(); <% } %>
    }

    public void zSetNonTxPersistenceState(int state)
    {
        this.persistenceState = (byte) state;
    }

    public MithraObjectPortal zGetPortal()
    {
        return <%= wrapper.getClassName()%>Finder.getMithraObjectPortal();
    }

    public boolean isDeletedOrMarkForDeletion()
    {
        return this.persistenceState == PersistenceState.DELETED;
    }

    public void zReindexAndSetDataIfChanged(MithraDataObject data, Cache cache)
    {
        if (this.zChanged(data))
        {
            cache.reindex(this, data, null, null);
        }
    }

    public void zSetData(MithraDataObject data, Object optionalBehavior)
    {
        this.setFrom<%=wrapper.getDataClassName()%>((<%=wrapper.getDataClassName()%>)data);
    }

    private void throwNullPrimitiveException(String attributeName)
    {
        throw new MithraNullPrimitiveException("attribute '"+attributeName+"' is null in database and a default is not specified in mithra xml for primary key / "+this.zGetPrintablePrimaryKey());
    }

	<% for (int i = 0; i < attributes.length; i ++) { %>
        <%= attributes[i].getVisibility() %> <%=attributes[i].isFinalGetter() ? "final " : ""%><%= attributes[i].getTypeAsString() %> <%= attributes[ i ].getGetter() %>()
        {
            <% if (attributes[i].isNullablePrimitive() && ((Attribute) attributes[i]).getDefaultIfNull() == null) { %>
                if (<%= attributes[i].getNullGetter() %>) throwNullPrimitiveException("<%= attributes[i].getName() %>");
            <% } %>
            return this.<%= attributes[i].getName() %>;
        }

        <% if (attributes[i].getType() instanceof DateJavaType) { %>
            <%= attributes[i].getVisibility() %> void <%= attributes[i].getSetter() %>(java.util.Date value)
            {
                this.ensureSetAttributeAllowed();
                if (value == null || value instanceof java.sql.Date)
                {
                    this.<%= attributes[i].getName() %> = value;
                }
                else
                {
                    this.<%= attributes[i].getName() %> = new java.sql.Date(value.getTime());
                }
        <% } else { %>
            <%= attributes[i].getVisibility() %> void <%= attributes[i].getSetter() %>(<%=attributes[i].getTypeAsString()%> value)
            {
                this.ensureSetAttributeAllowed();
            <% if (attributes[i].isPoolable()) { %>
                this.<%= attributes[i].getName() %> = <%= attributes[i].getType().getJavaTypeString() %>Pool.getInstance().getOrAddToCache(value, <%= wrapper.getFinderClassName()%>.isFullCache());
            <% } else { %>
                this.<%= attributes[i].getName() %> = value;
            <% } %>
                <% if (attributes[i].isNullablePrimitive()) { %>
                    <%= attributes[i].getNotNullSetterExpression() %>;
                <% } %>
        <% } %>
                <% if (wrapper.hasEmbeddedValueObjects()) { %>this.resetEmbeddedValueObjects(); <% } %>
            }
    <% } %>

    private void ensureSetAttributeAllowed()
    {
        if (this.persistenceState != PersistenceState.IN_MEMORY_NON_TRANSACTIONAL)
        {
            throw new MithraBusinessException("Cannot set an attribute of a persisted read-only object");
        }
    }

    <% for (int i = 0; i < nullablePrimitiveAttributes.length; i ++) { %>
        <%= nullablePrimitiveAttributes[i].getVisibility() %> void <%= nullablePrimitiveAttributes[i].getSetter() %>Null()
        {
            this.ensureSetAttributeAllowed();
            <%= wrapper.getNullSetterExpressionForIndex(i) %>;
            <% if (wrapper.hasEmbeddedValueObjects()) { %>this.resetEmbeddedValueObjects(); <% } %>
        }
    <% } %>

<% if (wrapper.hasDirectRefs()) { %>
    protected void zClearAllDirectRefs()
    {
    <% for (RelationshipAttribute rel : relationshipAttributes) { %>
        <% if (rel.isDirectReference()) { %>
        <%= rel.getDirectRefVariableName() %> = null;
        <% } %>
    <% } %>
    }
<% } %>

			<% for (int i = 0; i < relationshipAttributes.length; i ++)	{//accessors for relationship attributes start %>
<%@ include file="../RelationshipJavaDoc.jspi" %>
	public <%=relationshipAttributes[i].isFinalGetter() ? "final " : ""%><%=relationshipAttributes[ i ].getTypeAsString()%> <%=relationshipAttributes[ i ].getGetter()%>(<%= relationshipAttributes[i].getParameters()%>)
    {
        <%=relationshipAttributes[ i ].getMithraImplTypeAsString()%> _result = null;

        <% if (relationshipAttributes[ i ].isStorableInArray()) { %>
        if (_relationships != null)
        {
            _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>)(_relationships[<%= relationshipAttributes[i].getPositionInObjectArray()%>]);
        }
        <% } %>


        if (_result == null)
        {

            <% if (relationshipAttributes[i].needsPortal()) { %>
                MithraObjectPortal _portal = null;
            <% } %>

            <% accessorFilters = relationshipAttributes[ i ].getFilterExpression(); %>
            <%=accessorFilters%>
            {
                <% if (relationshipAttributes[i].needsPortal()) { %>
                    _portal = <%= relationshipAttributes[i].getRelatedObject().getFinderClassName()%>.getMithraObjectPortal();
                <% } %>
                <% if (relationshipAttributes[i].isToOneDirectReference()) { %>
                if (this.persistenceState == PersistenceState.PERSISTED_NON_TRANSACTIONAL)
                {
                    int _currentCount = <%= wrapper.getFinderClassName() %>.getMithraObjectPortal().getPerClassUpdateCountHolder().getNonTxUpdateCount();
                    if (this._classUpdateCount == _currentCount)
                    {
                        Object o = this.<%= relationshipAttributes[i].getDirectRefVariableName()%>;
                        if (o instanceof NullPersistedRelation)
                        {
                            if (((NullPersistedRelation)o).isValid()) return null;
                        }
                        else
                        {
                            _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _portal.unwrapRelatedObject(this, o, <%= relationshipAttributes[i].getDirectRefFromExtractorName()%>, <%= relationshipAttributes[i].getDirectRefToExtractorName()%>);
                            if (_result != null) return _result;
                        }
                    }
                    else
                    {
                        this.zClearAllDirectRefs();
                        this._classUpdateCount = _currentCount;
                    }
                }
                <% } %>
                <% if (relationshipAttributes[i].isToManyDirectReference()) { %>
                if (this.persistenceState == PersistenceState.PERSISTED_NON_TRANSACTIONAL)
                {
                    int _currentCount = <%= wrapper.getFinderClassName() %>.getMithraObjectPortal().getPerClassUpdateCountHolder().getNonTxUpdateCount();
                    if (this._classUpdateCount == _currentCount)
                    {
                        Object o = this.<%= relationshipAttributes[i].getDirectRefVariableName()%>;
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
                        this.zClearAllDirectRefs();
                        this._classUpdateCount = _currentCount;
                    }
                }
                <% } %>
                <% if (relationshipAttributes[i].isFindByUnique()) { %>
                       _result = (<%=relationshipAttributes[i].getMithraImplTypeAsString()%>) <%= relationshipAttributes[i].getRelatedObject().getFinderClassName()%>.<%= relationshipAttributes[i].getFindByUniqueMethodName() %>(<%= relationshipAttributes[i].getFindByUniqueParameters() %>);
                        <% if (relationshipAttributes[i].requiresOverSpecifiedParameterCheck()) { %>
                            if (_result != null && !(<%=relationshipAttributes[i].getOverSpecificationCheck()%>)) _result = null;
                        <% } %>
                <% } else { %>
                <% if (relationshipAttributes[i].isResolvableInCache()) { %>
                Object _related = _portal.<%= relationshipAttributes[i].getCacheLookupMethod()%>FromCache(this, null, <%= relationshipAttributes[i].getRhs()%><%= relationshipAttributes[i].getCacheLookUpParameters()%>);
                if (!(_related instanceof NulledRelation)) _result = (<%=relationshipAttributes[ i ].getMithraImplTypeAsString()%>) _related;
                    <% if (relationshipAttributes[i].requiresOverSpecifiedParameterCheck()) { %>
                        if (_result != null && !(<%=relationshipAttributes[i].getOverSpecificationCheck()%>)) _result = null;
                    <% } %>
                if (_related == null)
                <% } %>
                {
                    _result = <%=relationshipAttributes[ i ].getGetterExpression()%>;
                    <% if (relationshipAttributes[i].getCardinality().isToMany()) { %>
                    _result.zSetForRelationship();
                    <% } %>
                }
                <% } // isFindByUnique %>
            }
            <%if ( (accessorFilters.length() > 0) && (relationshipAttributes[i].isListType()) )  {%>
            else {
                _result = new <%=relationshipAttributes[i].getListClassName()%>( new None (
                                 <%=relationshipAttributes[i].getRelatedObject().getClassName()+"Finder."+relationshipAttributes[i].getRelatedObject().getPrimaryKeyAttributes()[0].getName()+"()"%>)
                 );
            }
            <% } %>
            <%if ( (accessorFilters.length() > 0) && !relationshipAttributes[i].isListType() )  {%>
            else {
                return null;
            }
            <% } %>

            <% if (relationshipAttributes[i].isDirectReference()) { %>
            if (this.persistenceState == PersistenceState.PERSISTED_NON_TRANSACTIONAL)
            {
                this.<%= relationshipAttributes[i].getDirectRefVariableName()%> = _portal.wrapRelatedObject(_result);
            }
            <% } %>

            <% if (relationshipAttributes[i].hasOrderBy()) { %>
            _result.setOrderBy(<%= relationshipAttributes[i].getCompleteOrderByForRelationship() %>);
            <% } // order by %>
    }
        return _result;
    }

<%}//accessors for relationship attributes end%>


    <% if (wrapper.hasArraySettableRelationships())
    {
        for (int i = 0; i < relationshipAttributes.length; i ++)
        {
            if (relationshipAttributes[ i ].isStorableInArray())
            {//operation for relationship mutators start %>

            public void <%=relationshipAttributes[ i ].getSetter()%>(<%=relationshipAttributes[ i ].getTypeAsString()%> <%= relationshipAttributes[i].getName()%>)
            {
                this.ensureSetAttributeAllowed();

                if (_relationships == null)
                {
                    _relationships = new Object[<%= wrapper.getSettableRelationshipCount() %>];
                }
                _relationships[<%= relationshipAttributes[i].getPositionInObjectArray()%>] = <%= relationshipAttributes[i].getName()%>;

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
                        this.<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getSetter() %>(<%= relationshipAttributes[i].getAttributeToGetForSetOnRelatedObject(r).getPrimitiveCastType(attributesToSet[r])%><%= relationshipAttributes[i].getName() %>.<%= attributesToSet[r].getGetter()%>());
                    <% } %>
                    }
                <% } %>
                }
        <%}}}
        %>

    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{//operation for relationship start %>
	public Operation <%= relationshipAttributes[i].getGetterOperationMethodName()%>(<%= relationshipAttributes[i].getParameters()%>)
    {
        <%=wrapper.getAbstractClassName()%> _data = this;
		return <%=relationshipAttributes[ i ].getOperationExpression()%>;
    }
    <%}//operation for relationship end%>

    protected boolean zChanged(MithraDataObject newData)
    {
<% if (wrapper.isTablePerClassSubClass()) { %>
        if (super.zChanged(newData)) return true;
<% } %>
        <% String dataVariable = StringUtility.firstLetterToLower(wrapper.getClassName())+"Data"; %>
        final <%=wrapper.getClassName()%>Data <%=dataVariable%> = (<%=wrapper.getClassName()%>Data) newData;

        <% if(nullBitsHolders != null) {
            for (int i = 0; i < nullBitsHolders.length; i ++) {%>
        if (<%=nullBitsHolders[i].getName()%> != <%=dataVariable%>.zGet<%=StringUtility.firstLetterToUpper(nullBitsHolders[i].getName())%>())
        {
            return true;
        }
            <% } %>
        <% } %>

        <% for (int i = 0; i < attributes.length; i ++) {%>
            <% if (!attributes[i].isPrimaryKey()) {  %>
                <% if (attributes[i].isPrimitive()) {  %>
        if (<%=attributes[i].getName()%> != <%=dataVariable%>.<%=attributes[i].getGetter()%>())
            {return true;}
                <% } else { %>

        if (!<%=attributes[i].getNullGetter()%> ? !<%=attributes[i].getGetter()%>().equals(<%=dataVariable%>.<%=attributes[i].getGetter()%>()) : !<%=dataVariable%>.<%=attributes[i].getNullGetter()%>)
            {return true;}
                <% } %>
            <% } %>
        <% } %>

        return false;
    }

<% if (!wrapper.isTablePerClassSubClass()) { %>

    protected String zGetPrintablePrimaryKey()
    {
        String result = "";
        <% for (int i=0;i<attributes.length;i++)
        {
            if (attributes[i].isPrimaryKey() || attributes[i].isSourceAttribute())
            {
        %>
        result += <%= attributes[i].getPrintableForm() %>+ " / ";
        <%  }
        }
        %>
        return result;
    }

    private void writeObject(ObjectOutputStream out)
    throws IOException
    {
        final boolean fullData = this.persistenceState == PersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
        out.writeBoolean(fullData);
        if (fullData)
        {
            this.zSerializeFullData(out);
            <% if (wrapper.hasArraySettableRelationships()) { %>
            if (_relationships == null)
            {
                out.writeInt(0);
            }
            else
            {
                out.writeInt(_relationships.length);
                for(int i=0;i < _relationships.length;i++)
                {
                    out.writeObject(_relationships[i]);
                }
            }
            <% } %>
        }
        else
        {
            <% for (AbstractAttribute attribute : attributes) {
                if (attribute.isPrimaryKey()) {%>
                <%= attribute.getSerializationStatement()%>;
            <% } } %>
        }
    }

<% } %>
    public void zWriteDataClassName(ObjectOutput out) throws IOException
    {
<% if (wrapper.isTablePerClassSubClass() || wrapper.isTablePerClassSuperClass()) { %>
        out.writeObject("<%= wrapper.getPackageName()%>.<%= wrapper.getDataClassName()%>");
<% } %>
    }

    public void zSerializeFullTxData(ObjectOutput out) throws IOException
    {
        this.zSerializeFullData(out);
    }

    protected void zReadObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        this.readObject(in);
    }

    private void readObject(ObjectInputStream in)
    throws IOException, ClassNotFoundException
    {
<% if (wrapper.isTablePerClassSubClass()) { %>
        super.zReadObject(in);
        final boolean fullData = this.persistenceState == PersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
<% } else { %>
        final boolean fullData = in.readBoolean();
<% } %>
        if (fullData)
        {
        <%
            if (nullBitsHolders != null)
            {
                for (MithraBaseObjectTypeWrapper.NullBitsHolder nullBitsHolder : nullBitsHolders)
                {
            %>
            this.<%= nullBitsHolder.getName()%> = in.read<%= nullBitsHolder.getIoType()%>();
            <%  }
            }
        %>
            <% for (AbstractAttribute attribute : attributes) { %>
            <%= attribute.getDeserializationStatement()%>;
            <% } %>
            this.persistenceState = PersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
            <% if (wrapper.hasArraySettableRelationships()) { %>
            int total = in.readInt();
            if(total > 0)
            {
                _relationships = new Object[total];
                for(int i=0;i < total;i++)
                {
                    _relationships[i] = in.readObject();
                }
            }
            <% } %>
        }
        else
        {
            <% for (AbstractAttribute attribute : attributes) {
                if (attribute.isPrimaryKey()) {%>
                <%= attribute.getDeserializationStatement()%>;
            <% } } %>
            this.persistenceState = PersistenceState.PERSISTED_NON_TRANSACTIONAL;
        }
    }

    public Object readResolve() throws ObjectStreamException
    {
        if (this.persistenceState == PersistenceState.PERSISTED_NON_TRANSACTIONAL)
        {
            <% AbstractAttribute[] pkAttributes = wrapper.getPrimaryKeyAttributes(); %>
            Operation op = <%= wrapper.getFinderClassName() %>.<%= pkAttributes[0].getName()%>().eq(this.<%= pkAttributes[0].getGetter()%>());
                <% for (int i = 1; i < pkAttributes.length; i++) { %>
            op = op.and(<%= wrapper.getFinderClassName() %>.<%= pkAttributes[i].getName()%>().eq(this.<%= pkAttributes[i].getGetter()%>()));
                <% } %>
            return <%= wrapper.getFinderClassName() %>.findOne(op);
        }
        return this;
    }

    public void zMarkDirty()
    {
    }

    private void resetEmbeddedValueObjects()
    {
        <% for (EmbeddedValue evo : embeddedValueObjects) { %>
           this.<%= evo.getNestedName() %> = null;
        <% } %>
    }
}
