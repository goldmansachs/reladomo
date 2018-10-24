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
<%-- Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license --%>
<%@ page import="java.util.*" %>
<%@ page import="com.gs.fw.common.mithra.generator.*" %>
<%@ page import="com.gs.fw.common.mithra.generator.util.StringUtility" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AttributeType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraObjectType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.RelationshipType" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
	String className = wrapper.getListAbstractClassName();
    String mithraObjectClassName = wrapper.getClassName();
    String listClassName = wrapper.getListClassName();
    RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
    AbstractAttribute[] normalAttributes = wrapper.getNormalAndInheritedAndSourceAttributes();
    EmbeddedValue[] embeddedValueObjects = wrapper.getEmbeddedValueObjects();
    Attribute[] nullablePrimitiveAttributes = wrapper.getNullablePrimitiveAttributes();
    Attribute[] pkAttributes = wrapper.getPrimaryKeyAttributes();
    boolean isGenerateGscListMethod = (Boolean) request.getAttribute("generateGscListMethod");
    boolean isGenerateEcListMethod = (Boolean) request.getAttribute("generateEcListMethod");
%>
package <%= wrapper.getPackageName() %>;

import java.util.*;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.list.*;
import com.gs.fw.common.mithra.list.merge.TopLevelMergeOptions;
import com.gs.fw.finder.OrderBy;
<% if (isGenerateEcListMethod) { %>
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.mutable.ListAdapter;
<% } %>
<%@  include file="../Import.jspi" %>
<%@  include file="../DoNotModifyWarning.jspi" %>
// Generated from templates/transactional/ListAbstract.jsp
public class <%= className %> extends DelegatingList<<%= wrapper.getClassName() %>> implements <%= wrapper.getListSuperInterface() %><<%= wrapper.getClassName() %>>
{
    public <%= className %>()
    {
        super();
        this.setDelegated(Abstract<%= wrapper.isTransactional() ? "Transactional": ""%>NonOperationBasedList.DEFAULT);
        Abstract<%= wrapper.isTransactional() ? "Transactional": ""%>NonOperationBasedList.DEFAULT.init(this);
    }

    public <%= className %>(int initialSize)
    {
        super();
        this.setDelegated(Abstract<%= wrapper.isTransactional() ? "Transactional": ""%>NonOperationBasedList.DEFAULT);
        Abstract<%= wrapper.isTransactional() ? "Transactional": ""%>NonOperationBasedList.DEFAULT.init(this, initialSize);
    }

    public <%= className %>(Collection c)
    {
        super();
        this.setDelegated(Abstract<%= wrapper.isTransactional() ? "Transactional": ""%>NonOperationBasedList.DEFAULT);
        Abstract<%= wrapper.isTransactional() ? "Transactional": ""%>NonOperationBasedList.DEFAULT.init(this, c);
    }

    public <%= className %>(com.gs.fw.finder.Operation operation)
    {
        super(operation);
        this.setDelegated(Abstract<%= wrapper.isTransactional() ? "Transactional": ""%>OperationBasedList.DEFAULT);
    }

    public <%= wrapper.getClassName() %>[] elements()
    {
        <%= wrapper.getClassName() %>[] result = new <%= wrapper.getClassName() %>[size()];
        this.getDelegated().toArray(this, result);
        return result;
    }

    public <%= listClassName %> intersection(<%= listClassName %> another)
    {
        return (<%= listClassName %>)super.intersection(another);
    }

    public <%= wrapper.getImplClassName() %> get<%= wrapper.getClassName() %>At(int index)
    {
        return (<%= wrapper.getImplClassName() %>)this.get(index);
    }

    <% for (int i = 0; i < relationshipAttributes.length; i ++)	{ %>
<%@ include file="../RelationshipJavaDoc.jspi" %>
        public <%= relationshipAttributes[ i ].getListInterfaceName() %> <%= relationshipAttributes[i].getGetterNameForList() %>(<%= relationshipAttributes[i].getParameters() %>)
        {
            return (<%= relationshipAttributes[i].getListInterfaceName() %>) this.getDelegated().resolveRelationship(this, <%= wrapper.getFinderClassName()%>.<%= relationshipAttributes[i].getName()%>(<%= relationshipAttributes[i].getParameterVariables() %>));
        }
        <% if (wrapper.isTransactional() && relationshipAttributes[i].hasParentContainer()) { %>
            public void zSetParentContainer<%=relationshipAttributes[i].getName()%>(<%= relationshipAttributes[i].getRelatedObject().getAbstractClassName() %> parent)
            {
                for (int i = 0; i < this.size(); i++)
                {
                    <%= wrapper.getImplClassName() %> item = this.get<%= wrapper.getClassName() %>At(i);
                    item.zSetParentContainer<%=relationshipAttributes[i].getName()%>(parent);
                }
            }
        <% } %>
    <% } %>

    public MithraObjectPortal getMithraObjectPortal()
    {
        return <%= wrapper.getClassName() %>Finder.getMithraObjectPortal();
    }

    public <%= wrapper.getListClassName() %> getNonPersistentCopy()
    {
        <%= wrapper.getListClassName() %> result = new <%= wrapper.getListClassName() %>();
        zCopyNonPersistentInto(result);
        return result;
    }

    public <%= wrapper.getListClassName() %> asAdhoc()
    {
        return (<%= wrapper.getListClassName() %>) super.asAdhoc();
    }

    public <%= wrapper.getListSuperInterface()%> getNonPersistentGenericCopy()
    {
        return this.getNonPersistentCopy();
    }

    <% if (isGenerateGscListMethod) { %>
    /**
     * Return a view of this list that implements GS Collections MutableList API.
     * Since the returned list will be operation-based, it is effectively read-only,
     * so mutating methods will throw a RuntimeException.
     * (Implemented by a light-weight adapter, not a copy)
     */
    public com.gs.collections.api.list.MutableList<<%= wrapper.getClassName() %>> asGscList()
    {
        return com.gs.collections.impl.list.mutable.ListAdapter.adapt(this);
    }
    <% } %>

    <% if (isGenerateEcListMethod) { %>
    /**
    * Return a view of this list that implements Eclipse Collections MutableList API.
    * Since the returned list will be operation-based, it is effectively read-only,
    * so mutating methods will throw a RuntimeException.
    * (Implemented by a light-weight adapter, not a copy)
    */
    public org.eclipse.collections.api.list.MutableList<<%= wrapper.getClassName() %>> asEcList()
    {
        return org.eclipse.collections.impl.list.mutable.ListAdapter.adapt(this);
    }
    <% } %>

    <% if (wrapper.isTransactional()) { %>
    public <%= wrapper.getListClassName() %> merge(MithraTransactionalList<<%= wrapper.getClassName() %>> incoming, TopLevelMergeOptions<<%= wrapper.getClassName() %>> mergeOptions)
    {
        return (<%= wrapper.getListClassName() %>) super.merge(incoming, mergeOptions);
    }

    public <%= wrapper.getListClassName() %> getDetachedCopy()
    {
        <%= wrapper.getListClassName() %> result = new <%= wrapper.getListClassName() %>();
        zDetachInto(result);
        return result;
    }

    public void zMakeDetached(Operation op, Object previousDetachedList)
    {
        super.zMakeDetached(op, previousDetachedList);
    }

    <% if (wrapper.hasPkGeneratorStrategy()) { %>
        protected void generateAndSetPrimaryKeys()
        {
        <% if (wrapper.hasSimulatedSequencePkGeneratorStrategy()) { %>
            <% if (wrapper.hasSourceAttribute()) { %>
                    <% for (int i = 0; i < pkAttributes.length; i++) { %>
                        <% if (pkAttributes[i].isPrimaryKeyUsingSimulatedSequence()) { %>
                            <% boolean sequenceHasSourceAttribute = pkAttributes[i].hasSimulatedSequenceSourceAttribute(); %>
                            zGenerateAndSetPrimaryKeysWithSourceAttribute((SequenceAttribute)<%= wrapper.getClassName()%>Finder.<%= pkAttributes[i].getName()%>(), <%= sequenceHasSourceAttribute %>, "<%= pkAttributes[i].getSimulatedSequence().getSequenceName() %>", "<%= pkAttributes[i].getSimulatedSequence().getSequenceObjectFactoryName() %>");
                        <% } %>
                    <% } %>
            <% } else { %>
                    <% for (int i = 0; i < pkAttributes.length; i++) { %>
                        <% if (pkAttributes[i].isPrimaryKeyUsingSimulatedSequence()) { %>
                            <% boolean sequenceHasSourceAttribute = pkAttributes[i].hasSimulatedSequenceSourceAttribute(); %>
                            zGenerateAndSetPrimaryKeysForSingleSource((SequenceAttribute)<%= wrapper.getClassName()%>Finder.<%= pkAttributes[i].getName()%>(), <%= sequenceHasSourceAttribute %>, "<%= pkAttributes[i].getSimulatedSequence().getSequenceName() %>", "<%= pkAttributes[i].getSimulatedSequence().getSequenceObjectFactoryName() %>", null);
                        <% } %>
                    <% } %>
            <% } %>
        <% } %>
        }
    <% } %>

    <% for (AbstractAttribute normalAttribute : normalAttributes) { %>
        public void <%= normalAttribute.getSetter() %>(<%= normalAttribute.getTypeAsString() %> newValue)
        {
            zSet<%= normalAttribute.getType().getJavaTypeStringPrimary()%>(<%= wrapper.getFinderClassName()%>.<%=normalAttribute.getName()%>(), newValue);
        }
    <% } %>

    <% for (EmbeddedValue object : embeddedValueObjects) { %>
        public void <%= object.getNestedCopyValuesFrom() %>(<%= object.getType() %> newValue)
        {
            zSetEvoValue(<%= wrapper.getFinderClassName()%>.<%=object.getNestedName()%>(), newValue);
        }
    <% } %>

    <% for (AbstractAttribute nullablePrimitiveAttribute : nullablePrimitiveAttributes) { %>
        public void <%= nullablePrimitiveAttribute.getSetter() %>Null()
        {
            zSetAttributeNull(<%= wrapper.getFinderClassName()%>.<%=nullablePrimitiveAttribute.getName()%>());
        }
    <% } %>

    <% if (wrapper.hasDependentRelationships()) { %>
    protected void zCascadeDeleteRelationships()
    {
        <% for (int i = 0; i < relationshipAttributes.length; i++) { %>
            <% if (relationshipAttributes[i].hasSetter() && relationshipAttributes[i].isRelatedDependent()
                && relationshipAttributes[i].getRelatedObject().isTransactional()
                && !relationshipAttributes[i].getRelatedObject().hasAsOfAttributes()) { %>
                ((DelegatingList)this.<%= relationshipAttributes[i].getGetterNameForList() %>()).cascadeDeleteAll();
            <% } %>
        <% } %>
    }
    <% } %>
    <% if (wrapper.hasAsOfAttributes()) { %>
    public void purgeAll()
    {
        super.purgeAll();
    }

    public void terminateAll()
    {
        super.terminateAll();
    }
    <% } %>

<% } %>

}