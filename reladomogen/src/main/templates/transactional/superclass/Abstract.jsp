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
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AttributeType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraObjectType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.ObjectType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.RelationshipType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
	AbstractAttribute[] attributes = wrapper.getNormalAndSourceAttributes();
    EmbeddedValue[] embeddedValueObjects = wrapper.getEmbeddedValueObjects();
    RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
    AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
	String className = wrapper.getImplClassName() + "Abstract";
    MithraInterfaceType[] mithraInterfaceTypes = wrapper.getMithraInterfaces();
%>
package <%= wrapper.getPackageName() %>;

<%@  include file="../../Import.jspi" %>
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.update.*;
import com.gs.fw.common.mithra.behavior.*;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.list.*;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;
import java.util.HashSet;
import java.util.Arrays;

<%@  include file="../../DoNotModifyWarning.jspi" %>
// Generated from templates/transactional/superclass/Abstract.jsp

<% if (wrapper.hasSuperClass()) { %>
public abstract class <%= className %> extends <%= wrapper.getFullyQualifiedSuperClassType() %> implements <%= wrapper.getMithraInterfaceName() %>
    <% if (wrapper.hasMithraInterfaces()) { %>
      <% for (int i=0;i<mithraInterfaceTypes.length;i++) { %>
          , <%=mithraInterfaceTypes[i].getClassName()%>
        <% } %>
    <% } %>
<% } else if (wrapper.canExtendNonGeneratedSuperClass()) { %>
public abstract class <%= className %> extends <%= wrapper.getNonGeneratedSuperClassName()%>
    <% if (wrapper.hasMithraInterfaces()) { %>
      implements
      <% for (int i=0;i<mithraInterfaceTypes.length-1;i++) { %>
           <%=mithraInterfaceTypes[i].getClassName()%>,
        <% } %>
       <%=mithraInterfaceTypes[mithraInterfaceTypes.length-1].getClassName()%>
    <% } %>
<% } else {%>
public abstract class <%= className %> implements <%= wrapper.getMithraInterfaceName() %>
    <% if (wrapper.hasMithraInterfaces()) { %>
      <% for (int i=0;i<mithraInterfaceTypes.length;i++) { %>
          , <%=mithraInterfaceTypes[i].getClassName()%>
        <% } %>
    <% } %>
<% } %>
{
    //add getters for all attributes
	<% for (AbstractAttribute attribute : attributes) { %>
        <%= attribute.getVisibility() %> abstract boolean <%= attribute.getNullGetter() %>;
        <%= attribute.getVisibility() %> abstract <%= attribute.getTypeAsString() %> <%= attribute.getGetter() %>();
		<%= attribute.getVisibility() %> abstract void <%= attribute.getSetter() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>);
        <% if (attribute.isNullablePrimitive()) { %>
        <%= attribute.getVisibility() %> abstract void <%= attribute.getSetter() %>Null();
        <% } %>
    <% } %>

    <% for (EmbeddedValue evo : embeddedValueObjects) { %>
        <%= evo.getVisibility() %> abstract <%= evo.getType() %> <%= evo.getNestedGetter() %>();
        <%= evo.getVisibility() %> abstract void <%= evo.getNestedCopyValuesFrom() %>(<%= evo.getType() %> <%= evo.getName() %>);
    <% } %>

    <% for (int i = 0; i < relationshipAttributes.length; i++) { %>
        public abstract <%= relationshipAttributes[ i ].getTypeAsString() %> <%= relationshipAttributes[ i ].getGetter() %>(<%= relationshipAttributes[i].getParameters()%>);
    <% } %>
	<% for (AsOfAttribute asOfAttribute : asOfAttributes) { %>
        public abstract <%= asOfAttribute.getTypeAsString() %> <%= asOfAttribute.getGetter() %>();
    <% } %>

    <% if(wrapper.isTransactional() && !wrapper.hasAsOfAttributes())
       {
          boolean flag = true;
          MithraObjectTypeWrapper[] subWrappers = wrapper.getSubClasses();
          for(MithraObjectTypeWrapper subWrapper:subWrappers)
          {
              if(subWrapper.hasAsOfAttributes())
              {
                  flag = false;
                  break;
              }
          }
          if(flag){
    %>
    public <%= wrapper.getImplClassName() %> getDetachedCopy() throws MithraBusinessException
    {
        return (<%= wrapper.getImplClassName() %>) super.getDetachedCopy();
    }

    public <%= wrapper.getImplClassName() %> getNonPersistentCopy() throws MithraBusinessException
    {
        return (<%= wrapper.getImplClassName() %>) super.getNonPersistentCopy();
    }
    <%}}%>
}
