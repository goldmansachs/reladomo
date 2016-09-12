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
<%@ page import="com.gs.fw.common.mithra.generator.MithraBaseObjectTypeWrapper" %>
<%@ page import="com.gs.fw.common.mithra.generator.MithraEmbeddedValueObjectTypeWrapper" %>
<%
	MithraEmbeddedValueObjectTypeWrapper wrapper = (MithraEmbeddedValueObjectTypeWrapper) request.getAttribute("mithraWrapper");
	String packageName = wrapper.getPackageName();
	String superClassType = wrapper.getFullyQualifiedSuperClassType();
	String classType = wrapper.getClassName();
    String dataClassType = classType + "Data";
    MithraBaseObjectTypeWrapper.NullBitsHolder[] nullBitsHolders = wrapper.getNullBitsHolders();
    MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper[] attributes = wrapper.getSortedAttributes();
    MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper[] embeddedValueObjects = wrapper.getSortedNestedObjects();
    List<RelationshipAttribute> relationships = wrapper.getRelationshipAttributes();
%>
package <%= packageName %>;

<%@  include file="../Import.jspi" %>
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;
import java.math.BigDecimal;

<%@  include file="../DoNotModifyWarning.jspi" %>
public class <%= dataClassType %> implements Serializable
{
    <% if (nullBitsHolders != null) { %>
        <% for (MithraBaseObjectTypeWrapper.NullBitsHolder nullBitsHolder : nullBitsHolders) { %>
            private <%= nullBitsHolder.getType() %> <%= nullBitsHolder.getName() %>;
        <% } %>
    <% } %>
    <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : attributes) { %>
        private <%= attribute.getTypeAsString() %> <%= attribute.getName() %>;
    <% } %>
    <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper object : embeddedValueObjects) { %>
        private final <%= object.getTypeAsString() %> <%= object.getName() %> = new <%= object.getTypeAsString() %>();
    <% } %>

    <% for (RelationshipAttribute rel: relationships) { %>
        <% if (!rel.hasParameters()) { %>
    	private <%=rel.getTypeAsString()%> <%=rel.getName()%>;
    	<% } %>
    <% } %>
    <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : attributes) { %>
        public <%= attribute.getTypeAsString() %> <%= attribute.getGetter() %>()
        {
            return this.<%= attribute.getName() %>;
        }

        public void <%= attribute.getSetter() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>)
        {
            this.<%= attribute.getName() %> = <%= attribute.getName() %>;
            <% if (attribute.isPrimitive()) { %>
                <%= wrapper.getNotNullSetterExpressionForAttribute(attribute) %>;
            <% } %>
        }

        public boolean <%= attribute.getNullGetter() %>()
        {
            <% if (attribute.isPrimitive()) { %>
                return <%= wrapper.getNullGetterExpressionForAttribute(attribute) %>;
            <% } else { %>
                return this.<%= attribute.getGetter() %>() == null;
            <% } %>
        }

        public void <%= attribute.getNullSetter() %>()
        {
            <% if (attribute.isPrimitive()) { %>
                <%= wrapper.getNullSetterExpressionForAttribute(attribute) %>;
            <% } else { %>
                this.<%= attribute.getSetter() %>(null);
            <% } %>
        }

        public void <%= attribute.getSetterUntil() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>, Timestamp exclusiveUntil)
        {
            throw new UnsupportedOperationException("<%= attribute.getSetterUntil() %> is not supported on transient objects.");
        }

        <% if (attribute.isDoubleAttribute()) { %>
            public void <%= attribute.getIncrementer() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>)
            {
                throw new UnsupportedOperationException("<%= attribute.getIncrementer() %> is not supported on transient objects.");
            }

            public void <%= attribute.getIncrementerUntil() %>(<%= attribute.getTypeAsString() %> <%= attribute.getName() %>, Timestamp exclusiveUntil)
            {
                throw new UnsupportedOperationException("<%= attribute.getIncrementerUntil() %> is not supported on transient objects.");
            }
        <% } %>
    <% } %>

    <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper object : embeddedValueObjects) { %>
        public <%= object.getTypeAsString() %> <%= object.getGetter() %>()
        {
            return this.<%= object.getName() %>;
        }

        public void <%= object.getCopyValuesFrom() %>(<%= object.getTypeAsString() %> <%= object.getName() %>)
        {
            <% for (MithraEmbeddedValueObjectTypeWrapper.AttributeWrapper attribute : object.getAttributes()) { %>
                this.<%= object.getName() %>.<%= attribute.getSetter() %>(<%= object.getName() %>.<%= attribute.getGetter() %>());
            <% } %>
            <% for (MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper nestedObject : object.getNestedObjects()) { %>
                this.<%= object.getName() %>.<%= nestedObject.getCopyValuesFrom() %>(<%= object.getName() %>.<%= nestedObject.getGetter() %>());
            <% } %>
        }

        public void <%= object.getCopyValuesFromUntil() %>(<%= object.getTypeAsString() %> <%= object.getName() %>, Timestamp exclusiveUntil)
        {
            throw new UnsupportedOperationException("<%= object.getCopyValuesFromUntil() %> is not supported on transient objects.");
        }
    <% } %>

    <% for (RelationshipAttribute rel: relationships) { %>

        <% if (!rel.hasParameters()) { %>
    	public <%=rel.getTypeAsString()%> <%=rel.getGetter()%>()
        {
            return this.<%= rel.getName() %>;
        }

        public void <%=rel.getSetter()%>(<%=rel.getTypeAsString()%> <%= rel.getName()%>)
        {
            this.<%= rel.getName() %> = <%= rel.getName() %>;
        }
        <% } %>
    <% } %>

}