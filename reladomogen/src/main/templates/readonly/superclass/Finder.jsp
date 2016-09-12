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
<%@ page import="com.gs.fw.common.mithra.generator.mapper.*" %>
<%@ page import="com.gs.fw.common.mithra.generator.queryparser.ASTAttributeName" %>
<%@ page import="com.gs.fw.common.mithra.generator.queryparser.ASTRelationalExpression" %>
<%@ page import="com.gs.fw.common.mithra.generator.util.StringUtility" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AttributeType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraObjectType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.RelationshipType" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
    CommonAttribute[] normalAndSourceAttributes = wrapper.getNormalAndSourceAttributes();
    EmbeddedValue[] embeddedValueObjects = wrapper.getEmbeddedValueObjects();
    RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
    AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
    String className = wrapper.getClassName() + "Finder";
%>
package <%= wrapper.getPackageName() %>;

<%@ include file="../../Import.jspi" %>
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.attribute.*;

<%@  include file="../../DoNotModifyWarning.jspi" %>
<% if (wrapper.isTablePerSubclassConcreteClass()) { %>
    public interface <%=className%><Result extends <%= wrapper.getClassName() %>> extends <%= wrapper.getSuperClassWrapper().getFinderClassName() %><Result>
<% } else if (wrapper.isTablePerClassSubClass()) { %>
    public interface <%=className%><Result extends <%= wrapper.getClassName() %>> extends <%= wrapper.getSuperClassWrapper().getFinderClassName() %>.<%= wrapper.getSuperClassWrapper().getClassName() %>RelatedFinder<Result>
<% } else { %>
    public interface <%=className%><Result extends <%= wrapper.getClassName() %>> extends RelatedFinder<Result>
<% } %>
{
	public RelatedFinder getRelationshipFinderByName(String relationshipName);
	public Attribute getAttributeByName(String attributeName);
	<% if (wrapper.hasSourceAttribute()) { %>
		public SourceAttributeType getSourceAttributeType();
	<% } %>

    <% for (CommonAttribute normalAndSourceAttribute : normalAndSourceAttributes) { %>
        public <%= normalAndSourceAttribute.getFinderAttributeType() %> <%=normalAndSourceAttribute.getName() %>();
    <% } %>

    <% for (EmbeddedValue object : embeddedValueObjects) { %>
        public <%= object.getQualifiedAttributeClassName() %> <%= object.getNestedName() %>();
    <% } %>

    <% for (RelationshipAttribute relationshipAttribute : relationshipAttributes) { %>
		public <%= relationshipAttribute.getFinderAttributeTypeForRelatedClass() %> <%=relationshipAttribute.getName() %>(<%= relationshipAttribute.getParameters()%>);
	<% } %>

    <% for (AsOfAttribute asOfAttribute : asOfAttributes) { %>
        public <%= asOfAttribute.getFinderAttributeType() %> <%=asOfAttribute.getName() %>();
    <% } %>
}
