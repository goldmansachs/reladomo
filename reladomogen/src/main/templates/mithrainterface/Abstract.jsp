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
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AttributeInterfaceType"%>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AsOfAttributeInterfaceType"%>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.SourceAttributeInterfaceType"%>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType" %>
<%@ page import="com.gs.fw.common.mithra.generator.util.StringUtility" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.*" %>
<%
	MithraInterfaceType wrapper = (MithraInterfaceType) request.getAttribute("mithraWrapper");
	String packageName = wrapper.getPackageName();
	String classType = wrapper.getClassName();
    String lowerCaseClassType = StringUtility.firstLetterToLower(classType);
    String interfaceName = wrapper.getAbstractClassName();
    AttributeInterfaceType[] attributes = wrapper.getAttributesAsArray();
    AsOfAttributeInterfaceType[] asOfAttributeInterfaceTypes = wrapper.getAsOfAttributesAsArray();
    RelationshipInterfaceType[] relationshipAttributes = wrapper.getRelationshipsAsArray();
%>
package <%= packageName %>;

<%@  include file="../Import.jspi" %>
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.finder.Operation;

import java.sql.Timestamp;
import java.util.Date;
import java.math.BigDecimal;

<%@  include file="../DoNotModifyWarning.jspi" %>
<% if (wrapper.hasSuperInterface()) { %>
public interface <%= interfaceName %> extends <%= wrapper.getSuperInterfacesForAbstract() %>
<% } else { %>
public interface <%= interfaceName %>
<% } %>
{
   <% if(wrapper.hasSourceAttribute()) { %>
      public <%= wrapper.getSourceAttribute().getTypeAsString() %> <%= wrapper.getSourceAttribute().getGetter() %>();
      <% if(!wrapper.isReadOnlyInterfaces()) { %>
      public void <%= wrapper.getSourceAttribute().getSetter() %>(<%= wrapper.getSourceAttribute().getTypeAsString() %> newValue);
      <% } %>
   <% } %>

   <% for (AttributeInterfaceType attribute : attributes) { %>
        public <%= attribute.getTypeAsString() %> <%= attribute.getGetter() %>();
        <% if(!wrapper.isReadOnlyInterfaces()) { %>
        public void <%= attribute.getSetter() %>(<%=attribute.getTypeAsString()%> newValue);
        <% } %>
   <% } %>

    <% for (AsOfAttributeInterfaceType attribute : asOfAttributeInterfaceTypes) { %>
        public Timestamp <%= attribute.getGetterFrom() %>();
        public Timestamp <%= attribute.getGetterTo() %>();

        <% if(!wrapper.isReadOnlyInterfaces()) { %>
        public void <%= attribute.getSetterFrom() %>(Timestamp newValue);
        public void <%= attribute.getSetterTo() %>(Timestamp newValue);
        <% } %>
    <% } %>

    <% for (int i = 0; i < relationshipAttributes.length; i++) { %>
        public <%=relationshipAttributes[ i ].getTypeAsString()%> <%=relationshipAttributes[ i ].getGetter()%>(<%=relationshipAttributes[ i ].getParameters() %>);
    <%}%>

}