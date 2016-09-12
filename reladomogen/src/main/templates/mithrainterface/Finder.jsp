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
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.RelationshipInterfaceType" %>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AsOfAttributeInterfaceType"%>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.SourceAttributeInterfaceType"%>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType"%>

<%
	MithraInterfaceType wrapper = (MithraInterfaceType) request.getAttribute("mithraWrapper");
	String className = wrapper.getClassName();
    String interfaceName = className + "Finder<Result>";
    String superClassName = "RelatedFinder<Result>";
    AttributeInterfaceType[] attributes = wrapper.getAttributesAsArray();
    RelationshipInterfaceType[] relationships = wrapper.getRelationshipsAsArray();
    AsOfAttributeInterfaceType[] asOfAttributeInterfaceType = wrapper.getAsOfAttributesAsArray();
%>
package <%= wrapper.getPackageName() %>;

<%@  include file="../Import.jspi" %>
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.PrintablePreparedStatement;
import com.gs.fw.common.mithra.finder.RelatedFinder;

<%@  include file="../DoNotModifyWarning.jspi" %>
<% if (wrapper.hasSuperInterface()) { %>
public interface <%= interfaceName %> extends <%= wrapper.getSuperInterfacesForFinder() %>
<% } else { %>
public interface <%= interfaceName %> extends RelatedFinder<Result>
 <% } %>
{

   <% if(wrapper.hasSourceAttribute()) { %>
      public <%= wrapper.getSourceAttribute().getAttributeClassName() %> <%= wrapper.getSourceAttribute().getName() %>();
   <% } %>

     <% for (AttributeInterfaceType attributeWrapper : attributes) { %>
        public <%= attributeWrapper.getAttributeClassName() %> <%=attributeWrapper.getName() %>();
     <% } %>

    <% for (AsOfAttributeInterfaceType asOfAttribute : asOfAttributeInterfaceType) { %>
          public AsOfAttribute <%= asOfAttribute.getName() %>();
    <% } %>

    <% for (RelationshipInterfaceType relationshipAttribute : relationships)	{ %>
      public <%= relationshipAttribute.getRelatedObjectClassNameForTemplate()%> <%= relationshipAttribute.getName()%>(<%= relationshipAttribute.getParameters() %>);

    <% } %>


}
