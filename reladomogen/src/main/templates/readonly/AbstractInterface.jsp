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
<%@ page import="java.util.Iterator" %>
<%@ page import="com.gs.fw.common.mithra.generator.*" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
    AbstractAttribute[] normalAttributes = wrapper.getSortedNormalAndSourceAttributes();
    Attribute[] nullablePrimitiveAttributes = wrapper.getNullablePrimitiveAttributes();
    RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
    AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
%>

<%@  include file="../DoNotModifyWarning.jspi" %>
package <%= wrapper.getPackageName() %>;

<% for (Iterator iterator = wrapper.getImports().iterator(); iterator.hasNext();)
	{
		String classToImport = (String) iterator.next();
	%>
import <%=classToImport%>;	<%}%>

<%if(wrapper.hasSuperClass() && wrapper.getSuperClassWrapper() != null){%>
public interface <%=wrapper.getAbstractInterfaceName()%> <% if (wrapper.hasSuperClass()){%> extends <%=wrapper.getFullyQualifiedSuperClassInterface()%><%}%>
<%}else{%>
public interface <%=wrapper.getAbstractInterfaceName()%> 
<%}%>
{

 <% for (int i = 0; i < normalAttributes.length; i++){ %>
        <%=normalAttributes[i].getTypeAsString()%> <%=normalAttributes[i].getGetter()%>();
        <%if(!wrapper.isReadOnlyInterfaces()) {%>
        void <%=normalAttributes[i].getSetter()%>(<%=normalAttributes[i].getTypeAsString()%> newValue);
        <%}%>
        boolean <%= normalAttributes[i].getNullGetter() %>;
        <% if (!wrapper.isReadOnlyInterfaces() && wrapper.isTransactional() && wrapper.hasBusinessDateAsOfAttribute() && !(normalAttributes[i].isAsOfAttributeFrom() || normalAttributes[i].isAsOfAttributeTo() )) { %>
        void <%= normalAttributes[i].getSetter()%>Until(<%=normalAttributes[i].getTypeAsString()%> newValue, Timestamp exclusiveUntil);
        <% if(normalAttributes[i].isDoubleAttribute() || normalAttributes[i].isBigDecimalAttribute()){%>
        void <%= normalAttributes[i].getIncrementer()%>(<%=normalAttributes[i].getTypeAsString()%> increment);
        void <%= normalAttributes[i].getIncrementer()%>Until(<%=normalAttributes[i].getTypeAsString()%> increment, Timestamp exclusiveUntil);
        <%}%>
        <%}%>
<%}%>
<%if(!wrapper.isReadOnlyInterfaces()){%>
<% for (int i = 0; i < nullablePrimitiveAttributes.length; i ++) { %>
    void <%= nullablePrimitiveAttributes[i].getSetter() %>Null();
<%}%>
<%}%>

<% for (int i = 0; i < relationshipAttributes.length; i++){%>
    public <%=relationshipAttributes[ i ].getTypeAsString()%> <%=relationshipAttributes[ i ].getGetter()%>(<%=relationshipAttributes[ i ].getParameters() %>);
    <% if (!wrapper.isReadOnlyInterfaces() && wrapper.isTransactional() && relationshipAttributes[i].hasSetter()) { %>
	void <%=relationshipAttributes[ i ].getSetter()%>(<%=relationshipAttributes[ i ].getTypeAsString()%> <%= relationshipAttributes[i].getName()%>);
    <%}%>
<%}%>

<% for (int i=0;i<asOfAttributes.length;i++) { %>
    <%=asOfAttributes[i].getTypeAsString()%> <%=asOfAttributes[ i ].getGetter()%>();
    <% } %>
}