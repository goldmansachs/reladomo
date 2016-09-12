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
<%@ page import="java.util.*"%>
<%@ page import="com.gs.fw.common.mithra.generator.*"%>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.AttributeType"%>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.MithraObjectType"%>
<%@ page import="com.gs.fw.common.mithra.generator.metamodel.RelationshipType"%>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
    RelationshipAttribute[] relationshipAttributes = wrapper.getRelationshipAttributes();
    AbstractAttribute[] normalAttributes = wrapper.getSortedNormalAndSourceAttributes();
%>
package <%= wrapper.getPackageName() %>;

<% for (Iterator iterator = wrapper.getImports().iterator(); iterator.hasNext();)
	{
		String classToImport = (String) iterator.next();
	%>
import <%=classToImport%>;	<%}%>

public interface <%=wrapper.getListAbstractInterfaceName()%> extends List<<%=wrapper.getClassName()%>>
{
<% for (int i = 0; i < relationshipAttributes.length; i++){%>
    public <%= relationshipAttributes[i].getListInterfaceName() %> <%= relationshipAttributes[i].getGetterNameForList() %>(<%= relationshipAttributes[i].getParameters() %>);
<%}%>
<% if(!wrapper.isReadOnlyInterfaces() && wrapper.isTransactional()){%>
<% for (AbstractAttribute normalAttribute : normalAttributes) { %>
        public void <%= normalAttribute.getSetter() %>(<%= normalAttribute.getTypeAsString() %> newValue);
    <% } %>
<%}%>
}