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
<%@ page import="com.gs.fw.common.mithra.generator.*" %>
<%
	MithraObjectTypeWrapper wrapper = (MithraObjectTypeWrapper) request.getAttribute("mithraWrapper");
    AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
%>
package <%= wrapper.getPackageName() %>;

import java.sql.Timestamp;
<%@  include file="../CvsComment.jspi" %>
public class <%=wrapper.getImplClassName()%> extends <%=wrapper.getAbstractClassName()%><% if (wrapper.isGenerateInterfaces()){%> implements <%=wrapper.getInterfaceName()%><%}%>
{
<%@  include file="../VersionId.jspi" %>

    public <%=wrapper.getImplClassName()%>(Timestamp <%= asOfAttributes[0].getName()%>
    <% for (int i=1;i<asOfAttributes.length;i++) { %>
            , Timestamp <%= asOfAttributes[i].getName()%>
    <% } %>
        )
    {
        super(<%= asOfAttributes[0].getName()%>
        <% for (int i=1;i<asOfAttributes.length;i++) { %>
        ,<%= asOfAttributes[i].getName()%>
        <% } %>
        );
        // You must not modify this constructor. Mithra calls this internally.
        // You can call this constructor. You can also add new constructors.
    }

    <% if (wrapper.hasBusinessDateAsOfAttribute() && wrapper.hasProcessingDate()) { %>
    public <%=wrapper.getImplClassName()%>(Timestamp <%= wrapper.getBusinessDateAsOfAttribute().getName()%>)
    {
        super(<%= wrapper.getBusinessDateAsOfAttribute().getName()%>);
    }
    <% } %>

    <% if (!wrapper.hasBusinessDateAsOfAttribute() && wrapper.hasProcessingDate()) { %>
    public  <%=wrapper.getImplClassName()%>()
    {
    this(<%= wrapper.getProcessingDateAttribute().getInfinityExpression() %>);
    }
    <% } %>
}
