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
	String className = wrapper.getClassName() + "List";
	String abstractClassName = wrapper.getClassName() + "ListAbstract";
%>
package <%= wrapper.getPackageName() %>;

import com.gs.fw.finder.Operation;

import java.util.*;
<%@  include file="../CvsComment.jspi" %>
public class <%=wrapper.getListClassName()%> extends <%=wrapper.getListAbstractClassName()%> <% if (wrapper.isGenerateInterfaces()){%> implements <%=wrapper.getListInterfaceName()%><%}%>
{
<%@  include file="../VersionId.jspi" %>
    public <%=wrapper.getListClassName()%>()
    {
        super();
    }

    public <%=wrapper.getListClassName()%>(int initialSize)
    {
        super(initialSize);
    }

    public <%=wrapper.getListClassName()%>(Collection c)
    {
        super(c);
    }

    public <%=wrapper.getListClassName()%>(Operation operation)
    {
        super(operation);
    }

}
