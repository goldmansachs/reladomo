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
<%@ page import="com.gs.fw.common.mithra.generator.MithraEnumerationTypeWrapper" %>
<%
	MithraEnumerationTypeWrapper wrapper = (MithraEnumerationTypeWrapper) request.getAttribute("mithraWrapper");
	String packageName = wrapper.getPackageName();
	String classType = wrapper.getClassName();
%>
<%@ include file="../CvsComment.jspi" %>
package <%= packageName %>;

public enum <%= classType %>
{
    <% for (int i = 0; i < wrapper.getMembers().length; i++) { %>
        <% MithraEnumerationTypeWrapper.MemberWrapper member = wrapper.getMembers()[i]; %>
        <%= member.getName() %>
        <% if (i != wrapper.getMembers().length - 1) { %>
            ,
        <% } else {%>
            ;
        <% } %>
    <% } %>
    <%@ include file="../VersionId.jspi" %>
}
