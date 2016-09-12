/*
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
 */

package com.gs.fw.common.mithra.finder.string;

import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.util.WildcardParser;



public class StringContainsOperation extends StringLikeOperation
{

    public StringContainsOperation(StringAttribute attribute, String parameter)
    {
        super(attribute, parameter);
    }

    protected String getLikeParameter(SqlQuery sqlQuery)
    {
        return "%" + sqlQuery.getDatabaseType().escapeLikeMetaChars(this.getParameter()) + "%";
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("contains").append("\""+this.getParameter()+"\"");
    }

    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        String s = ((StringAttribute) this.getAttribute()).stringValueOf(o);
        return s != null && s.indexOf(this.getParameter()) >=0;
    }
}
