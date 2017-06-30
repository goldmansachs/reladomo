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
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.util.WildcardParser;


public class StringWildCardEqOperation extends StringLikeOperation
{
    private static final long serialVersionUID = -2531503578551321234L;

    private final WildcardParser parser;

    public StringWildCardEqOperation(StringAttribute attribute, String pattern)
    {
        super(attribute, pattern);
        this.parser = new WildcardParser(pattern);
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("wildCardEquals").append("\""+this.getParameter()+"\"");
    }

    @Override
    protected String getLikeParameter(SqlQuery sqlQuery)
    {
        return sqlQuery.getDatabaseType().getSqlLikeExpression(parser);
    }

    @Override
    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        return parser.matches((String) extractor.valueOf(o));
    }
}
