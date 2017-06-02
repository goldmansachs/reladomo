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

package com.gs.fw.common.mithra.finder.charop;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.extractor.CharExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.paramop.OpWithCharParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithCharParamExtractor;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CharGreaterThanOperation extends GreaterThanOperation implements OpWithCharParam
{
    private char parameter;

    public CharGreaterThanOperation(Attribute attribute, char parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    public char getParameter()
    {
        return parameter;
    }

    @Override
    public Extractor getStaticExtractor()
    {
        return OpWithCharParamExtractor.INSTANCE;
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append(">").append(this.parameter);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setString(startIndex, new String(new char[] { parameter} ));
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.parameter ^ 0xF0F0F0;
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof CharGreaterThanOperation)
        {
            CharGreaterThanOperation other = (CharGreaterThanOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    @Override
    protected boolean matchesWithoutDeleteCheck(Object holder, Extractor extractor)
    {
        return !extractor.isAttributeNull(holder) && ((CharExtractor) extractor).charValueOf(holder) > this.getParameter();
    }
}
