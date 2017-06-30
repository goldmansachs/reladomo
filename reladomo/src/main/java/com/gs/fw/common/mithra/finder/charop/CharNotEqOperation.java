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

import com.gs.fw.common.mithra.attribute.CharAttribute;
import com.gs.fw.common.mithra.extractor.CharExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.AtomicNotEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.paramop.OpWithCharParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithCharParamExtractor;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CharNotEqOperation  extends AtomicNotEqualityOperation implements SqlParameterSetter, OpWithCharParam
{
    private char parameter;

    public CharNotEqOperation(CharAttribute attribute, char parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    @Override
    protected Extractor getStaticExtractor()
    {
        return OpWithCharParamExtractor.INSTANCE;
    }

    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        CharExtractor charAttribute = (CharExtractor) extractor;
        if (charAttribute.isAttributeNull(o)) return false;
        return charAttribute.charValueOf(o) != parameter;
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setString(startIndex, new String(new char[] { parameter} ));
        return 1;
    }

    public int hashCode()
    {
        return ~(this.getAttribute().hashCode() ^ this.parameter);
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof CharNotEqOperation)
        {
            CharNotEqOperation other = (CharNotEqOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    public Object getParameterAsObject()
    {
        return new Character(this.parameter);
    }

    public char getParameter()
    {
        return parameter;
    }
}
