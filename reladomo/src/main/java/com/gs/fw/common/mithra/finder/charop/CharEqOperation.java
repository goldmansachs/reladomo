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
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.finder.AtomicEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


public class CharEqOperation  extends AtomicEqualityOperation implements SqlParameterSetter
{
    private char parameter;

    public CharEqOperation(CharAttribute attribute, char parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        CharAttribute charAttribute = (CharAttribute)this.getAttribute();
        if (charAttribute.isAttributeNull(o)) return false;
        return charAttribute.charValueOf(o) == parameter;
    }

    protected List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), parameter);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setString(startIndex, new String(new char[] { parameter} ));
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.parameter;
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof CharEqOperation)
        {
            CharEqOperation other = (CharEqOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    @Override
    public int getParameterHashCode()
    {
        return HashUtil.hash(this.parameter);
    }

    public Object getParameterAsObject()
    {
        return new Character(this.parameter);
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        if (extractor.isAttributeNull(other)) return false;
        return ((CharExtractor) extractor).charValueOf(other) == this.parameter;
    }

    public char getParameter()
    {
        return parameter;
    }

    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements CharExtractor
    {
        public char charValueOf(Object o)
        {
            return getParameter();
        }

        public Object valueOf(Object o)
        {
            return new Character(this.charValueOf(o));
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.charValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.charValueOf(first) == this.charValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((CharExtractor) secondExtractor).charValueOf(second) == this.charValueOf(first);
        }
    }
}
