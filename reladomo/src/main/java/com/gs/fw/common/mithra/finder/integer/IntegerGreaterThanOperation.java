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

package com.gs.fw.common.mithra.finder.integer;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.paramop.OpWithIntParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithIntParamExtractor;

import java.sql.PreparedStatement;
import java.sql.SQLException;



public class IntegerGreaterThanOperation extends GreaterThanOperation implements OpWithIntParam
{

    private int parameter;

    public IntegerGreaterThanOperation(Attribute attribute, int parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    public int getParameter()
    {
        return parameter;
    }

    @Override
    public Extractor getStaticExtractor()
    {
        return OpWithIntParamExtractor.INSTANCE;
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append(">").append(this.parameter);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setInt(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.parameter ^ 0xF0F0F0;
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof IntegerGreaterThanOperation)
        {
            IntegerGreaterThanOperation other = (IntegerGreaterThanOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    @Override
    protected boolean matchesWithoutDeleteCheck(Object holder, Extractor extractor)
    {
        return !extractor.isAttributeNull(holder) && ((IntExtractor) extractor).intValueOf(holder) > this.getParameter();
    }
}
