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

package com.gs.fw.common.mithra.attribute.calculator;

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;

import java.util.Set;


public class SubstringCalculator implements StringAttributeCalculator
{

    private StringAttribute attribute;
    private int start;
    private int end;

    public SubstringCalculator(StringAttribute attribute, int start, int end)
    {
        this.attribute = attribute;
        this.start = start;
        this.end = end;
    }

    public String stringValueOf(Object o)
    {
        String s = this.attribute.stringValueOf(o);
        if (s == null) return null;
        if (s.length() <= start) return "";
        int end = this.end;
        if (end > s.length() || end < 0) end = s.length();
        return s.substring(this.start, end);
    }

    public boolean isAttributeNull(Object o)
    {
        return this.attribute.isAttributeNull(o);
    }

    public MithraObjectPortal getOwnerPortal()
    {
        return this.attribute.getOwnerPortal();
    }

    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        return query.getDatabaseType().createSubstringExpression(this.attribute.getFullyQualifiedLeftHandExpression(query), this.start, this.end);
    }

    public void appendToString(ToStringContext toStringContext)
    {
        toStringContext.append("substring(");
        this.attribute.zAppendToString(toStringContext);
        toStringContext.append(",").append(""+this.start).append(",").append(""+this.end);
        toStringContext.append(")");
    }

    @Override
    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return ((SingleColumnAttribute)this.attribute).createTupleAttribute(pos, tupleTempContext);
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        this.attribute.zAddDepenedentAttributesToSet(set);
    }

    public void addDependentPortalsToSet(Set set)
    {
        this.attribute.zAddDependentPortalsToSet(set);
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return this.attribute.getAsOfAttributes();
    }

    @Override
    public void setUpdateCountDetachedMode(boolean isDetachedMode)
    {
        this.attribute.setUpdateCountDetachedMode(isDetachedMode);
    }

    public int getUpdateCount()
    {
        return attribute.getUpdateCount();
    }

    public int getNonTxUpdateCount()
    {
        return attribute.getNonTxUpdateCount();
    }

    public void incrementUpdateCount()
    {
        attribute.incrementUpdateCount();
    }

    public void commitUpdateCount()
    {
        attribute.commitUpdateCount();
    }

    public void rollbackUpdateCount()
    {
        attribute.rollbackUpdateCount();
    }

    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj instanceof SubstringCalculator)
        {
            return this.attribute.equals(((SubstringCalculator)obj).attribute);
        }
        return false;
    }

    public String getTopOwnerClassName()
    {
        return this.attribute.zGetTopOwnerClassName();
    }

    public int hashCode()
    {
        return 0x83AEF345 ^ this.attribute.hashCode();
    }
}
