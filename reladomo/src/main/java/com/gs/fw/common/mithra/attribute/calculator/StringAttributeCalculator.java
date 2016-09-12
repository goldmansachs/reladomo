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
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.finder.UpdateCountHolder;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;

import java.io.Serializable;
import java.util.Set;


public interface StringAttributeCalculator extends Serializable, UpdateCountHolder
{

    public String stringValueOf(Object o);

    public boolean isAttributeNull(Object o);

    public MithraObjectPortal getOwnerPortal();

    public String getFullyQualifiedCalculatedExpression(SqlQuery query);

    public void addDepenedentAttributesToSet(Set set);

    public AsOfAttribute[] getAsOfAttributes();

    public void addDependentPortalsToSet(Set set);

    public String getTopOwnerClassName();

    public void appendToString(ToStringContext toStringContext);

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext);
}
