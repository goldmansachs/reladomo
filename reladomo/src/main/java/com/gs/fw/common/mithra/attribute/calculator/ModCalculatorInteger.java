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

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;

import java.util.Set;
import java.math.BigDecimal;



public class ModCalculatorInteger extends AbstractSingleAttributeCalculator
 {

     private int divisor;

     public ModCalculatorInteger(IntegerAttribute attribute, int divisor)
     {
         super(attribute);
         this.divisor = divisor;
     }

     public int intValueOf(Object o)
     {
         return ((IntegerAttribute)this.attribute).intValueOf(o) % divisor;
     }

     public BigDecimal bigDecimalValueOf(Object o)
     {
         return BigDecimal.valueOf(intValueOf(o));
     }

     public float floatValueOf(Object o)
     {
         return intValueOf(o);
     }

     public long longValueOf(Object o)
     {
         return intValueOf(o);
     }

     public double doubleValueOf(Object o)
     {
         return intValueOf(o);
     }

     public String getFullyQualifiedCalculatedExpression(SqlQuery query)
     {
         return query.getDatabaseType().getModFunction(this.attribute.getFullyQualifiedLeftHandExpression(query), divisor);
     }

     public void appendToString(ToStringContext toStringContext)
     {
         toStringContext.append("(");
         ((Attribute)this.attribute).zAppendToString(toStringContext);
         toStringContext.append("mod");
         toStringContext.append(""+divisor);
         toStringContext.append(")");
     }

     public boolean equals(Object obj)
     {
         if (this == obj) return true;
         if (obj instanceof ModCalculatorInteger)
         {
             ModCalculatorInteger other = (ModCalculatorInteger) obj;
             return this.attribute.equals(other.attribute) && this.divisor == other.divisor;
         }
         return false;
     }

     public int hashCode()
     {
         return 0x3742A2D0 ^ this.attribute.hashCode();
     }

     public boolean execute(double object, Object context)
     {
         WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
         return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(((int)object) % divisor, realContext.getWrappedContext());
     }

     public boolean executeForNull(Object context)
     {
         WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
         return ((NullHandlingProcedure)realContext.getWrappedProcedure()).executeForNull(realContext.getWrappedContext());
     }

     public boolean execute(int object, Object context)
     {
         WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
         return ((IntegerProcedure)realContext.getWrappedProcedure()).execute(object % divisor, realContext.getWrappedContext());
     }

     public boolean execute(float object, Object context)
     {
         WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
         return ((FloatProcedure)realContext.getWrappedProcedure()).execute(((int)object) % divisor, realContext.getWrappedContext());
     }

     public boolean execute(long object, Object context)
     {
         WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
         return ((LongProcedure)realContext.getWrappedProcedure()).execute(((int)object) % divisor, realContext.getWrappedContext());
     }

     public boolean execute(BigDecimal object, Object context)
     {
         WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
         return ((BigDecimalProcedure)realContext.getWrappedProcedure()).execute(object.remainder(BigDecimal.valueOf(divisor)), realContext.getWrappedContext());
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

     public void generateMapperSql(AggregateSqlQuery query)
     {
         this.attribute.generateMapperSql(query);
     }

     public Operation createMappedOperation()
     {
         return this.attribute.zCreateMappedOperation();
     }

     public int getScale()
     {
         return 0;
     }

     public int getPrecision()
     {
         return 0;
     }

     public void forEach(DoubleProcedure proc, Object o, Object context)
     {
         context = new WrappedProcedureAndContext(proc, context);
         this.attribute.forEach((DoubleProcedure)this, o, context);
     }

     public void forEach(FloatProcedure proc, Object o, Object context)
     {
         context = new WrappedProcedureAndContext(proc, context);
         this.attribute.forEach((FloatProcedure)this, o, context);
     }

     public void forEach(LongProcedure proc, Object o, Object context)
     {
         context = new WrappedProcedureAndContext(proc, context);
         this.attribute.forEach((LongProcedure)this, o, context);
     }

     public void forEach(IntegerProcedure proc, Object o, Object context)
     {
         context = new WrappedProcedureAndContext(proc, context);
         this.attribute.forEach((IntegerProcedure)this, o, context);
     }

     public void forEach(BigDecimalProcedure proc, Object o, Object context)
     {
         context = new WrappedProcedureAndContext(proc, context);
         this.attribute.forEach((BigDecimalProcedure)this, o, context);
     }
 }
