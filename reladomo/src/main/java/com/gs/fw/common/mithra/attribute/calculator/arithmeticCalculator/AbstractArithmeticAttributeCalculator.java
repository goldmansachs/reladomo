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

package com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator;

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.HashUtil;

import java.util.Set;
import java.math.BigDecimal;


public abstract class AbstractArithmeticAttributeCalculator implements ArithmeticAttributeCalculator
{
    private NumericAttribute attribute1;
    private NumericAttribute attribute2;

    public AbstractArithmeticAttributeCalculator(NumericAttribute attribute1, NumericAttribute attribute2)
    {
        this.attribute1 = attribute1;
        this.attribute2 = attribute2;
    }

    public NumericAttribute getAttribute1()
    {
        return attribute1;
    }

    public NumericAttribute getAttribute2()
    {
        return attribute2;
    }

    public String getTopOwnerClassName()
    {
        return this.attribute1.zGetTopOwnerClassName();
    }

    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        StringBuilder builder = new StringBuilder("(");
        builder.append(this.getAttribute1ExpressionForAggregation(query));
        builder.append(this.getOperatorString());
        builder.append(this.getAttribute2ExpressionForAggregation(query));
        builder.append(")");
        return builder.toString();
    }

    public void appendToString(ToStringContext toStringContext)
    {
        toStringContext.append("(");
        ((Attribute)this.attribute1).zAppendToString(toStringContext);
        toStringContext.append(this.getOperatorString());
        ((Attribute)this.attribute2).zAppendToString(toStringContext);
        toStringContext.append(")");
    }

    @Override
    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleIntegerAttribute(pos, true, tupleTempContext);
    }

    protected abstract CharSequence getOperatorString();

    public String getAttribute1ExpressionForAggregation(SqlQuery query)
    {
        return this.getAttribute1().getFullyQualifiedLeftHandExpression(query);
    }

    public String getAttribute2ExpressionForAggregation(SqlQuery query)
    {
        return this.getAttribute2().getFullyQualifiedLeftHandExpression(query);
    }

    public boolean isAttributeNull(Object o)
    {
        return false;
    }

    //todo: check with moh - should i return the owner or top level portal?
    public MithraObjectPortal getOwnerPortal()
    {
        return attribute1.getOwnerPortal();
    }

    public MithraObjectPortal getTopLevelPortal()
    {
        return attribute1.getTopLevelPortal();
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        ((Attribute)attribute1).zAddDepenedentAttributesToSet(set);
        ((Attribute)attribute2).zAddDepenedentAttributesToSet(set);
    }

    public void addDependentPortalsToSet(Set set)
    {
        ((Attribute)attribute1).zAddDependentPortalsToSet(set);
        ((Attribute)attribute2).zAddDependentPortalsToSet(set);
    }

    //    public int hashCode()
//    {
//        return this.attribute1.hashCode() ^ this.attribute2.hashCode();
//    }
//
//    public boolean equals(Object obj)
//    {
//        if (obj == this) return true;
//        if (obj instanceof ArithmeticAttributeCalculator)
//        {
//            ArithmeticAttributeCalculator other = (ArithmeticAttributeCalculator) obj;
//            return this.attribute1.equals(other.getAttribute1()) && this.attribute2.equals(other.getAttribute2());
//        }
//        return false;        
//    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return null;
    }

    @Override
    public void setUpdateCountDetachedMode(boolean isDetachedMode)
    {
        this.attribute1.setUpdateCountDetachedMode(isDetachedMode);
    }

    public int getUpdateCount()
    {
        return attribute1.getUpdateCount() + attribute2.getUpdateCount();
    }

    public int getNonTxUpdateCount()
    {
        return attribute1.getNonTxUpdateCount() + attribute2.getNonTxUpdateCount();
    }

    public void incrementUpdateCount()
    {
    }

    public void commitUpdateCount()
    {
    }

    public void rollbackUpdateCount()
    {
    }

    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.getAttribute1().generateMapperSql(query);
        this.getAttribute2().generateMapperSql(query);
    }

    public Operation createMappedOperation()
    {
        return this.getAttribute1().zCreateMappedOperation().and(this.getAttribute2().zCreateMappedOperation());
    }

    protected void forEach(final FloatAndContext floatAndContext, final FloatProcedure innerProcedure, final Object o, final InnerFloatProcedureForNull innerProcedureForNull, Object context1)
    {
        this.getAttribute1().forEach(new FloatProcedure()
        {
            public boolean execute(final float firstFloat, Object context2)
            {
                floatAndContext.setValue(firstFloat);
                floatAndContext.setContext(context2);
                getAttribute2().forEach(innerProcedure, o, floatAndContext);
                return false;
            }

            public boolean executeForNull(Object context2)
            {
                getAttribute2().forEach(innerProcedureForNull, o, floatAndContext);
                return false;
            }
        }, o, context1);
    }

    protected void forEach(final LongAndContext longAndContext, final LongProcedure innerProcedure,
                           final Object o, final InnerLongProcedureForNull innerProcedureForNull, Object context1)
    {
        this.getAttribute1().forEach(new LongProcedure()
        {
            public boolean execute(final long firstLong, Object context2)
            {
                longAndContext.setValue(firstLong);
                longAndContext.setContext(context2);
                getAttribute2().forEach(innerProcedure, o, longAndContext);
                return false;
            }

            public boolean executeForNull(Object context2)
            {
                getAttribute2().forEach(innerProcedureForNull, o, longAndContext);
                return false;
            }
        }, o, context1);
    }

    protected void forEach(final IntegerAndContext intAndContext, final IntegerProcedure innerProcedure,
                           final Object o, final InnerIntProcedureForNull innerProcedureForNull, Object context1)
    {
        this.getAttribute1().forEach(new IntegerProcedure()
        {
            public boolean execute(final int firstInt, Object context2)
            {
                intAndContext.setValue(firstInt);
                intAndContext.setContext(context2);
                getAttribute2().forEach(innerProcedure, o, intAndContext);
                return false;
            }

            public boolean executeForNull(Object context2)
            {
                getAttribute2().forEach(innerProcedureForNull, o, intAndContext);
                return false;
            }
        }, o, context1);
    }

    protected void forEach(final DoubleAndContext doubleAndContext, final DoubleProcedure innerProcedure,
                           final Object o, final InnerDoubleProcedureForNull innerProcedureForNull, Object context1)
    {
        this.getAttribute1().forEach(new DoubleProcedure()
        {
            public boolean execute(final double firstDouble, Object context2)
            {
                doubleAndContext.setValue(firstDouble);
                doubleAndContext.setContext(context2);
                getAttribute2().forEach(innerProcedure, o, doubleAndContext);
                return false;
            }

            public boolean executeForNull(Object context2)
            {
                getAttribute2().forEach(innerProcedureForNull, o, doubleAndContext);
                return false;
            }
        }, o, context1);
    }

    protected void forEach(final BigDecimalAndContext bigDecimalAndContext, final BigDecimalProcedure innerProcedure,
                           final Object o, final InnerBigDecimalProcedureForNull innerProcedureForNull, Object context1)
    {
        this.getAttribute1().forEach(new BigDecimalProcedure()
        {
            public boolean execute(final BigDecimal firstDouble, Object context2)
            {
                bigDecimalAndContext.setValue(firstDouble);
                bigDecimalAndContext.setContext(context2);
                getAttribute2().forEach(innerProcedure, o, bigDecimalAndContext);
                return false;
            }

            public boolean executeForNull(Object context2)
            {
                getAttribute2().forEach(innerProcedureForNull, o, bigDecimalAndContext);
                return false;
            }
        }, o, context1);
    }

    public void forEach(final DoubleProcedure proc, final Object o, final Object context1)
    {
        final DoubleAndContext doubleAndContext = new DoubleAndContext(0, context1);
        final DoubleProcedure innerProcedure = this.createInnerDoubleProcedure(proc);
        final InnerDoubleProcedureForNull innerProcedureForNull = new InnerDoubleProcedureForNull(proc);
        forEach(doubleAndContext, innerProcedure, o, innerProcedureForNull, context1);
    }

    public void forEach(final IntegerProcedure proc, final Object o, final Object context1)
    {
        final IntegerAndContext intAndContext = new IntegerAndContext(0, context1);
        final IntegerProcedure innerProcedure = this.createInnerIntegerProcedure(proc);
        final InnerIntProcedureForNull innerProcedureForNull = new InnerIntProcedureForNull(proc);
        forEach(intAndContext, innerProcedure, o, innerProcedureForNull, context1);
    }

    public void forEach(final LongProcedure proc, final Object o, final Object context1)
    {
        final LongAndContext longAndContext = new LongAndContext(0, context1);
        final LongProcedure innerProcedure = this.createInnerLongProcedure(proc);
        final InnerLongProcedureForNull innerProcedureForNull = new InnerLongProcedureForNull(proc);
        forEach(longAndContext, innerProcedure, o, innerProcedureForNull, context1);
    }

    public void forEach(final FloatProcedure proc, final Object o, final Object context1)
    {
        final FloatAndContext floatAndContext = new FloatAndContext(0, context1);
        final FloatProcedure innerProcedure = this.createInnerFloatProcedure(proc);
        final InnerFloatProcedureForNull innerProcedureForNull = new InnerFloatProcedureForNull(proc);
        forEach(floatAndContext, innerProcedure, o, innerProcedureForNull, context1);
    }

    public void forEach(final BigDecimalProcedure proc, final Object o, final Object context1)
    {
        final BigDecimalAndContext bigDecimalAndContext = new BigDecimalAndContext(BigDecimal.ZERO, context1);
        final BigDecimalProcedure innerProcedure = this.createInnerBigDecimalProcedure(proc);
        final InnerBigDecimalProcedureForNull innerProcedureForNull = new InnerBigDecimalProcedureForNull(proc);
        forEach(bigDecimalAndContext, innerProcedure, o, innerProcedureForNull, context1);
    }

    @Override
    public Operation optimizedIntegerEq(int value, CalculatedIntegerAttribute intAttribute)
    {
        return intAttribute.defaultEq(value);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractArithmeticAttributeCalculator that = (AbstractArithmeticAttributeCalculator) o;

        if (!attribute1.equals(that.attribute1)) return false;
        if (!attribute2.equals(that.attribute2)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return HashUtil.combineHashes(this.getClass().getName().hashCode(), HashUtil.combineHashes(attribute1.hashCode(), attribute2.hashCode()));
    }

    protected abstract DoubleProcedure createInnerDoubleProcedure(DoubleProcedure proc);
    protected abstract IntegerProcedure createInnerIntegerProcedure(IntegerProcedure proc);
    protected abstract LongProcedure createInnerLongProcedure(LongProcedure proc);
    protected abstract FloatProcedure createInnerFloatProcedure(FloatProcedure proc);
    protected abstract BigDecimalProcedure createInnerBigDecimalProcedure(BigDecimalProcedure proc);

    protected static class ContextHolder
    {
        private Object context;

        public ContextHolder(Object context)
        {
            this.context = context;
        }
        public Object getContext()
        {
            return context;
        }

        public void setContext(Object context)
        {
            this.context = context;
        }
    }

    protected static class DoubleAndContext extends ContextHolder
    {
        private double value;


        public DoubleAndContext(double value, Object context)
        {
            super(context);
            this.value = value;
        }

        public double getValue()
        {
            return value;
        }

        public void setValue(double value)
        {
            this.value = value;
        }
    }

    protected static class FloatAndContext extends ContextHolder
    {
        private float value;


        public FloatAndContext(float value, Object context)
        {
            super(context);
            this.value = value;
        }

        public float getValue()
        {
            return value;
        }

        public void setValue(float value)
        {
            this.value = value;
        }
    }

    protected static class BigDecimalAndContext extends ContextHolder
    {
        private BigDecimal value;
        public BigDecimalAndContext(BigDecimal value, Object context)
        {
            super(context);
            this.value = value;
        }

        public BigDecimal getValue()
        {
            return value;
        }

        public void setValue(BigDecimal value)
        {
            this.value = value;
        }
    }


    protected static class IntegerAndContext extends ContextHolder
    {
        private int value;

        public IntegerAndContext(int value, Object context)
        {
            super(context);
            this.value = value;
        }

        public int getValue()
        {
            return value;
        }

        public void setValue(int value)
        {
            this.value = value;
        }
    }

    protected static class LongAndContext extends ContextHolder
    {
        private long value;

        public LongAndContext(long value, Object context)
        {
            super(context);
            this.value = value;
        }

        public long getValue()
        {
            return value;
        }

        public void setValue(long value)
        {
            this.value = value;
        }
    }


    protected static class InnerProcedureForNull
    {
        private NullHandlingProcedure proc;

        protected NullHandlingProcedure getProcedure()
        {
            return this.proc;
        }

        public InnerProcedureForNull(NullHandlingProcedure proc)
        {
            this.proc = proc;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((ContextHolder) context).getContext());
            return false;
        }
    }

    protected static class InnerDoubleProcedureForNull extends InnerProcedureForNull implements DoubleProcedure
    {
        public InnerDoubleProcedureForNull(DoubleProcedure doubleProcedure)
        {
            super(doubleProcedure);
        }

        public boolean execute(double object, Object context)
        {
            getProcedure().executeForNull(((ContextHolder) context).getContext());
            return false;
        }
    }

    protected static class InnerFloatProcedureForNull extends InnerProcedureForNull implements FloatProcedure
    {
        public InnerFloatProcedureForNull(FloatProcedure doubleProcedure)
        {
            super(doubleProcedure);
        }

        public boolean execute(float object, Object context)
        {
            getProcedure().executeForNull(((ContextHolder) context).getContext());
            return false;
        }
    }

    protected static class InnerIntProcedureForNull extends InnerProcedureForNull implements IntegerProcedure
    {
        public InnerIntProcedureForNull(IntegerProcedure proc)
        {
            super(proc);
        }

        public boolean execute(int object, Object context)
        {
            getProcedure().executeForNull(((IntegerAndContext) context).getContext());
            return false;
        }
    }

    protected static class InnerLongProcedureForNull extends InnerProcedureForNull implements LongProcedure
    {
        public InnerLongProcedureForNull(LongProcedure proc)
        {
            super(proc);
        }

        public boolean execute(long object, Object context)
        {
            getProcedure().executeForNull(((IntegerAndContext) context).getContext());
            return false;
        }
    }

    protected static class InnerBigDecimalProcedureForNull extends InnerProcedureForNull implements BigDecimalProcedure
    {
        public InnerBigDecimalProcedureForNull(BigDecimalProcedure proc)
        {
            super(proc);
        }

        public boolean execute(BigDecimal object, Object context)
        {
            getProcedure().executeForNull(((BigDecimalAndContext) context).getContext());
            return false;
        }
    }
}
