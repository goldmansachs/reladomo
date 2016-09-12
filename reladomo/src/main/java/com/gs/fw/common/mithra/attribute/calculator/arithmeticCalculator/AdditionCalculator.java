
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

import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.BigDecimalUtil;

import java.math.BigDecimal;

public class AdditionCalculator extends AbstractArithmeticAttributeCalculator
{
    private static final long serialVersionUID = -2337424118414682768L;

    public AdditionCalculator(NumericAttribute attribute1, NumericAttribute attribute2)
    {
        super(attribute1, attribute2);
    }

    public int getScale()
    {
        return BigDecimalUtil.calculateAdditionScale(getAttribute1().getScale(), getAttribute2().getScale());
    }

    public int getPrecision()
    {
        int scale1 = getAttribute1().getScale();
        int scale2 = getAttribute2().getScale();
        int precision1 = getAttribute1().getPrecision();
        int precision2 = getAttribute2().getPrecision();
       return BigDecimalUtil.calculateAdditionPrecision( precision1, scale1,  precision2,scale2);
    }

    @Override
    protected CharSequence getOperatorString()
    {
        return "+";
    }

    public BigDecimal bigDecimalValueOf(Object o)
    {
        return (new BigDecimal((String.valueOf(this.getAttribute1().valueOf(o)))).add(new BigDecimal(String.valueOf(this.getAttribute2().valueOf(o))))) ;
    }

    public double doubleValueOf(Object o)
    {
        return ((Number)this.getAttribute1().valueOf(o)).doubleValue() + ((Number)this.getAttribute2().valueOf(o)).doubleValue();
    }

    public float floatValueOf(Object o)
    {
        return ((Number)this.getAttribute1().valueOf(o)).floatValue() + ((Number)this.getAttribute2().valueOf(o)).floatValue();
    }

    public long longValueOf(Object o)
    {
        return ((Number)this.getAttribute1().valueOf(o)).longValue() + ((Number)this.getAttribute2().valueOf(o)).longValue();
    }

    public int intValueOf(Object o)
    {
        return ((Number)this.getAttribute1().valueOf(o)).intValue() + ((Number)this.getAttribute2().valueOf(o)).intValue();
    }

    protected IntegerProcedure createInnerIntegerProcedure(IntegerProcedure proc)
    {
        return new InnerIntegerProcedure(proc);
    }

    protected LongProcedure createInnerLongProcedure(LongProcedure proc)
    {
        return new InnerLongProcedure(proc);
    }

    protected FloatProcedure createInnerFloatProcedure(FloatProcedure proc)
    {
        return new InnerFloatProcedure(proc);
    }

    protected DoubleProcedure createInnerDoubleProcedure(DoubleProcedure proc)
    {
        return new InnerDoubleProcedure(proc);
    }

    protected BigDecimalProcedure createInnerBigDecimalProcedure(BigDecimalProcedure proc)
    {
        return new InnerBigDecimalProcedure(proc);
    }

    private static class InnerDoubleProcedure implements DoubleProcedure
    {
        private DoubleProcedure proc;

        public InnerDoubleProcedure(DoubleProcedure proc)
        {
            this.proc = proc;
        }

        public boolean execute(double object, Object context)
        {
            DoubleAndContext doubleAndContext = (DoubleAndContext) context;
            proc.execute(doubleAndContext.getValue() + object, doubleAndContext.getContext());
            return false;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((DoubleAndContext) context).getContext());
            return false;
        }
    }

    private static class InnerBigDecimalProcedure implements BigDecimalProcedure
    {
        private BigDecimalProcedure proc;

        public InnerBigDecimalProcedure(BigDecimalProcedure proc)
        {
            this.proc = proc;
        }

        public boolean execute(BigDecimal object, Object context)
        {
            BigDecimalAndContext bigDecimalAndContext = (BigDecimalAndContext) context;
            proc.execute(bigDecimalAndContext.getValue().add(object), bigDecimalAndContext.getContext());
            return false;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((BigDecimalAndContext) context).getContext());
            return false;
        }
    }

    private static class InnerFloatProcedure implements FloatProcedure
    {
        private FloatProcedure proc;

        public InnerFloatProcedure(FloatProcedure proc)
        {
            this.proc = proc;
        }

        public boolean execute(float object, Object context)
        {
            FloatAndContext floatAndContext = (FloatAndContext) context;
            proc.execute(floatAndContext.getValue() + object, floatAndContext.getContext());
            return false;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((FloatAndContext) context).getContext());
            return false;
        }
    }

    private static class InnerIntegerProcedure implements IntegerProcedure
    {
        private IntegerProcedure proc;

        public InnerIntegerProcedure(IntegerProcedure proc)
        {
            this.proc = proc;
        }

        public boolean execute(int object, Object context)
        {
            IntegerAndContext intAndContext = (IntegerAndContext) context;
            proc.execute(intAndContext.getValue() + object, intAndContext.getContext());
            return false;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((IntegerAndContext) context).getContext());
            return false;
        }
    }

    private static class InnerLongProcedure implements LongProcedure
    {
        private LongProcedure proc;

        public InnerLongProcedure(LongProcedure proc)
        {
            this.proc = proc;
        }

        public boolean execute(long object, Object context)
        {
            LongAndContext longAndContext = (LongAndContext) context;
            proc.execute(longAndContext.getValue() + object, longAndContext.getContext());
            return false;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((LongAndContext) context).getContext());
            return false;
        }
    }
}
