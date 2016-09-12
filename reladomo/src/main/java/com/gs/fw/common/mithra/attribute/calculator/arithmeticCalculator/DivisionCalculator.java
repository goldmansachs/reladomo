
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

public class DivisionCalculator extends AbstractArithmeticAttributeCalculator
{
    private static final long serialVersionUID = -2292100496122385282L;
    private int scale;

    public DivisionCalculator(NumericAttribute attribute1, NumericAttribute attribute2)
    {
        super(attribute1,  attribute2);
    }

    public DivisionCalculator(NumericAttribute attribute1, NumericAttribute attribute2, int scale)
    {
        super(attribute1,  attribute2);
        this.scale = scale;
    }

    @Override
    protected CharSequence getOperatorString()
    {
        return "/";
    }

    public double doubleValueOf(Object o)
    {
        return ((Number)this.getAttribute1().valueOf(o)).doubleValue() / ((Number)this.getAttribute2().valueOf(o)).doubleValue();
    }

    public BigDecimal bigDecimalValueOf(Object o)
    {
        BigDecimal bd1 = new BigDecimal((String.valueOf(this.getAttribute1().valueOf(o))));
        BigDecimal bd2 = new BigDecimal(String.valueOf(this.getAttribute2().valueOf(o)));
        return BigDecimalUtil.divide(this.getScale(), bd1, bd2);
    }

    public int getScale()
    {
        return this.scale;
    }

    public int getPrecision()
    {
        return Math.max(this.getAttribute1().getScale() + this.getAttribute2().getPrecision() + 1, 6) +
                this.getAttribute1().getPrecision() - this.getAttribute1().getScale() + this.getAttribute2().getPrecision();
    }

    public float floatValueOf(Object o)
    {
        return ((Number)this.getAttribute1().valueOf(o)).floatValue() / ((Number)this.getAttribute2().valueOf(o)).floatValue();
    }

    public long longValueOf(Object o)
    {
        return ((Number)this.getAttribute1().valueOf(o)).longValue() / ((Number)this.getAttribute2().valueOf(o)).longValue();
    }

    public int intValueOf(Object o)
    {
        return ((Number)this.getAttribute1().valueOf(o)).intValue() / ((Number)this.getAttribute2().valueOf(o)).intValue();
    }

    public IntegerProcedure createInnerIntegerProcedure(IntegerProcedure proc)
    {
        return new InnerIntegerProcedure(proc);
    }

    public LongProcedure createInnerLongProcedure(LongProcedure proc)
    {
        return new InnerLongProcedure(proc);
    }

    public FloatProcedure createInnerFloatProcedure(FloatProcedure proc)
    {
        return new InnerFloatProcedure(proc);
    }

    protected BigDecimalProcedure createInnerBigDecimalProcedure(BigDecimalProcedure proc)
    {
        return new InnerBigDecimalProcedure(proc);
    }

    public DoubleProcedure createInnerDoubleProcedure(DoubleProcedure proc)
    {
        return new InnerDoubleProcedure(proc);
    }

    private static class InnerIntegerProcedure implements IntegerProcedure
    {
        private IntegerProcedure proc;

        public InnerIntegerProcedure(IntegerProcedure doubleProcedure)
        {
            this.proc = doubleProcedure;
        }

        public boolean execute(int object, Object context)
        {
            IntegerAndContext intAndContext = (IntegerAndContext) context;
            proc.execute(intAndContext.getValue() / object, intAndContext.getContext());
            return false;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((ContextHolder) context).getContext());
            return false;
        }
    }

    private static class InnerDoubleProcedure implements DoubleProcedure
    {
        private DoubleProcedure proc;

        public InnerDoubleProcedure(DoubleProcedure doubleProcedure)
        {
            this.proc = doubleProcedure;
        }

        public boolean execute(double object, Object context)
        {
            DoubleAndContext doubleAndContext = (DoubleAndContext) context;
            proc.execute(doubleAndContext.getValue() / object, doubleAndContext.getContext());
            return false;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((ContextHolder) context).getContext());
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

        public boolean execute(BigDecimal divisor, Object context)
        {
            BigDecimalAndContext bigDecimalAndContext = (BigDecimalAndContext) context;
            BigDecimal dividend = bigDecimalAndContext.getValue();
            int quotientScale = BigDecimalUtil.calculateQuotientScale(dividend.precision(), dividend.scale(),divisor.precision(),  divisor.scale());
            proc.execute(BigDecimalUtil.divide(
                    quotientScale,
                    dividend, divisor),
                    bigDecimalAndContext.getContext());
            return false;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((BigDecimalAndContext) context).getContext());
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
            proc.execute(longAndContext.getValue() / object, longAndContext.getContext());
            return false;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((LongAndContext) context).getContext());
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
            proc.execute(floatAndContext.getValue() / object, floatAndContext.getContext());
            return false;
        }

        public boolean executeForNull(Object context)
        {
            proc.executeForNull(((FloatAndContext) context).getContext());
            return false;
        }
    }
}
