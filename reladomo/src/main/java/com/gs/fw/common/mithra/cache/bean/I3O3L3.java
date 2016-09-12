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

package com.gs.fw.common.mithra.cache.bean;


import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.util.Time;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

public class I3O3L3 implements MutableBean
{
    public static final MutableBeanPool<I3O3L3> POOL = new MutableBeanPool(new MutableBean.Factory()
    {
        @Override
        public MutableBean construct(int cachePosition)
        {
            return new I3O3L3(cachePosition);
        }
    });

    private int cachePosition;

    private int i1, i2, i3;
    private Object o1, o2, o3;
    private long l1, l2, l3;

    public I3O3L3(int cachePosition)
    {
        this.cachePosition = cachePosition;
    }

    @Override
    public int getCachePosition()
    {
        return this.cachePosition;
    }

    public void release()
    {
        POOL.release(this);
    }

    public static final BooleanExtractor BOOLEAN_EXTRACTOR_1 = new BeanBooleanExtractor()
    {
        @Override
        public boolean booleanValueOf(Object o)
        {
            return ((I3O3L3)o).i1 == 1;
        }
    };

    public static final BooleanExtractor BOOLEAN_EXTRACTOR_2 = new BeanBooleanExtractor()
    {
        @Override
        public boolean booleanValueOf(Object o)
        {
            return ((I3O3L3)o).i2 == 1;
        }
    };

    public static final BooleanExtractor BOOLEAN_EXTRACTOR_3 = new BeanBooleanExtractor()
    {
        @Override
        public boolean booleanValueOf(Object o)
        {
            return ((I3O3L3)o).i3 == 1;
        }
    };

    public static final CharExtractor CHAR_EXTRACTOR_1 = new BeanCharExtractor()
    {
        @Override
        public char charValueOf(Object o)
        {
            return (char) ((I3O3L3)o).i1;
        }
    };

    public static final CharExtractor CHAR_EXTRACTOR_2 = new BeanCharExtractor()
    {
        @Override
        public char charValueOf(Object o)
        {
            return (char) ((I3O3L3)o).i2;
        }
    };

    public static final CharExtractor CHAR_EXTRACTOR_3 = new BeanCharExtractor()
    {
        @Override
        public char charValueOf(Object o)
        {
            return (char) ((I3O3L3)o).i3;
        }
    };

    public static final ByteExtractor BYTE_EXTRACTOR_1 = new BeanByteExtractor()
    {
        @Override
        public byte byteValueOf(Object o)
        {
            return (byte) ((I3O3L3)o).i1;
        }
    };

    public static final ByteExtractor BYTE_EXTRACTOR_2 = new BeanByteExtractor()
    {
        @Override
        public byte byteValueOf(Object o)
        {
            return (byte) ((I3O3L3)o).i2;
        }
    };

    public static final ByteExtractor BYTE_EXTRACTOR_3 = new BeanByteExtractor()
    {
        @Override
        public byte byteValueOf(Object o)
        {
            return (byte) ((I3O3L3)o).i3;
        }
    };

    public static final ShortExtractor SHORT_EXTRACTOR_1 = new BeanShortExtractor()
    {
        @Override
        public short shortValueOf(Object o)
        {
            return (short) ((I3O3L3)o).i1;
        }
    };

    public static final ShortExtractor SHORT_EXTRACTOR_2 = new BeanShortExtractor()
    {
        @Override
        public short shortValueOf(Object o)
        {
            return (short) ((I3O3L3)o).i2;
        }
    };

    public static final ShortExtractor SHORT_EXTRACTOR_3 = new BeanShortExtractor()
    {
        @Override
        public short shortValueOf(Object o)
        {
            return (short) ((I3O3L3)o).i3;
        }
    };

    public static final IntExtractor INT_EXTRACTOR_1 = new BeanIntExtractor()
    {
        @Override
        public int intValueOf(Object o)
        {
            return ((I3O3L3)o).i1;
        }
    };

    public static final IntExtractor INT_EXTRACTOR_2 = new BeanIntExtractor()
    {
        @Override
        public int intValueOf(Object o)
        {
            return ((I3O3L3)o).i2;
        }
    };

    public static final IntExtractor INT_EXTRACTOR_3 = new BeanIntExtractor()
    {
        @Override
        public int intValueOf(Object o)
        {
            return ((I3O3L3)o).i3;
        }
    };

    public static final LongExtractor LONG_EXTRACTOR_1 = new BeanLongExtractor()
    {
        @Override
        public long longValueOf(Object o)
        {
            return ((I3O3L3)o).l1;
        }
    };

    public static final LongExtractor LONG_EXTRACTOR_2 = new BeanLongExtractor()
    {
        @Override
        public long longValueOf(Object o)
        {
            return ((I3O3L3)o).l2;
        }
    };

    public static final LongExtractor LONG_EXTRACTOR_3 = new BeanLongExtractor()
    {
        @Override
        public long longValueOf(Object o)
        {
            return ((I3O3L3)o).l3;
        }
    };

    public static final FloatExtractor FLOAT_EXTRACTOR_1 = new BeanFloatExtractor()
    {
        @Override
        public float floatValueOf(Object o)
        {
            return Float.intBitsToFloat(((I3O3L3)o).i1);
        }
    };

    public static final FloatExtractor FLOAT_EXTRACTOR_2 = new BeanFloatExtractor()
    {
        @Override
        public float floatValueOf(Object o)
        {
            return Float.intBitsToFloat(((I3O3L3)o).i2);
        }
    };

    public static final FloatExtractor FLOAT_EXTRACTOR_3 = new BeanFloatExtractor()
    {
        @Override
        public float floatValueOf(Object o)
        {
            return Float.intBitsToFloat(((I3O3L3)o).i3);
        }
    };

    public static final DoubleExtractor DOUBLE_EXTRACTOR_1 = new BeanDoubleExtractor()
    {
        @Override
        public double doubleValueOf(Object o)
        {
            return Double.longBitsToDouble(((I3O3L3) o).l1);
        }
    };

    public static final DoubleExtractor DOUBLE_EXTRACTOR_2 = new BeanDoubleExtractor()
    {
        @Override
        public double doubleValueOf(Object o)
        {
            return Double.longBitsToDouble(((I3O3L3) o).l2);
        }
    };

    public static final DoubleExtractor DOUBLE_EXTRACTOR_3 = new BeanDoubleExtractor()
    {
        @Override
        public double doubleValueOf(Object o)
        {
            return Double.longBitsToDouble(((I3O3L3)o).l3);
        }
    };

    public static final BigDecimalExtractor BIG_DECIMAL_EXTRACTOR_1 = new BeanBigDecimalExtractor()
    {
        @Override
        public BigDecimal bigDecimalValueOf(Object o)
        {
            return (BigDecimal) ((I3O3L3)o).o1;
        }
    };

    public static final BigDecimalExtractor BIG_DECIMAL_EXTRACTOR_2 = new BeanBigDecimalExtractor()
    {
        @Override
        public BigDecimal bigDecimalValueOf(Object o)
        {
            return (BigDecimal) ((I3O3L3)o).o2;
        }
    };

    public static final BigDecimalExtractor BIG_DECIMAL_EXTRACTOR_3 = new BeanBigDecimalExtractor()
    {
        @Override
        public BigDecimal bigDecimalValueOf(Object o)
        {
            return (BigDecimal) ((I3O3L3)o).o3;
        }
    };

    public static final ByteArrayExtractor BYTE_ARRAY_EXTRACTOR_1 = new BeanByteArrayExtractor()
    {
        @Override
        public byte[] byteArrayValueOf(Object o)
        {
            return (byte[]) ((I3O3L3)o).o1;
        }
    };

    public static final ByteArrayExtractor BYTE_ARRAY_EXTRACTOR_2 = new BeanByteArrayExtractor()
    {
        @Override
        public byte[] byteArrayValueOf(Object o)
        {
            return (byte[]) ((I3O3L3)o).o2;
        }
    };

    public static final ByteArrayExtractor BYTE_ARRAY_EXTRACTOR_3 = new BeanByteArrayExtractor()
    {
        @Override
        public byte[] byteArrayValueOf(Object o)
        {
            return (byte[]) ((I3O3L3)o).o3;
        }
    };

    public static final DateExtractor DATE_EXTRACTOR_1 = new BeanDateExtractor()
    {
        @Override
        public Date dateValueOf(Object o)
        {
            return (Date) ((I3O3L3)o).o1;
        }
    };

    public static final DateExtractor DATE_EXTRACTOR_2 = new BeanDateExtractor()
    {
        @Override
        public Date dateValueOf(Object o)
        {
            return (Date) ((I3O3L3)o).o2;
        }
    };

    public static final DateExtractor DATE_EXTRACTOR_3 = new BeanDateExtractor()
    {
        @Override
        public Date dateValueOf(Object o)
        {
            return (Date) ((I3O3L3)o).o3;
        }
    };

    public static final TimeExtractor TIME_EXTRACTOR_1 = new BeanTimeExtractor()
    {
        @Override
        public Time timeValueOf(Object o)
        {
            return (Time) ((I3O3L3)o).o1;
        }
    };

    public static final TimeExtractor TIME_EXTRACTOR_2 = new BeanTimeExtractor()
    {
        @Override
        public Time timeValueOf(Object o)
        {
            return (Time) ((I3O3L3)o).o2;
        }
    };

    public static final TimeExtractor TIME_EXTRACTOR_3 = new BeanTimeExtractor()
    {
        @Override
        public Time timeValueOf(Object o)
        {
            return (Time) ((I3O3L3)o).o3;
        }
    };

    public static final StringExtractor STRING_EXTRACTOR_1 = new BeanStringExtractor()
    {
        @Override
        public String stringValueOf(Object o)
        {
            return (String) ((I3O3L3)o).o1;
        }
    };

    public static final StringExtractor STRING_EXTRACTOR_2 = new BeanStringExtractor()
    {
        @Override
        public String stringValueOf(Object o)
        {
            return (String) ((I3O3L3)o).o2;
        }
    };

    public static final StringExtractor STRING_EXTRACTOR_3 = new BeanStringExtractor()
    {
        @Override
        public String stringValueOf(Object o)
        {
            return (String) ((I3O3L3)o).o3;
        }
    };

    public static final TimestampExtractor TIMESTAMP_EXTRACTOR_1 = new BeanTimestampExtractor()
    {
        @Override
        public Timestamp timestampValueOf(Object o)
        {
            return (Timestamp) ((I3O3L3)o).o1;
        }
    };

    public static final TimestampExtractor TIMESTAMP_EXTRACTOR_2 = new BeanTimestampExtractor()
    {
        @Override
        public Timestamp timestampValueOf(Object o)
        {
            return (Timestamp) ((I3O3L3)o).o2;
        }
    };

    public static final TimestampExtractor TIMESTAMP_EXTRACTOR_3 = new BeanTimestampExtractor()
    {
        @Override
        public Timestamp timestampValueOf(Object o)
        {
            return (Timestamp) ((I3O3L3)o).o3;
        }
    };

    public void setI1AsInteger(int i1)
    {
        this.i1 = i1;
    }

    public void setI2AsInteger(int i2)
    {
        this.i2 = i2;
    }

    public void setI3AsInteger(int i3)
    {
        this.i3 = i3;
    }

    public void setI1AsBoolean(boolean b)
    {
        this.i1 = b ? 1 : 0;
    }

    public void setI2AsBoolean(boolean b)
    {
        this.i2 = b ? 1 : 0;
    }

    public void setI3AsBoolean(boolean b)
    {
        this.i3 = b ? 1 : 0;
    }

    public void setI1AsFloat(float f)
    {
        this.i1 = Float.floatToIntBits(f);
    }

    public void setI2AsFloat(float f)
    {
        this.i2 = Float.floatToIntBits(f);
    }

    public void setI3AsFloat(float f)
    {
        this.i3 = Float.floatToIntBits(f);
    }

    public void setL1AsLong(long l1)
    {
        this.l1 = l1;
    }

    public void setL2AsLong(long l2)
    {
        this.l2 = l2;
    }

    public void setL3AsLong(long l3)
    {
        this.l3 = l3;
    }

    public void setL1AsDouble(double d)
    {
        this.l1 = Double.doubleToLongBits(d);
    }

    public void setL2AsDouble(double d)
    {
        this.l2 = Double.doubleToLongBits(d);
    }

    public void setL3AsDouble(double d)
    {
        this.l3 = Double.doubleToLongBits(d);
    }

    public void setO1(Object o1)
    {
        this.o1 = o1;
    }

    public void setO2(Object o2)
    {
        this.o2 = o2;
    }

    public void setO3(Object o3)
    {
        this.o3 = o3;
    }

    public boolean getI1AsBoolean()
    {
        return i1 == 1;
    }

    public boolean getI2AsBoolean()
    {
        return i2 == 1;
    }

    public boolean getI3AsBoolean()
    {
        return i3 == 1;
    }

    public byte getI1AsByte()
    {
        return (byte) i1;
    }

    public byte getI2AsByte()
    {
        return (byte) i2;
    }

    public byte getI3AsByte()
    {
        return (byte) i3;
    }

    public char getI1AsChar()
    {
        return (char) i1;
    }

    public char getI2AsChar()
    {
        return (char) i2;
    }

    public char getI3AsChar()
    {
        return (char) i3;
    }

    public short getI1AsShort()
    {
        return (short) i1;
    }

    public short getI2AsShort()
    {
        return (short) i2;
    }

    public short getI3AsShort()
    {
        return (short) i3;
    }

    public int getI1AsInteger()
    {
        return i1;
    }

    public int getI2AsInteger()
    {
        return i2;
    }

    public int getI3AsInteger()
    {
        return i3;
    }

    public float getI1AsFloat()
    {
        return Float.intBitsToFloat(i1);
    }

    public float getI2AsFloat()
    {
        return Float.intBitsToFloat(i2);
    }

    public float getI3AsFloat()
    {
        return Float.intBitsToFloat(i3);
    }

    public long getL1AsLong()
    {
        return l1;
    }

    public long getL2AsLong()
    {
        return l2;
    }

    public long getL3AsLong()
    {
        return l3;
    }

    public double getL1AsDouble()
    {
        return Double.longBitsToDouble(l1);
    }

    public double getL2AsDouble()
    {
        return Double.longBitsToDouble(l2);
    }

    public double getL3AsDouble()
    {
        return Double.longBitsToDouble(l3);
    }

    public BigDecimal getO1AsBigDecimal()
    {
        return (BigDecimal) o1;
    }

    public BigDecimal getO2AsBigDecimal()
    {
        return (BigDecimal) o2;
    }

    public BigDecimal getO3AsBigDecimal()
    {
        return (BigDecimal) o3;
    }

    public byte[] getO1AsByteArray()
    {
        return (byte[]) o1;
    }

    public byte[] getO2AsByteArray()
    {
        return (byte[]) o2;
    }

    public byte[] getO3AsByteArray()
    {
        return (byte[]) o3;
    }

    public Date getO1AsDate()
    {
        return (Date) o1;
    }

    public Date getO2AsDate()
    {
        return (Date) o2;
    }

    public Date getO3AsDate()
    {
        return (Date) o3;
    }

    public Time getO1AsTime()
    {
        return (Time) o1;
    }

    public Time getO2AsTime()
    {
        return (Time) o2;
    }

    public Time getO3AsTime()
    {
        return (Time) o3;
    }

    public String getO1AsString()
    {
        return (String) o1;
    }

    public String getO2AsString()
    {
        return (String) o2;
    }

    public String getO3AsString()
    {
        return (String) o3;
    }

    public Timestamp getO1AsTimestamp()
    {
        return (Timestamp) o1;
    }

    public Timestamp getO2AsTimestamp()
    {
        return (Timestamp) o2;
    }

    public Timestamp getO3AsTimestamp()
    {
        return (Timestamp) o3;
    }

}
