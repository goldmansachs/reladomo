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

package com.gs.fw.common.mithra.test.domain.evo;

import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.util.Time;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

public class EvoTypesTime extends EvoTypesTimeAbstract
{
    public EvoTypesTime()
    {
    }

    protected EvoTypesTime(MithraObject ref, EvoTypesTimeExtractors attribute)
    {
        super(ref, attribute);
    }

    private EvoTypesTime(Builder builder)
    {
        this.setCharAttribute(builder.type);
        this.setBooleanAttribute(builder.boolean0);
        this.setByteAttribute(builder.byte0);
        this.setDateAttribute(builder.date0);
        this.setTimeAttribute(builder.time0);
        this.setDoubleAttribute(builder.double0);
        this.setFloatAttribute(builder.float0);
        this.setIntAttribute(builder.int0);
        this.setLongAttribute(builder.long0);
        this.setShortAttribute(builder.short0);
        this.setStringAttribute(builder.string0);
        this.setTimestampAttribute(builder.timestamp0);
        this.setBigDecimalAttribute(builder.bigDecimal0);
    }

    public static class Builder
    {
        private char type;
        private boolean boolean0;
        private byte byte0;
        private Date date0;
        private double double0;
        private float float0;
        private int int0;
        private long long0;
        private short short0;
        private String string0;
        private Timestamp timestamp0;
        private BigDecimal bigDecimal0;
        private Time time0;

        public Builder(char type)
        {
            this.type = type;
        }

        public EvoTypesTime build()
        {
            return new EvoTypesTime(this);
        }

        public Builder withBoolean(boolean boolean0)
        {
            this.boolean0 = boolean0;
            return this;
        }

        public Builder withByte(byte byte0)
        {
            this.byte0 = byte0;
            return this;
        }

        public Builder withDate(Date date0)
        {
            this.date0 = date0;
            return this;
        }

        public Builder withTime(Time time0)
        {
            this.time0 = time0;
            return this;
        }

        public Builder withDouble(double double0)
        {
            this.double0 = double0;
            return this;
        }

        public Builder withFloat(float float0)
        {
            this.float0 = float0;
            return this;
        }

        public Builder withInt(int int0)
        {
            this.int0 = int0;
            return this;
        }

        public Builder withLong(long long0)
        {
            this.long0 = long0;
            return this;
        }

        public Builder withShort(short short0)
        {
            this.short0 = short0;
            return this;
        }

        public Builder withString(String string0)
        {
            this.string0 = string0;
            return this;
        }

        public Builder withTimestamp(Timestamp timestamp0)
        {
            this.timestamp0 = timestamp0;
            return this;
        }

        public Builder withBigDecimal(BigDecimal bigDecimal0)
        {
            this.bigDecimal0 = bigDecimal0;
            return this;
        }
    }
}
