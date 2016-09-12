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

package com.gs.fw.common.mithra.test.domain.pure;

import java.sql.Timestamp;
import java.util.Date;

public abstract class PureType2DatedTxnTypes extends PureType2DatedTxnTypesAbstract
{
    public static final char TYPE_A = 'A';
    public static final char TYPE_B = 'B';

    private static PureType2DatedTxnTypes createFromBuilder(Builder builder)
    {
        PureType2DatedTxnTypes txnTypes;
        if (builder.type == 'A')
        {
            txnTypes = new PureType2DatedTxnTypesA(builder.businessDate, builder.processingDate);
        }
        else if (builder.type == 'B')
        {
            txnTypes = new PureType2DatedTxnTypesB(builder.businessDate, builder.processingDate);
        }
        else
        {
            throw new IllegalStateException("type must be 'A' or 'B'");
        }
        txnTypes.setPkCharAttribute(builder.type);
        txnTypes.setPkBooleanAttribute(builder.boolean0);
        txnTypes.setPkByteAttribute(builder.byte0);
        txnTypes.setPkDateAttribute(builder.date0);
        txnTypes.setPkDoubleAttribute(builder.double0);
        txnTypes.setPkFloatAttribute(builder.float0);
        txnTypes.setPkIntAttribute(builder.int0);
        txnTypes.setPkLongAttribute(builder.long0);
        txnTypes.setPkShortAttribute(builder.short0);
        txnTypes.setPkStringAttribute(builder.string0);
        txnTypes.setPkTimestampAttribute(builder.timestamp0);
        return txnTypes;
    }

    public static class Builder
    {
        private Timestamp businessDate;
        private Timestamp processingDate;
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

        public Builder(Timestamp businessDate, char type)
        {
            this.businessDate = businessDate;
            this.type = type;
        }

        public Builder(Timestamp businessDate, Timestamp processingDate, char type)
        {
            this.businessDate = businessDate;
            this.processingDate = processingDate;
            this.type = type;
        }

        public PureType2DatedTxnTypes build()
        {
            return PureType2DatedTxnTypes.createFromBuilder(this);
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
    }
}
