/*
 Copyright 2019 Goldman Sachs.
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

package com.gs.reladomo.graphql;

import graphql.scalars.datetime.TimeScalar;
import graphql.schema.Coercing;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;


public class TimestampScalarTest {
    @Test
    public void parseTimestamp() {
        Coercing<Timestamp, String> coercing = new TimestampScalar().getCoercing();

        Timestamp t = coercing.parseValue("2019-03-23T14:09:39-00:00");
        Assert.assertEquals("2019-03-23 10:09:39.0", t.toString());
        Assert.assertEquals("2019-03-23T14:09:39Z", coercing.serialize(t));
    }

    @Test
    public void parseDate() {
        Coercing<Timestamp, String> coercing = new TimestampScalar().getCoercing();

        Timestamp t = coercing.parseValue("2019-03-23");
        Assert.assertEquals("2019-03-23 23:59:00.0", t.toString());
    }

    @Test
    public void parseInfinity() {
        Coercing<Timestamp, String> coercing = new TimestampScalar().getCoercing();

        Timestamp t = coercing.parseValue("INFINITY");
        Assert.assertEquals(TimestampScalar.INFINITY_TIMESTAMP_MARKER, t);
    }
}
