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

package com.gs.fw.common.mithra.test.attribute;

import com.gs.fw.common.mithra.test.domain.DatedAllTypesNullFinder;
import junit.framework.TestCase;

/**
 * Created by borisv on 7/10/2014.
 */
public class MappedAttributeTest extends TestCase
{
    public void testMappedIntegerIsNullable()
    {
        assertFalse((DatedAllTypesNullFinder.related().booleanValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().byteValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().shortValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().charValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().intValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().longValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().floatValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().doubleValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().dateValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().timeValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().timestampValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().stringValue().getMetaData().isNullable()));
        assertFalse((DatedAllTypesNullFinder.related().byteArrayValue().getMetaData().isNullable()));

        assertTrue((DatedAllTypesNullFinder.related().nullableByteValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableShortValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableCharValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableIntValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableLongValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableFloatValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableDoubleValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableDateValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableTimeValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableTimestampValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableStringValue().getMetaData().isNullable()));
        assertTrue((DatedAllTypesNullFinder.related().nullableByteArrayValue().getMetaData().isNullable()));
    }
}
