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

package com.gs.fw.common.mithra.test.util;


import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.test.glew.LewContractData;
import com.gs.fw.common.mithra.test.glew.LewContractDatabaseObject;
import com.gs.fw.common.mithra.test.glew.LewContractFinder;
import com.gs.fw.common.mithra.util.AbstractBooleanFilter;
import com.gs.fw.common.mithra.util.BooleanFilter;
import com.gs.fw.common.mithra.util.KeepOnlySpecifiedDatesFilter;
import com.gs.fw.common.mithra.util.OperationBasedFilter;
import junit.framework.TestCase;

import java.sql.Timestamp;

public class BooleanFilterTest extends TestCase
{
    public void testKeepOnlySpecifiedDatesFilter()
    {
        Timestamp DATE1 = Timestamp.valueOf("2001-08-01 23:59:00");
        Timestamp DATE2 = Timestamp.valueOf("2002-08-01 23:59:00");
        Timestamp DATE3 = Timestamp.valueOf("2003-08-01 23:59:00");
        Timestamp DATE4 = Timestamp.valueOf("2004-08-01 23:59:00");

        AsOfAttribute asOfAttribute = (AsOfAttribute) LewContractFinder.businessDate();
        BooleanFilter filter1 = new KeepOnlySpecifiedDatesFilter(asOfAttribute, FastList.newListWith(DATE1, DATE3));
        assertFalse(filter1.matches(newDataObject(DATE1)));
        assertTrue(filter1.matches(newDataObject(DATE2)));
        assertFalse(filter1.matches(newDataObject(DATE3)));
        assertTrue(filter1.matches(newDataObject(DATE4)));

        BooleanFilter filter2 = new KeepOnlySpecifiedDatesFilter(asOfAttribute, FastList.newListWith(DATE3, DATE4));
        assertTrue(filter2.matches(newDataObject(DATE1)));
        assertTrue(filter2.matches(newDataObject(DATE2)));
        assertFalse(filter2.matches(newDataObject(DATE3)));
        assertFalse(filter2.matches(newDataObject(DATE4)));

        final BooleanFilter andFilter = filter1.and(filter2);
        assertFalse(andFilter.matches(newDataObject(DATE1)));
        assertTrue(andFilter.matches(newDataObject(DATE2)));
        assertFalse(andFilter.matches(newDataObject(DATE3)));
        assertFalse(andFilter.matches(newDataObject(DATE4)));

        final BooleanFilter orFilter = filter1.or(filter2);
        assertTrue(orFilter.matches(newDataObject(DATE1)));
        assertTrue(orFilter.matches(newDataObject(DATE2)));
        assertFalse(orFilter.matches(newDataObject(DATE3)));
        assertTrue(orFilter.matches(newDataObject(DATE4)));
    }

    public void testOperationBasedFilter()
    {
        Timestamp DATE1 = Timestamp.valueOf("2001-08-01 23:59:00");
        Timestamp DATE2 = Timestamp.valueOf("2002-08-01 23:59:00");
        Timestamp DATE3 = Timestamp.valueOf("2003-08-01 23:59:00");
        Timestamp DATE4 = Timestamp.valueOf("2004-08-01 23:59:00");

        TimestampAttribute fromAttribute = LewContractFinder.businessDate().getFromAttribute();

        BooleanFilter filter1 = new OperationBasedFilter(fromAttribute.notIn(UnifiedSet.newSetWith(DATE1, DATE3)));
        assertFalse(filter1.matches(newDataObject(DATE1)));
        assertTrue(filter1.matches(newDataObject(DATE2)));
        assertFalse(filter1.matches(newDataObject(DATE3)));
        assertTrue(filter1.matches(newDataObject(DATE4)));

        final BooleanFilter filter2 = new OperationBasedFilter(fromAttribute.notIn(UnifiedSet.newSetWith(DATE3, DATE4)));
        assertTrue(filter2.matches(newDataObject(DATE1)));
        assertTrue(filter2.matches(newDataObject(DATE2)));
        assertFalse(filter2.matches(newDataObject(DATE3)));
        assertFalse(filter2.matches(newDataObject(DATE4)));

        final BooleanFilter andFilter = filter1.and(filter2);
        assertFalse(andFilter.matches(newDataObject(DATE1)));
        assertTrue(andFilter.matches(newDataObject(DATE2)));
        assertFalse(andFilter.matches(newDataObject(DATE3)));
        assertFalse(andFilter.matches(newDataObject(DATE4)));

        final BooleanFilter orFilter = filter1.or(filter2);
        assertTrue(orFilter.matches(newDataObject(DATE1)));
        assertTrue(orFilter.matches(newDataObject(DATE2)));
        assertFalse(orFilter.matches(newDataObject(DATE3)));
        assertTrue(orFilter.matches(newDataObject(DATE4)));
    }

    public void testFilterLogic()
    {
        BooleanFilter filter = new TestFilter(6);
        assertTrue(filter.matches(6));
        assertFalse(filter.matches(5));

        filter = filter.or(new TestFilter(7));
        assertFalse(filter.matches(5));
        assertTrue(filter.matches(6));
        assertTrue(filter.matches(7));
    }

    public void testNegate()
    {
        BooleanFilter filter = new TestFilter(6);
        assertTrue(filter.matches(6));
        assertFalse(filter.matches(5));

        filter = filter.negate();
        assertFalse(filter.matches(6));
        assertTrue(filter.matches(5));
    }

    private class TestFilter extends AbstractBooleanFilter
    {
        private int matchingValue;

        private TestFilter(int matchingValue)
        {
            this.matchingValue = matchingValue;
        }

        @Override
        public boolean matches(Object o)
        {
            return ((Integer) o).intValue() == matchingValue;
        }
    }

    private Object newDataObject(Timestamp date)
    {
        LewContractData obj = LewContractDatabaseObject.allocateOnHeapData();

        obj.setBusinessDateFrom(date);
        obj.setBusinessDateTo(new Timestamp(date.getTime() + 24 * 60 * 60 * 1000));  // works in august

        return obj;
    }
}
