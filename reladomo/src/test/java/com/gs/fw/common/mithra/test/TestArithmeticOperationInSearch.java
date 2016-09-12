
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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

public class TestArithmeticOperationInSearch extends MithraTestAbstract
{

    public TestArithmeticOperationInSearch()
    {
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Book.class,
            ParaDesk.class
        };
    }

    public void testArithmeticOperationInSearchForIntegerAttribute()
    {
        Operation op;
        Book book;
        op = BookFinder.inventoryLevel().absoluteValue().plus(BookFinder.numberOfPages()).greaterThanEquals(540);
        assertEqualsAndHashCode(BookFinder.inventoryLevel().absoluteValue().plus(BookFinder.numberOfPages()).greaterThanEquals(540), op);
        book = BookFinder.findOne(op);
        assertNotNull(book);
        
        op = BookFinder.manufacturerId().minus(BookFinder.inventoryLevel()).absoluteValue().eq(195);
        assertEqualsAndHashCode(BookFinder.manufacturerId().minus(BookFinder.inventoryLevel()).absoluteValue().eq(195), op);
        book = BookFinder.findOne(op);
        assertNotNull(book);

        op = BookFinder.manufacturerId().absoluteValue().times(BookFinder.inventoryLevel()).eq(1000);
        assertEqualsAndHashCode(BookFinder.manufacturerId().absoluteValue().times(BookFinder.inventoryLevel()).eq(1000), op);
        book = BookFinder.findOne(op);
        assertNotNull(book);

        op = BookFinder.numberOfPages().absoluteValue().dividedBy(BookFinder.manufacturerId()).eq(60);
        assertEqualsAndHashCode(BookFinder.numberOfPages().absoluteValue().dividedBy(BookFinder.manufacturerId()).eq(60), op);
        book = BookFinder.findOne(op);
        assertNotNull(book);
    }
    
    public void testArithmeticOperationInSearchForDoubleAttribute()
    throws Exception
    {
        Operation op;
        ParaDesk pd;
        op = ParaDeskFinder.sizeDouble().absoluteValue().plus(ParaDeskFinder.maxFloat().absoluteValue()).eq(1581.25);
        assertEqualsAndHashCode(ParaDeskFinder.sizeDouble().absoluteValue().plus(ParaDeskFinder.maxFloat().absoluteValue()).eq(1581.25), op);
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
        
        op = ParaDeskFinder.sizeDouble().absoluteValue().minus(ParaDeskFinder.maxFloat().absoluteValue()).eq(272.75);
        assertEqualsAndHashCode(ParaDeskFinder.sizeDouble().absoluteValue().minus(ParaDeskFinder.maxFloat().absoluteValue()).eq(272.75), op);
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
        
        op = ParaDeskFinder.sizeDouble().absoluteValue().times(ParaDeskFinder.maxFloat().absoluteValue()).eq(606489.75);
        assertEqualsAndHashCode(ParaDeskFinder.sizeDouble().absoluteValue().times(ParaDeskFinder.maxFloat().absoluteValue()).eq(606489.75), op);
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
        
        op = ParaDeskFinder.sizeDouble().absoluteValue().dividedBy(ParaDeskFinder.maxFloat()).greaterThan(1.41);
        op = op.and(ParaDeskFinder.sizeDouble().absoluteValue().dividedBy(ParaDeskFinder.maxFloat()).lessThan(1.43));
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
    }
    
    public void testArithmeticOperationInSearchForFloatAttribute()
    {
        Operation op;
        ParaDesk pd;
        op = ParaDeskFinder.maxFloat().absoluteValue().plus(ParaDeskFinder.maxFloat()).eq((float)654.25 + (float)654.25);
        assertEqualsAndHashCode(ParaDeskFinder.maxFloat().absoluteValue().plus(ParaDeskFinder.maxFloat()).eq((float)654.25 + (float)654.25), op);
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
        
        op = ParaDeskFinder.tagInt().absoluteValue().minus(ParaDeskFinder.maxFloat()).eq(827-(float)654.25);
        assertEqualsAndHashCode(ParaDeskFinder.tagInt().absoluteValue().minus(ParaDeskFinder.maxFloat()).eq(827-(float)654.25), op);
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
        
        op = ParaDeskFinder.maxFloat().absoluteValue().times(ParaDeskFinder.maxFloat()).eq((float)654.25 * (float)654.25);
        assertEqualsAndHashCode(ParaDeskFinder.maxFloat().absoluteValue().times(ParaDeskFinder.maxFloat()).eq((float)654.25 * (float)654.25), op);
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);

        op = ParaDeskFinder.tagInt().absoluteValue().dividedBy(ParaDeskFinder.maxFloat()).greaterThan((float)1.25);
        op = op.and(ParaDeskFinder.tagInt().absoluteValue().dividedBy(ParaDeskFinder.maxFloat()).lessThan((float)1.27));
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
    }

    public void testArithmeticOperationInSearchForLongAttribute()
    {
        Operation op;
        ParaDesk pd;
        op = ParaDeskFinder.connectionLong().plus(ParaDeskFinder.tagInt().absoluteValue()).eq(1000827);
        assertEqualsAndHashCode(ParaDeskFinder.connectionLong().plus(ParaDeskFinder.tagInt().absoluteValue()).eq(1000827), op);
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
        
        op = ParaDeskFinder.connectionLong().minus(ParaDeskFinder.tagInt().absoluteValue()).eq(999173);
        assertEqualsAndHashCode(ParaDeskFinder.connectionLong().minus(ParaDeskFinder.tagInt().absoluteValue()).eq(999173), op);
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
        
        op = ParaDeskFinder.connectionLong().times(ParaDeskFinder.tagInt().absoluteValue()).eq(827000000);
        assertEqualsAndHashCode(ParaDeskFinder.connectionLong().times(ParaDeskFinder.tagInt().absoluteValue()).eq(827000000), op);
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
        
        op = ParaDeskFinder.connectionLong().dividedBy(ParaDeskFinder.tagInt().absoluteValue()).eq(1209);
        assertEqualsAndHashCode(ParaDeskFinder.connectionLong().dividedBy(ParaDeskFinder.tagInt().absoluteValue()).eq(1209), op);
        pd = ParaDeskFinder.findOne(op);
        assertNotNull(pd);
    }
}
