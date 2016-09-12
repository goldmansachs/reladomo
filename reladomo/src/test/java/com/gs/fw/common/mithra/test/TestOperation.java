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

import junit.framework.TestCase;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.OrderFinder;


/**
 * TestOperation
 */
public class TestOperation extends TestCase
{
    public void testOperationDoesNotContainMappedOperation()
    {
        Operation eqOperation = OrderFinder.description().eq("");
        Operation eqOperation2 = OrderFinder.orderId().eq(1);
        assertFalse(eqOperation.zContainsMappedOperation());
        
        Operation lessThanOp = OrderFinder.orderId().lessThan(2);
        assertFalse(lessThanOp.zContainsMappedOperation());

        assertFalse(eqOperation.and(lessThanOp).zContainsMappedOperation());
        assertFalse(eqOperation.and(eqOperation2).zContainsMappedOperation());
        
        assertFalse(eqOperation.or(lessThanOp).zContainsMappedOperation());
        assertFalse(eqOperation.or(eqOperation2).zContainsMappedOperation());


        testOperationContainsMappedOperation();
    }

    public void testOperationContainsMappedOperation()
    {
        Operation eqOperation = OrderFinder.description().eq("");
        
        Operation mappedEqOperation = OrderFinder.orderStatus().status().eq(1);
        Operation mappedEqOperation2 = OrderFinder.orderStatus().lastUser().eq("");
        assertTrue(mappedEqOperation.zContainsMappedOperation());

        assertTrue(mappedEqOperation.and(mappedEqOperation2).zContainsMappedOperation());
        assertTrue(mappedEqOperation.and(eqOperation).zContainsMappedOperation());

        assertTrue(mappedEqOperation.or(mappedEqOperation2).zContainsMappedOperation());
        assertTrue(mappedEqOperation.or(eqOperation).zContainsMappedOperation());
    }
}
