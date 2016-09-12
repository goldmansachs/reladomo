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
import com.gs.fw.common.mithra.test.domain.BookFinder;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;


public class TestArithmeticOperationsToString extends MithraTestAbstract
{
    public void testPlusOperation()
    {
        Operation intPlus = BookFinder.inventoryLevel().absoluteValue().plus(BookFinder.numberOfPages()).greaterThanEquals(540);
        assertEquals("( abs( Book.inventoryLevel ) + Book.numberOfPages ) >= 540", intPlus.toString());

        Operation doublePlus = ParaDeskFinder.sizeDouble().absoluteValue().plus(ParaDeskFinder.maxFloat().absoluteValue()).eq(1581.25);
        assertEquals("( abs( ParaDesk.sizeDouble ) + abs( ParaDesk.maxFloat ) ) = 1581.25", doublePlus.toString());

        Operation floatPlus = ParaDeskFinder.maxFloat().absoluteValue().plus(ParaDeskFinder.maxFloat()).eq((float)654.25 + (float)654.25);
        assertEquals("( abs( ParaDesk.maxFloat ) + ParaDesk.maxFloat ) = 1308.5", floatPlus.toString());

        Operation longPlus = ParaDeskFinder.connectionLong().plus(ParaDeskFinder.tagInt().absoluteValue()).eq(1000827);
        assertEquals("( ParaDesk.connectionLong + abs( ParaDesk.tagInt ) ) = 1000827", longPlus.toString());
    }

    public void testMinusOperation()
    {
        Operation intMinus = BookFinder.manufacturerId().minus(BookFinder.inventoryLevel()).absoluteValue().eq(195);
        assertEquals("abs( ( Book.manufacturerId - Book.inventoryLevel ) ) = 195", intMinus.toString());

        Operation doubleMinus = ParaDeskFinder.sizeDouble().absoluteValue().minus(ParaDeskFinder.maxFloat().absoluteValue()).eq(272.75);
        assertEquals("( abs( ParaDesk.sizeDouble ) - abs( ParaDesk.maxFloat ) ) = 272.75", doubleMinus.toString());

        Operation floatMinus = ParaDeskFinder.tagInt().absoluteValue().minus(ParaDeskFinder.maxFloat()).eq(827-(float)654.25);
        assertEquals("( abs( ParaDesk.tagInt ) - ParaDesk.maxFloat ) = 172.75", floatMinus.toString());

        Operation longMinus = ParaDeskFinder.connectionLong().minus(ParaDeskFinder.tagInt().absoluteValue()).eq(999173);
        assertEquals("( ParaDesk.connectionLong - abs( ParaDesk.tagInt ) ) = 999173", longMinus.toString());
    }

    public void testTimesOperation()
    {
        Operation intTimes = BookFinder.manufacturerId().absoluteValue().times(BookFinder.inventoryLevel()).eq(1000);
        assertEquals("( abs( Book.manufacturerId ) * Book.inventoryLevel ) = 1000", intTimes.toString());

        Operation doubleTimes = ParaDeskFinder.sizeDouble().absoluteValue().times(ParaDeskFinder.maxFloat().absoluteValue()).eq(606489.75);
        assertEquals("( abs( ParaDesk.sizeDouble ) * abs( ParaDesk.maxFloat ) ) = 606489.75", doubleTimes.toString());

        Operation floatTimes = ParaDeskFinder.maxFloat().absoluteValue().times(ParaDeskFinder.maxFloat()).eq((float)654.25 * (float)654.25);
        assertEquals("( abs( ParaDesk.maxFloat ) * ParaDesk.maxFloat ) = 428043.06", floatTimes.toString());

        Operation longTimes = ParaDeskFinder.connectionLong().times(ParaDeskFinder.tagInt().absoluteValue()).eq(827000000);
        assertEquals("( ParaDesk.connectionLong * abs( ParaDesk.tagInt ) ) = 827000000", longTimes.toString());
    }

    public void testDividedByOperation()
    {
        Operation intDividedBy = BookFinder.numberOfPages().absoluteValue().dividedBy(BookFinder.manufacturerId()).eq(60);
        assertEquals("( abs( Book.numberOfPages ) / Book.manufacturerId ) = 60", intDividedBy.toString());

        Operation doubleDividedBy = ParaDeskFinder.sizeDouble().absoluteValue().dividedBy(ParaDeskFinder.maxFloat()).greaterThan(1.41);
        doubleDividedBy = doubleDividedBy.and(ParaDeskFinder.sizeDouble().absoluteValue().dividedBy(ParaDeskFinder.maxFloat()).lessThan(1.43));
        assertEquals("( abs( ParaDesk.sizeDouble ) / ParaDesk.maxFloat ) > 1.41 & ( abs( ParaDesk.sizeDouble ) / ParaDesk.maxFloat ) < 1.43", doubleDividedBy.toString());

        Operation floatDividedBy = ParaDeskFinder.tagInt().absoluteValue().dividedBy(ParaDeskFinder.maxFloat()).greaterThan((float)1.25);
        floatDividedBy = floatDividedBy.and(ParaDeskFinder.tagInt().absoluteValue().dividedBy(ParaDeskFinder.maxFloat()).lessThan((float)1.27));
        assertEquals("( abs( ParaDesk.tagInt ) / ParaDesk.maxFloat ) > 1.25 & ( abs( ParaDesk.tagInt ) / ParaDesk.maxFloat ) < 1.27", floatDividedBy.toString());

        Operation longDividedBy = ParaDeskFinder.connectionLong().dividedBy(ParaDeskFinder.tagInt().absoluteValue()).eq(1209);
        assertEquals("( ParaDesk.connectionLong / abs( ParaDesk.tagInt ) ) = 1209", longDividedBy.toString());
    }
}