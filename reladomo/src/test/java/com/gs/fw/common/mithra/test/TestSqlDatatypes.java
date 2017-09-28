
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

import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantity;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

public class TestSqlDatatypes
extends MithraTestAbstract
{
    private static final Timestamp testTimestamp;
    private static final Date testDate;

    static
    {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 1981);
        cal.set(Calendar.MONTH, 5);
        cal.set(Calendar.DAY_OF_MONTH, 8);
        cal.set(Calendar.AM_PM, Calendar.AM);

        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        testDate = cal.getTime();

        cal.set(Calendar.HOUR_OF_DAY, 2);
        cal.set(Calendar.MINUTE, 1);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        testTimestamp = new Timestamp(cal.getTime().getTime());
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            ParaDesk.class,
            User.class,
            Group.class,
            Profile.class,
            AuditedOrderItem.class,
            PositionQuantity.class,
            Order.class,
            OrderItem.class,
            OrderItemStatus.class,
            OrderStatus.class,
            OrderStatusWi.class,
            Product.class,
            ProductSynonym.class,
            UserGroup.class,
            BigOrder.class,
            BigOrderItem.class
        };
    }

    public static Timestamp getTestTimestamp()
    {
        return testTimestamp;
    }

    public static Date getTestDate()
    {
        return testDate;
    }

    public TestSqlDatatypes()
    {
        super("Mithra Object Tests");
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new ParaDeskResultSetComparator());
    }
}
