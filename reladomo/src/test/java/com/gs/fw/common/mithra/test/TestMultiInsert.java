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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;


public class TestMultiInsert extends MithraTestAbstract
{
    static private Logger logger = LoggerFactory.getLogger(TestMultiInsert.class.getName());

    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new OrderTestResultSetComparator());
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class,
            OrderItem.class,
            FileDirectory.class,
            Employee.class,
            Book.class,
            TestAgeBalanceSheetRunRate.class
        };
    }

    public void testMultiInsert() throws SQLException
    {
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        Connection con = this.getConnection();
//        String sql = "insert into APP.ORDERS (ORDER_ID,ORDER_DATE,USER_ID,DESCRIPTION,STATE,TRACKING_ID) select ?,?,?,?,?,? union all select ?,?,?,?,?,?";
        String sql = "insert into APP.ORDERS (ORDER_ID,ORDER_DATE,USER_ID,DESCRIPTION,STATE,TRACKING_ID) values (?,?,?,?,?,?), (?,?,?,?,?,?)";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, 5555);
        ps.setTimestamp(2, orderDate);
        ps.setInt(3, 2222);
        ps.setString(4, "test desc");
        ps.setString(5, "teststate");
        ps.setString(6, "testid");
        ps.setInt(7, 5556);
        ps.setTimestamp(8, orderDate);
        ps.setInt(9, 2222);
        ps.setString(10, "test desc");
        ps.setString(11, "teststate");
        ps.setString(12, "testid");
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        assertEquals(updatedRows, 2);

    }

    public void serializeStuff()
    {
        Order order;
        RelatedFinder finder = OrderFinder.getFinderInstance();

        Attribute[] attributes = finder.getPersistentAttributes();
        Class type = attributes[0].valueType();
        attributes[0].getAttributeName();


    }
}
