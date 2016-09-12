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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestInheritance
		extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            InventoryItem.class,
            Book.class,
            Manufacturer.class,
            SupplierInventoryItem.class,
            Location.class,
            Supplier.class

        };
    }

	public void testSimpleRetreival()
	{
		InventoryItemFinder itemFinder = BookFinder.getFinderInstance();
		BookList list = new BookList(itemFinder.inventoryId().eq(1));
		InventoryItem firstBook = list.getBookAt(0);
		assertEquals(firstBook.getDescription(), "Design Patterns");
	}

	public void testRelationshipRetreival() throws SQLException
	{
		InventoryItemFinder itemFinder = BookFinder.getFinderInstance();
		String location = "New York";
		BookList list = new BookList(itemFinder.manufacturer().location().city().eq(location)
				.and(itemFinder.suppliers().location().eq(location)));
		list.forceResolve();
		String sql = "select count(distinct BOOK_ID) from BOOK A, SUPPLIER C, SUPPLIER_INVENTORY_ITEM D, MANUFACTURER B, LOCATION L " +
				"where A.MANUFACTURER_ID = B.MANUFACTURER_ID and B.LOCATION_ID = L.OBJECTID and L.CITY = ? " +
				"and A.BOOK_ID = D.INVENTORY_ID and D.SUPPLIER_ID = C.SUPPLIER_ID and C.LOCATION = ?";
		Connection conn = getConnection();
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, location);
		pstmt.setString(2, location);
		ResultSet rs = pstmt.executeQuery();
		rs.next();
		int count = rs.getInt(1);
		conn.close();
		assertEquals(list.size(), count);
	}

	public void testDistinctRelationshipRetreival() throws SQLException
	{
		InventoryItemFinder itemFinder = BookFinder.getFinderInstance();
		String location = "New York";
		BookList list = new BookList(itemFinder.suppliers().location().eq(location));
		list.forceResolve();
		String sql = "select count(distinct BOOK_ID) from BOOK A, SUPPLIER C, SUPPLIER_INVENTORY_ITEM D " +
				"where A.BOOK_ID = D.INVENTORY_ID and D.SUPPLIER_ID = C.SUPPLIER_ID and C.LOCATION = ?";
		Connection conn = getConnection();
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, location);
		ResultSet rs = pstmt.executeQuery();
		rs.next();
		int count = rs.getInt(1);
		conn.close();
		assertEquals(list.size(), count);
	}

    public void testFinderWithSuperClassAttribute() throws SQLException
    {
        InventoryItemFinder itemFinder = BookFinder.getFinderInstance();
        BookList list = new BookList(itemFinder.unitPrice().greaterThan(10));
        list.forceResolve();
    }

}
