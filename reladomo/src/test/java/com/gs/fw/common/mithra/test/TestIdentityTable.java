
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

import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.domain.IdentityTable;
import com.gs.fw.common.mithra.test.domain.IdentityTableFinder;
import com.gs.fw.common.mithra.test.domain.IdentityTableList;

import java.sql.SQLException;
import java.sql.Timestamp;

/**
 *  Test various ops against table with identity column
 */
public class TestIdentityTable
extends MithraTestAbstract
{
    private static final int TIMES_REPEAT = 3;

    public Class[] getRestrictedClassList()
    {
        return new Class[] {IdentityTable.class};
    }

    public void testOneRowInsertToIdentityTable() throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                IdentityTable it = createNewIdentityTableWithDescription("TEST ROW", 1);
                it.insert();
                assertTrue("Identity value wasn't set inside a transaction", it.getObjectId()!=0);
                return null;
            }
        });
        assertEquals("Insert failed", 3 + 1, new IdentityTableList(IdentityTableFinder.all()).size());
    }
    
    public void testBatchInsertToIdentityTable() throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                IdentityTableList list = new IdentityTableList();
                for (int i=0; i<TIMES_REPEAT; i++)
                {
                    IdentityTable it = createNewIdentityTableWithDescription("TEST ROW " + i, i);
                    list.add(it);
                }
                list.insertAll();
                for (int i=0; i<TIMES_REPEAT; i++)
                {
                    assertTrue("Identity value wasn't set inside a transaction", list.get(i).getObjectId()!=0);
                }
                return null;
            }
        });
        assertEquals("Insert failed", 3 + TIMES_REPEAT, new IdentityTableList(IdentityTableFinder.all()).size());
    }
    
    public void testBatchUpdateToIdentityTable() throws SQLException
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();

        IdentityTableList list = new IdentityTableList(IdentityTableFinder.all());
        list.setOrderBy(IdentityTableFinder.objectId().ascendingOrderBy());
        for (int i=0; i<list.size(); i++)
        {
            list.get(i).setDescription("TEST ROW " + i);
        }
        
        tx.commit();
        
        list = new IdentityTableList(IdentityTableFinder.all());
        list.setOrderBy(IdentityTableFinder.objectId().ascendingOrderBy());
        for (int i=0; i<list.size(); i++)
        {
            assertTrue("Update failed", list.get(i).getDescription().equals("TEST ROW " + i));
        }
    }

    public void testMultiUpdateToIdentityTable() throws SQLException
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        
        IdentityTableList list = new IdentityTableList(IdentityTableFinder.all());
        for (int i=0; i< list.size(); i++)
        {
            list.setDescription("UPDATED DESCRIPTION");
        }

        tx.commit();

        list = new IdentityTableList(IdentityTableFinder.all());
        for (IdentityTable it : list)
        {
            assertEquals("Batch update failed", "UPDATED DESCRIPTION", it.getDescription());
        }
    }
    
    public void testOneRowDeleteFromIdentityTable() throws SQLException
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        IdentityTableList list = new IdentityTableList(IdentityTableFinder.all());
        list.get(0).delete();        
        tx.commit();

        assertEquals("Delete failed", 2, new IdentityTableList(IdentityTableFinder.all()).size());
    }
    
    public void testBatchDeleteFromIdentityTable() throws SQLException
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        
        IdentityTableList list = new IdentityTableList();
        for (int i=0; i< TIMES_REPEAT; i++)
        {
            IdentityTable it = this.createNewIdentityTableWithDescription("TEST ROW " + i, i);
            list.add(it);                        
        }
        list.insertAll();

        new IdentityTableList(IdentityTableFinder.all()).deleteAll();
        
        tx.commit();

        assertEquals("Batch delete failed", 0, new IdentityTableList(IdentityTableFinder.all()).size());
    }
    
    private IdentityTable createNewIdentityTableWithDescription(String description, int i)
    {
        IdentityTable it = new IdentityTable();
        it.setMonth(new Timestamp(System.currentTimeMillis()));
        it.setOrderId(i);
        it.setDescription(description);
 
        return it;
    }    
}
