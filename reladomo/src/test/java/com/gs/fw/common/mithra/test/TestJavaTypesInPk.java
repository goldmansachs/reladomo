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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.AllJavaTypesInPk;
import com.gs.fw.common.mithra.test.domain.AllJavaTypesInPkFinder;
import com.gs.fw.common.mithra.test.domain.AllJavaTypesInPkList;

import java.sql.Timestamp;
import java.util.Date;

/**
 * ******************************************************************************
 * File:        : $Source$
 *
 *
 *
 * ******************************************************************************
 */
public class TestJavaTypesInPk
extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            AllJavaTypesInPk.class,
        };
    }

    public void testCharPk()
    {
        insertForTest(true, 'N', 'N', new Date(), 123.45, 678.90, 88, 880, 8, "s", new Timestamp(System.currentTimeMillis()), 99999.99);

        Operation op = AllJavaTypesInPkFinder.charPk().eq('N');
        AllJavaTypesInPk first = AllJavaTypesInPkFinder.findOne(op);

        assertEquals(first.getCharPk(), 'N');
    }

    public void testMultiUpdateChangeInt()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Date d = new java.sql.Date(2006, 5, 7);
                Timestamp t = new Timestamp(System.currentTimeMillis());
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 1, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 2, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 3, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 4, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 5, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 6, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 7, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 8, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 9, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 10, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 11, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 12, 880, 8, "mu", t, 99999.99);
                return null;
            }
        });

        Operation op = AllJavaTypesInPkFinder.stringPk().eq("mu");
        final AllJavaTypesInPkList list = new AllJavaTypesInPkList(op);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                list.setDoubleNonPk(1234);
                return null;
            }
        });
        AllJavaTypesInPkFinder.clearQueryCache();
        AllJavaTypesInPkList list2 = new AllJavaTypesInPkList(op);
        for(int i=0;i<list.size();i++)
        {
            assertEquals(1234.0, list2.getAllJavaTypesInPkAt(i).getDoubleNonPk(), 0.0);
        }
    }

    public void testMultiUpdateChangeLongChar()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Date d = new java.sql.Date(2006, 5, 7);
                Timestamp t = new Timestamp(System.currentTimeMillis());
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 2, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 2, 881, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 2, 882, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 2, 883, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 2, 884, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 2, 885, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'M', d, 123.45, 678.90, 2, 886, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'M', d, 123.45, 678.90, 2, 887, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'M', d, 123.45, 678.90, 2, 888, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'M', d, 123.45, 678.90, 20, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'M', d, 123.45, 678.90, 20, 890, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'M', d, 123.45, 678.90, 21, 891, 8, "mu", t, 99999.99);
                return null;
            }
        });

        Operation op = AllJavaTypesInPkFinder.stringPk().eq("mu");
        final AllJavaTypesInPkList list = new AllJavaTypesInPkList(op);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                list.setDoubleNonPk(1234);
                return null;
            }
        });
        AllJavaTypesInPkFinder.clearQueryCache();
        AllJavaTypesInPkList list2 = new AllJavaTypesInPkList(op);
        for(int i=0;i<list.size();i++)
        {
            assertEquals(1234.0, list2.getAllJavaTypesInPkAt(i).getDoubleNonPk(), 0.0);
        }
    }

    public void testMultiUpdateChangeIntLong()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Date d = new java.sql.Date(2006, 5, 7);
                Timestamp t = new Timestamp(System.currentTimeMillis());
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 1, 880, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 2, 881, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 3, 882, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 4, 883, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 5, 884, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 6, 885, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 7, 886, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 8, 887, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 9, 888, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 10, 890, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 11, 891, 8, "mu", t, 99999.99);
                insertForTest(true, 'N', 'N', d, 123.45, 678.90, 12, 892, 8, "mu", t, 99999.99);
                return null;
            }
        });

        Operation op = AllJavaTypesInPkFinder.stringPk().eq("mu");
        final AllJavaTypesInPkList list = new AllJavaTypesInPkList(op);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                list.setDoubleNonPk(1234);
                return null;
            }
        });
        AllJavaTypesInPkFinder.clearQueryCache();
        AllJavaTypesInPkList list2 = new AllJavaTypesInPkList(op);
        for(int i=0;i<list.size();i++)
        {
            assertEquals(1234.0, list2.getAllJavaTypesInPkAt(i).getDoubleNonPk(), 0.0);
        }
    }

    private void insertForTest(boolean b, char charForByte, char c, Date d, double db, double doubleForFloat, int i, long l, int intForShort,
            String str, Timestamp t, double value)
    {
        AllJavaTypesInPk insert = new AllJavaTypesInPk();

        insert.setBooleanPk(b);
        insert.setBytePk((byte) charForByte);
        insert.setCharPk(c);
        insert.setDatePk(d);
        insert.setDoublePk(db);
        insert.setFloatPk((float) doubleForFloat);
        insert.setIntPk(i);
        insert.setLongPk(l);
        insert.setShortPk((short) intForShort);
        insert.setStringPk(str);
        insert.setTimestampPk(t);

        insert.setDoubleNonPk(value);

        insert.insert();
    }
}
