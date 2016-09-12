
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

package com.gs.fw.common.mithra.test.pure;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.pure.*;
import com.gs.fw.finder.Operation;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class TestPureObjects extends MithraTestAbstract
{

    protected Class[] getRestrictedClassList()
    {
        Set<Class> result = new HashSet<Class>();

        result.add(PureType2ReadOnlyTypes.class);
        result.add(PureType2ReadOnlyTypesA.class);
        result.add(PureType2ReadOnlyTypesB.class);
        result.add(PureType2TxnTypes.class);
        result.add(PureType2TxnTypesA.class);
        result.add(PureType2TxnTypesB.class);
        result.add(PureType2DatedReadOnlyTypes.class);
        result.add(PureType2DatedReadOnlyTypesA.class);
        result.add(PureType2DatedReadOnlyTypesB.class);
        result.add(PureType2DatedTxnTypes.class);
        result.add(PureType2DatedTxnTypesA.class);
        result.add(PureType2DatedTxnTypesB.class);

        Class[] array = new Class[result.size()];
        result.toArray(array);
        return array;
    }

    public void testType2FindOne()
    {
        Operation op1 = PureType2ReadOnlyTypesAFinder.pkCharAttribute().eq('A').and(PureType2ReadOnlyTypesAFinder.pkBooleanAttribute().eq(true));
        assertNotNull(PureType2ReadOnlyTypesAFinder.findOne(op1));
        assertNotNull(PureType2ReadOnlyTypesAFinder.findOneBypassCache(op1));

        Operation op2 = PureType2TxnTypesBFinder.pkCharAttribute().eq('B').and(PureType2TxnTypesBFinder.pkBooleanAttribute().eq(false));
        assertNotNull(PureType2TxnTypesBFinder.findOne(op2));
        assertNotNull(PureType2TxnTypesBFinder.findOneBypassCache(op2));
    }

    public void testType2DatedFindOne()
    {
        Operation op1 = PureType2DatedReadOnlyTypesAFinder.pkCharAttribute().eq('A').and(
            PureType2DatedReadOnlyTypesAFinder.pkBooleanAttribute().eq(true)).and(
            PureType2DatedReadOnlyTypesAFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            PureType2DatedReadOnlyTypesAFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0"))));
        assertNotNull(PureType2DatedReadOnlyTypesAFinder.findOne(op1));
        assertNotNull(PureType2DatedReadOnlyTypesAFinder.findOneBypassCache(op1));

        Operation op2 = PureType2DatedTxnTypesBFinder.pkCharAttribute().eq('B').and(
            PureType2DatedTxnTypesBFinder.pkBooleanAttribute().eq(true).and(
            PureType2DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            PureType2DatedTxnTypesBFinder.processingDate().eq(Timestamp.valueOf("2007-09-22 00:00:00.0")))));
        assertNotNull(PureType2DatedTxnTypesBFinder.findOne(op2));
        assertNotNull(PureType2DatedTxnTypesBFinder.findOneBypassCache(op2));
    }

    public void testType2FindAll()
    {
        assertEquals(2, PureType2ReadOnlyTypesAFinder.findMany(PureType2ReadOnlyTypesAFinder.all()).size());
        assertEquals(2, PureType2ReadOnlyTypesAFinder.findManyBypassCache(PureType2ReadOnlyTypesAFinder.all()).size());

        assertEquals(2, PureType2TxnTypesBFinder.findMany(PureType2TxnTypesBFinder.all()).size());
        assertEquals(2, PureType2TxnTypesBFinder.findManyBypassCache(PureType2TxnTypesBFinder.all()).size());
    }

    public void testType2DatedFindAll()
    {
        Operation op1 = PureType2DatedReadOnlyTypesAFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            PureType2DatedReadOnlyTypesAFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")));
        assertEquals(2, PureType2DatedReadOnlyTypesAFinder.findMany(op1).size());
        assertEquals(2, PureType2DatedReadOnlyTypesAFinder.findManyBypassCache(op1).size());

        Operation op2 = PureType2DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            PureType2DatedTxnTypesBFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")));
        assertEquals(2, PureType2DatedTxnTypesBFinder.findMany(op2).size());
        assertEquals(2, PureType2DatedTxnTypesBFinder.findManyBypassCache(op2).size());
    }

    public void testType2Insert()
    {
        Operation op = PureType2TxnTypesAFinder.pkCharAttribute().eq('A').and(
            PureType2TxnTypesAFinder.pkBooleanAttribute().eq(true).and(
            PureType2TxnTypesAFinder.pkByteAttribute().eq((byte) 1).and(
            PureType2TxnTypesAFinder.pkStringAttribute().eq("insert"))));
        assertNull(PureType2TxnTypesAFinder.findOne(op));

        this.createPureType2TxnTypes('A', true, (byte) 1, "insert").insert();

        assertNotNull(PureType2TxnTypesAFinder.findOne(op));
        assertNotNull(PureType2TxnTypesAFinder.findOneBypassCache(op));
    }

//    public void testType2DatedInsert()
//    {
//        Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
//        Operation op = PureType2DatedTxnTypesAFinder.pkCharAttribute().eq('A').and(
//            PureType2DatedTxnTypesAFinder.pkBooleanAttribute().eq(true).and(
//            PureType2DatedTxnTypesAFinder.pkByteAttribute().eq((byte) 1).and(
//            PureType2DatedTxnTypesAFinder.pkStringAttribute().eq("insert").and(
//            PureType2DatedTxnTypesAFinder.businessDate().eq(businessDate)))));
//        assertNull(PureType2TxnTypesAFinder.findOne(op));
//
//        this.createPureType2TxnTypes('A', true, (byte) 1, "insert").insert();
//
//        assertNotNull(PureType2DatedTxnTypesAFinder.findOne(op));
//        assertNotNull(PureType2DatedTxnTypesAFinder.findOneBypassCache(op));
//    }

    public void testType2BatchInsert()
    {
        Operation op = PureType2TxnTypesBFinder.pkStringAttribute().eq("batch");
        assertEquals(0, PureType2TxnTypesBFinder.findMany(op).size());

        final PureType2TxnTypesBList typesList = new PureType2TxnTypesBList();
        for (int i = 0; i < 10; i++)
        {
            PureType2TxnTypesB types = (PureType2TxnTypesB) this.createPureType2TxnTypes('B', true, (byte) i, "batch");
            typesList.add(types);
        }
        typesList.insertAll();

        assertEquals(10, PureType2TxnTypesBFinder.findMany(op).size());
        assertEquals(10, PureType2TxnTypesBFinder.findManyBypassCache(op).size());
    }

    protected PureType2TxnTypes createPureType2TxnTypes(char type, boolean boolean0, byte byte0, String string0)
    {
        Date now = new Date();
        java.sql.Date nowSqlDate = new java.sql.Date(now.getTime());
        Timestamp nowTimestamp = new Timestamp(now.getTime());
        return new PureType2TxnTypes.Builder(type).withBoolean(boolean0).withByte(byte0).withDate(nowSqlDate).withDouble(-1.1d)
                .withFloat(-2.2f).withInt(-3).withLong(-4l).withShort((short) -5).withString(string0).withTimestamp(nowTimestamp).build();
    }

    protected PureType2DatedTxnTypes createPureType2DatedTxnTypes(char type, boolean boolean0, byte byte0, String string0)
    {
        Date now = new Date();
        java.sql.Date nowSqlDate = new java.sql.Date(now.getTime());
        Timestamp nowTimestamp = new Timestamp(now.getTime());
        Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        return new PureType2DatedTxnTypes.Builder(businessDate, type).withBoolean(boolean0).withByte(byte0).withDate(nowSqlDate).withDouble(-1.1d)
                .withFloat(-2.2f).withInt(-3).withLong(-4l).withShort((short) -5).withString(string0).withTimestamp(nowTimestamp).build();
    }

    public void testType2Update()
    {
        Operation op = PureType2TxnTypesBFinder.charAttribute().eq('B').and(
            PureType2TxnTypesBFinder.booleanAttribute().eq(false).and(
            PureType2TxnTypesBFinder.byteAttribute().eq((byte) 1).and(
            PureType2TxnTypesBFinder.stringAttribute().eq("update"))));
        assertNull(PureType2TxnTypesBFinder.findOne(op));

        final PureType2TxnTypesB newTypes = PureType2TxnTypesBFinder.findOneBypassCache(
                PureType2TxnTypesBFinder.pkCharAttribute().eq('B').and(
                PureType2TxnTypesBFinder.pkBooleanAttribute().eq(true)));
        assertNotNull(newTypes);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                newTypes.setCharAttribute('B');
                newTypes.setBooleanAttribute(false);
                newTypes.setByteAttribute((byte) 1);
                newTypes.setStringAttribute("update");
                return null;
            }
        });

        assertNotNull(PureType2TxnTypesBFinder.findOne(op));
        assertNotNull(PureType2TxnTypesBFinder.findOneBypassCache(op));
    }

    public void testType2BatchUpdate()
    {
        Operation op = PureType2TxnTypesAFinder.stringAttribute().eq("batch");
        assertEquals(0, PureType2TxnTypesAFinder.findMany(op).size());
        final PureType2TxnTypesAList typesList = PureType2TxnTypesAFinder.findMany(PureType2TxnTypesAFinder.all());
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                typesList.setStringAttribute("batch");
                return null;
            }
        });

        assertEquals(2, PureType2TxnTypesAFinder.findMany(op).size());
        assertEquals(2, PureType2TxnTypesAFinder.findManyBypassCache(op).size());
    }

    public void testType2MultiUpdate()
    {
        Operation op = PureType2TxnTypesAFinder.charAttribute().eq('A').and(
            PureType2TxnTypesAFinder.booleanAttribute().eq(false).and(
            PureType2TxnTypesAFinder.byteAttribute().eq((byte) 1).and(
            PureType2TxnTypesAFinder.stringAttribute().eq("multi"))));
        assertEquals(0, PureType2TxnTypesAFinder.findMany(op).size());
        final PureType2TxnTypesAList typesList = PureType2TxnTypesAFinder.findMany(PureType2TxnTypesAFinder.all());
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (PureType2TxnTypesA types : typesList)
                {
                    types.setCharAttribute('A');
                    types.setBooleanAttribute(false);
                    types.setByteAttribute((byte) 1);
                    types.setStringAttribute("multi");
                }
                return null;
            }
        });

        assertEquals(2, PureType2TxnTypesAFinder.findMany(op).size());
        assertEquals(2, PureType2TxnTypesAFinder.findManyBypassCache(op).size());
    }

    public void testType2Delete()
    {
        Operation op = PureType2TxnTypesBFinder.pkCharAttribute().eq('B').and(PureType2TxnTypesBFinder.pkBooleanAttribute().eq(false));
        final PureType2TxnTypesB types = PureType2TxnTypesBFinder.findOneBypassCache(op);
        assertNotNull(types);

        types.delete();

        assertNull(PureType2TxnTypesBFinder.findOne(op));
        assertNull(PureType2TxnTypesBFinder.findOneBypassCache(op));
    }

    public void testType2BatchDelete()
    {
        final PureType2TxnTypesAList typesList = PureType2TxnTypesAFinder.findMany(PureType2TxnTypesAFinder.all());
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (PureType2TxnTypesA types : typesList)
                {
                    types.delete();
                }
                return null;
            }
        });

        assertEquals(0, PureType2TxnTypesAFinder.findMany(PureType2TxnTypesAFinder.all()).size());
        assertEquals(0, PureType2TxnTypesAFinder.findManyBypassCache(PureType2TxnTypesAFinder.all()).size());
    }

    public void testType2OperationBasedDelete()
    {
        final PureType2TxnTypesBList typesList = PureType2TxnTypesBFinder.findMany(PureType2TxnTypesBFinder.all());
        assertEquals(2, typesList.size());

        typesList.deleteAll();

        assertEquals(0, PureType2TxnTypesBFinder.findMany(PureType2TxnTypesBFinder.all()).size());
        assertEquals(0, PureType2TxnTypesBFinder.findManyBypassCache(PureType2TxnTypesBFinder.all()).size());
    }
}
