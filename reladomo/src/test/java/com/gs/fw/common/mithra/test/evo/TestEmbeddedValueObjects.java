
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

package com.gs.fw.common.mithra.test.evo;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderStatus;
import com.gs.fw.common.mithra.test.domain.evo.*;
import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.finder.Operation;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.math.BigDecimal;

public class TestEmbeddedValueObjects extends MithraTestAbstract
{

    protected Class[] getRestrictedClassList()
    {
        Set<Class> result = new HashSet<Class>();

        result.add(EvoType1ReadOnlyTypes.class);
        result.add(OrderStatus.class);
        result.add(Order.class);
        result.add(EvoType1TxnTypes.class);
        result.add(EvoType1DatedReadOnlyTypes.class);
        result.add(EvoType1DatedTxnTypes.class);

        result.add(EvoType2ReadOnlyTypes.class);
        result.add(EvoType2ReadOnlyTypesA.class);
        result.add(EvoType2ReadOnlyTypesB.class);
        result.add(EvoType2TxnTypes.class);
        result.add(EvoType2TxnTypesA.class);
        result.add(EvoType2TxnTypesB.class);
        result.add(EvoType2DatedReadOnlyTypes.class);
        result.add(EvoType2DatedReadOnlyTypesA.class);
        result.add(EvoType2DatedReadOnlyTypesB.class);
        result.add(EvoType2DatedTxnTypes.class);
        result.add(EvoType2DatedTxnTypesA.class);
        result.add(EvoType2DatedTxnTypesB.class);
        result.add(EvoTypeTime.class);
        result.add(EvoType1TimeTypes.class);

        result.add(EvoType3ReadOnlyTypes.class);
        result.add(EvoType3ReadOnlyTypesA.class);
        result.add(EvoType3ReadOnlyTypesB.class);
        result.add(EvoType3TxnTypes.class);
        result.add(EvoType3TxnTypesA.class);
        result.add(EvoType3TxnTypesB.class);
        //TODO: ledav fix dated type 3
//        result.add(EvoType3DatedReadOnlyTypes.class);
//        result.add(EvoType3DatedReadOnlyTypesA.class);
//        result.add(EvoType3DatedReadOnlyTypesB.class);
//        result.add(EvoType3DatedTxnTypes.class);
//        result.add(EvoType3DatedTxnTypesA.class);
//        result.add(EvoType3DatedTxnTypesB.class);

        result.add(EvoTypesRoot.class);
        result.add(EvoTypesNested.class);
        result.add(EvoTypesLeaf.class);
        result.add(EvoTypesLeafA.class);
        result.add(EvoTypesLeafB.class);
        result.add(EvoTypesTime.class);
        result.add(EvoTypeTime.class);

        Class[] array = new Class[result.size()];
        result.toArray(array);
        return array;
    }

    public void testType1FindOne()
    {
        Operation op1 = EvoType1ReadOnlyTypesFinder.pk().charAttribute().eq('A').and(EvoType1ReadOnlyTypesFinder.pk().booleanAttribute().eq(true));
        assertNotNull(EvoType1ReadOnlyTypesFinder.findOne(op1) instanceof EvoType1ReadOnlyTypesA);
        assertNotNull(EvoType1ReadOnlyTypesFinder.findOneBypassCache(op1) instanceof EvoType1ReadOnlyTypesA);

        Operation op2 = EvoType1TxnTypesFinder.pk().charAttribute().eq('B').and(EvoType1TxnTypesFinder.pk().booleanAttribute().eq(false));
        assertNotNull(EvoType1TxnTypesFinder.findOne(op2) instanceof EvoType1TxnTypesB);
        assertNotNull(EvoType1TxnTypesFinder.findOneBypassCache(op2) instanceof EvoType1TxnTypesB);

        Operation op3 = EvoType1ReadOnlyTypesFinder.pk().bigDecimalAttribute().eq(new BigDecimal("14444444.44"));
        assertNotNull(EvoType1ReadOnlyTypesFinder.findOne(op3) instanceof EvoType1ReadOnlyTypesA);
        assertNotNull(EvoType1ReadOnlyTypesFinder.findOneBypassCache(op3) instanceof EvoType1ReadOnlyTypesA);

        Operation op4 = EvoType1TxnTypesFinder.pk().charAttribute().eq('B');
        op4 = op4.and(EvoType1TxnTypesFinder.pk().booleanAttribute().eq(true));
        op4 = op4.and(EvoType1TxnTypesFinder.pk().bigDecimalAttribute().eq(new BigDecimal("1333333.33")));
        assertNotNull(EvoType1TxnTypesFinder.findOne(op4) instanceof EvoType1TxnTypesB);
        assertNotNull(EvoType1TxnTypesFinder.findOneBypassCache(op4) instanceof EvoType1TxnTypesB);

        Operation op5 = EvoType1TimeTypesFinder.pk().timeAttribute().eq(Time.withMillis(1, 25, 30, 00)).and(EvoType1TimeTypesFinder.booleanAttribute().eq(true));
        assertNotNull(EvoType1TimeTypesFinder.findOne(op5) instanceof EvoType1TimeTypes);
        assertNotNull(EvoType1TimeTypesFinder.findOneBypassCache(op5) instanceof EvoType1TimeTypes);

        Operation op6 = EvoType1TimeTypesFinder.pk().timeAttribute().eq(Time.withMillis(1, 25, 30, 00)).and(EvoType1TimeTypesFinder.booleanAttribute().eq(false));
        assertNotNull(EvoType1TimeTypesFinder.findOne(op6) instanceof EvoType1TimeTypes);
        assertNotNull(EvoType1TimeTypesFinder.findOneBypassCache(op6) instanceof EvoType1TimeTypes);
    }

    public void testType2FindOne()
    {
        Operation op1 = EvoType2ReadOnlyTypesAFinder.pk().charAttribute().eq('A').and(EvoType2ReadOnlyTypesAFinder.pk().booleanAttribute().eq(true));
        assertNotNull(EvoType2ReadOnlyTypesAFinder.findOne(op1));
        assertNotNull(EvoType2ReadOnlyTypesAFinder.findOneBypassCache(op1));

        Operation op2 = EvoType2TxnTypesBFinder.pk().charAttribute().eq('B').and(EvoType2TxnTypesBFinder.pk().booleanAttribute().eq(false));
        assertNotNull(EvoType2TxnTypesBFinder.findOne(op2));
        assertNotNull(EvoType2TxnTypesBFinder.findOneBypassCache(op2));
    }

    public void testType3FindOne()
    {
        Operation op1 = EvoType3ReadOnlyTypesAFinder.pk().charAttribute().eq('A').and(EvoType3ReadOnlyTypesAFinder.pk().booleanAttribute().eq(true));
        assertNotNull(EvoType3ReadOnlyTypesAFinder.findOne(op1));
        assertNotNull(EvoType3ReadOnlyTypesAFinder.findOneBypassCache(op1));

        Operation op2 = EvoType3TxnTypesBFinder.pk().charAttribute().eq('B').and(EvoType3TxnTypesBFinder.pk().booleanAttribute().eq(false));
        assertNotNull(EvoType3TxnTypesBFinder.findOne(op2));
        assertNotNull(EvoType3TxnTypesBFinder.findOneBypassCache(op2));
    }

    public void testType1DatedFindOne()
    {
        Operation op1 = EvoType1DatedReadOnlyTypesFinder.pk().charAttribute().eq('A').and(
            EvoType1DatedReadOnlyTypesFinder.pk().booleanAttribute().eq(true)).and(
            EvoType1DatedReadOnlyTypesFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType1DatedReadOnlyTypesFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0"))));
        assertEquals(150, EvoType1DatedReadOnlyTypesFinder.findOne(op1).getRootEvo().getIntAttribute());
        assertEquals(150, EvoType1DatedReadOnlyTypesFinder.findOneBypassCache(op1).getRootEvo().getIntAttribute());
        assertEquals(new BigDecimal("111111111111111.111"), EvoType1DatedReadOnlyTypesFinder.findOne(op1).getRootEvo().getBigDecimalAttribute());

        Operation op3 = EvoType1DatedReadOnlyTypesFinder.pk().charAttribute().eq('A').and(
            EvoType1DatedReadOnlyTypesFinder.pk().booleanAttribute().eq(true)).and(
            EvoType1DatedReadOnlyTypesFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
        assertEquals(250, EvoType1DatedReadOnlyTypesFinder.findOne(op3).getRootEvo().getIntAttribute());
        assertEquals(250, EvoType1DatedReadOnlyTypesFinder.findOneBypassCache(op3).getRootEvo().getIntAttribute());
        assertEquals(new BigDecimal("122222222222222.222"), EvoType1DatedReadOnlyTypesFinder.findOne(op3).getRootEvo().getBigDecimalAttribute());

        Operation op2 = EvoType1DatedTxnTypesFinder.pk().charAttribute().eq('B').and(
            EvoType1DatedTxnTypesFinder.pk().booleanAttribute().eq(true)).and(
            EvoType1DatedTxnTypesFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType1DatedTxnTypesFinder.processingDate().eq(Timestamp.valueOf("2007-09-22 00:00:00.0"))));
        assertEquals(100, EvoType1DatedTxnTypesFinder.findOne(op2).getRootEvo().getIntAttribute());
        assertEquals(100, EvoType1DatedTxnTypesFinder.findOneBypassCache(op2).getRootEvo().getIntAttribute());

        Operation op4 = EvoType1TimeTypesFinder.pk().charAttribute().eq('B').and(
                EvoType1TimeTypesFinder.pk().timeAttribute().eq(Time.withMillis(1, 25, 30, 0)));
        assertEquals(0, EvoType1TimeTypesFinder.findOne(op4).getRootEvo().getIntAttribute());
        assertEquals(0, EvoType1TimeTypesFinder.findOneBypassCache(op4).getRootEvo().getIntAttribute());
    }

    public void testType2DatedFindOne()
    {
        Operation op1 = EvoType2DatedReadOnlyTypesAFinder.pk().booleanAttribute().eq(true).and(
            EvoType2DatedReadOnlyTypesAFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType2DatedReadOnlyTypesAFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0"))));
        assertEquals(150, EvoType2DatedReadOnlyTypesAFinder.findOne(op1).getRootEvo().getIntAttribute());
        assertEquals(150, EvoType2DatedReadOnlyTypesAFinder.findOneBypassCache(op1).getRootEvo().getIntAttribute());

        Operation op2 = EvoType2DatedTxnTypesBFinder.pk().booleanAttribute().eq(true).and(
            EvoType2DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType2DatedTxnTypesBFinder.processingDate().eq(Timestamp.valueOf("2007-09-22 00:00:00.0"))));
        assertEquals(100, EvoType2DatedTxnTypesBFinder.findOne(op2).getRootEvo().getIntAttribute());
        assertEquals(100, EvoType2DatedTxnTypesBFinder.findOneBypassCache(op2).getRootEvo().getIntAttribute());
    }

//    public void testType3DatedFindOne()
//    {
//        Operation op1 = EvoType3DatedReadOnlyTypesAFinder.pk().booleanAttribute().eq(true).and(
//            EvoType3DatedReadOnlyTypesAFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
//            EvoType3DatedReadOnlyTypesAFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0"))));
//        assertEquals(150, EvoType3DatedReadOnlyTypesAFinder.findOne(op1).getRootEvo().getIntAttribute());
//        assertEquals(150, EvoType3DatedReadOnlyTypesAFinder.findOneBypassCache(op1).getRootEvo().getIntAttribute());
//
//        Operation op2 = EvoType3DatedTxnTypesBFinder.pk().booleanAttribute().eq(true).and(
//            EvoType3DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
//            EvoType3DatedTxnTypesBFinder.processingDate().eq(Timestamp.valueOf("2007-09-22 00:00:00.0"))));
//        assertEquals(100, EvoType3DatedTxnTypesBFinder.findOne(op2).getRootEvo().getIntAttribute());
//        assertEquals(100, EvoType3DatedTxnTypesBFinder.findOneBypassCache(op2).getRootEvo().getIntAttribute());
//    }

    public void testType1FindAll()
    {
        int count = this.getRetrievalCount();
        EvoType1ReadOnlyTypesList many = EvoType1ReadOnlyTypesFinder.findMany(EvoType1ReadOnlyTypesFinder.all());
        many.deepFetch(EvoType1ReadOnlyTypesFinder.rootEvo().order());
        many.deepFetch(EvoType1ReadOnlyTypesFinder.rootEvo().nestedEvo().orderStatus());
        assertEquals(4, many.size());
        if (EvoType1ReadOnlyTypesFinder.getMithraObjectPortal().isPartiallyCached())
        {
            assertEquals(3+count, this.getRetrievalCount());
        }
        assertNotNull(many.get(0).getRootEvo().getOrder());
        assertNull(many.get(0).getRootEvo().getNestedEvo().getOrderStatus());
        if (EvoType1ReadOnlyTypesFinder.getMithraObjectPortal().isPartiallyCached())
        {
            assertEquals(3+count, this.getRetrievalCount());
        }
        assertEquals(4, EvoType1ReadOnlyTypesFinder.findManyBypassCache(EvoType1ReadOnlyTypesFinder.all()).size());

        assertEquals(4, EvoType1TxnTypesFinder.findMany(EvoType1TxnTypesFinder.all()).size());
        assertEquals(4, EvoType1TxnTypesFinder.findManyBypassCache(EvoType1TxnTypesFinder.all()).size());

        assertEquals(4, EvoType1TxnTypesFinder.findManyBypassCache(EvoType1ReadOnlyTypesFinder.rootEvo().order().description().startsWith("Third")).size());

        assertEquals(2, EvoType1TimeTypesFinder.findMany(EvoType1TimeTypesFinder.all()).size());
        assertEquals(2, EvoType1TimeTypesFinder.findManyBypassCache(EvoType1TimeTypesFinder.all()).size());
    }

    public void testType2FindAll()
    {
        assertEquals(2, EvoType2ReadOnlyTypesAFinder.findMany(EvoType2ReadOnlyTypesAFinder.all()).size());
        assertEquals(2, EvoType2ReadOnlyTypesAFinder.findManyBypassCache(EvoType2ReadOnlyTypesAFinder.all()).size());

        assertEquals(2, EvoType2TxnTypesBFinder.findMany(EvoType2TxnTypesBFinder.all()).size());
        assertEquals(2, EvoType2TxnTypesBFinder.findManyBypassCache(EvoType2TxnTypesBFinder.all()).size());
    }

    public void testType3FindAll()
    {
        assertEquals(4, EvoType3ReadOnlyTypesFinder.findMany(EvoType3ReadOnlyTypesFinder.all()).size());
        assertEquals(4, EvoType3ReadOnlyTypesFinder.findManyBypassCache(EvoType3ReadOnlyTypesFinder.all()).size());

        assertEquals(4, EvoType3TxnTypesFinder.findMany(EvoType3TxnTypesFinder.all()).size());
        assertEquals(4, EvoType3TxnTypesFinder.findManyBypassCache(EvoType3TxnTypesFinder.all()).size());
    }

    public void testType1DatedFindAll()
    {
        Operation op1 = EvoType1DatedReadOnlyTypesFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType1DatedReadOnlyTypesFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")));
        assertEquals(4, EvoType1DatedReadOnlyTypesFinder.findMany(op1).size());
        assertEquals(4, EvoType1DatedReadOnlyTypesFinder.findManyBypassCache(op1).size());

        Operation op2 = EvoType1DatedTxnTypesFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType1DatedTxnTypesFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")));
        assertEquals(4, EvoType1DatedTxnTypesFinder.findMany(op2).size());
        assertEquals(4, EvoType1DatedTxnTypesFinder.findManyBypassCache(op2).size());
    }

    public void testType2DatedFindAll()
    {
        Operation op1 = EvoType2DatedReadOnlyTypesAFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType2DatedReadOnlyTypesAFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")));
        assertEquals(2, EvoType2DatedReadOnlyTypesAFinder.findMany(op1).size());
        assertEquals(2, EvoType2DatedReadOnlyTypesAFinder.findManyBypassCache(op1).size());

        Operation op2 = EvoType2DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType2DatedTxnTypesBFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")));
        assertEquals(2, EvoType2DatedTxnTypesBFinder.findMany(op2).size());
        assertEquals(2, EvoType2DatedTxnTypesBFinder.findManyBypassCache(op2).size());
    }

//    public void testType3DatedFindAll()
//    {
//        Operation op1 = EvoType3DatedReadOnlyTypesAFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
//            EvoType3DatedReadOnlyTypesAFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")));
//        assertEquals(2, EvoType3DatedReadOnlyTypesAFinder.findMany(op1).size());
//        assertEquals(2, EvoType3DatedReadOnlyTypesAFinder.findManyBypassCache(op1).size());
//
//        Operation op2 = EvoType3DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
//            EvoType3DatedTxnTypesBFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")));
//        assertEquals(2, EvoType3DatedTxnTypesBFinder.findMany(op2).size());
//        assertEquals(2, EvoType3DatedTxnTypesBFinder.findManyBypassCache(op2).size());=
//    }

    public void testType1Insert()
    {
        EvoTypesLeaf pk = createEvoTypesLeaf('A', true, (byte) 1, "insert");
        Operation op = EvoType1TxnTypesFinder.pk().eq(pk);
        assertNull(EvoType1TxnTypesFinder.findOneBypassCache(op));

        final EvoType1TxnTypes types = new EvoType1TxnTypes();
        types.copyPk(pk);
        types.insert();

        EvoType1TxnTypesFinder.clearQueryCache();
        //TODO: ledav fix date matching in full cache
//        assertNotNull(EvoType1TxnTypesFinder.findOne(op));
        assertNotNull(EvoType1TxnTypesFinder.findOneBypassCache(op));

        EvoTypesTime pk2 = createEvoTypesTime('A', true, (byte) 1, "insert", Time.withMillis(1, 1, 1, 1));
        Operation op2 = EvoType1TimeTypesFinder.pk().eq(pk2);
        assertNull(EvoType1TimeTypesFinder.findOneBypassCache(op2));

        final EvoType1TimeTypes types2 = new EvoType1TimeTypes();
        types2.copyPk(pk2);
        types2.insert();

        EvoType1TimeTypesFinder.clearQueryCache();
        assertNotNull(EvoType1TimeTypesFinder.findOneBypassCache(op2));
    }

    public void testType2Insert()
    {
        EvoTypesLeaf pk = createEvoTypesLeaf('A', true, (byte) 1, "insert");
        Operation op = EvoType2TxnTypesAFinder.pk().eq(pk);
        assertNull(EvoType2TxnTypesAFinder.findOneBypassCache(op));

        final EvoType2TxnTypes types = new EvoType2TxnTypesA();
        types.copyPk(pk);
        types.insert();

        EvoType2TxnTypesAFinder.clearQueryCache();
//        assertNotNull(EvoType2TxnTypesAFinder.findOne(op));
        assertNotNull(EvoType2TxnTypesAFinder.findOneBypassCache(op));
    }

    public void testType3Insert()
    {
        EvoTypesLeaf pk = createEvoTypesLeaf('A', true, (byte) 1, "insert");
        Operation op = EvoType3TxnTypesAFinder.pk().eq(pk);
        assertNull(EvoType3TxnTypesAFinder.findOneBypassCache(op));

        final EvoType3TxnTypes types = new EvoType3TxnTypesA();
        types.copyPk(pk);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.insert();
                return null;
            }
        });

//        assertNotNull(EvoType3TxnTypesAFinder.findOne(op));
        assertNotNull(EvoType3TxnTypesAFinder.findOneBypassCache(op));
    }

    public void testType1DatedInsert()
    {
        EvoTypesLeaf pk = createEvoTypesLeaf('A', true, (byte) 1, "insert");
        Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        Operation op = EvoType1DatedTxnTypesFinder.pk().eq(pk).and(EvoType1DatedTxnTypesFinder.businessDate().eq(businessDate));
        assertNull(EvoType1DatedTxnTypesFinder.findOneBypassCache(op));

        final EvoType1DatedTxnTypes types = new EvoType1DatedTxnTypes(businessDate);
        types.copyPk(pk);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.insert();
                return null;
            }
        });

//        assertNotNull(EvoType1DatedTxnTypesFinder.findOne(op));
        assertNotNull(EvoType1DatedTxnTypesFinder.findOneBypassCache(op));
    }

    public void testType2DatedInsert()
    {
        EvoTypesLeaf pk = createEvoTypesLeaf('A', true, (byte) 1, "insert");
        Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        Operation op = EvoType2DatedTxnTypesAFinder.pk().eq(pk).and(EvoType2DatedTxnTypesAFinder.businessDate().eq(businessDate));
        assertNull(EvoType2DatedTxnTypesAFinder.findOneBypassCache(op));

        final EvoType2DatedTxnTypes types = new EvoType2DatedTxnTypesA(businessDate);
        types.copyPk(pk);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.insert();
                return null;
            }
        });

//        assertNotNull(EvoType2DatedTxnTypesAFinder.findOne(op));
        assertNotNull(EvoType2DatedTxnTypesAFinder.findOneBypassCache(op));
    }

//    public void testType3DatedInsert()
//    {
//        EvoTypesLeaf pk = createEvoTypesLeaf('A', true, (byte) 1, "insert");
//        Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
//        Operation op = EvoType3DatedTxnTypesBFinder.pk().eq(pk).and(EvoType3DatedTxnTypesBFinder.businessDate().eq(businessDate));
//        assertNull(EvoType3DatedTxnTypesBFinder.findOneBypassCache(op));
//
//        final EvoType3DatedTxnTypes types = new EvoType3DatedTxnTypesB(businessDate);
//        types.copyPk(pk);
//        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
//        {
//            public Object executeTransaction(MithraTransaction tx) throws Throwable
//            {
//                types.insert();
//                return null;
//            }
//        });
//
////        assertNotNull(EvoType3DatedTxnTypesBFinder.findOne(op));
//        assertNotNull(EvoType3DatedTxnTypesBFinder.findOneBypassCache(op));
//    }

    public void testType1BatchInsert()
    {
        Operation op = EvoType1TxnTypesFinder.pk().stringAttribute().eq("batch");
        assertEquals(0, EvoType1TxnTypesFinder.findMany(op).size());

        final EvoType1TxnTypesList typesList = new EvoType1TxnTypesList();
        for (int i = 0; i < 10; i++)
        {
            EvoTypesLeaf typesPk = createEvoTypesLeaf('B', true, (byte) i, "batch");
            EvoType1TxnTypes types = new EvoType1TxnTypes();
            types.copyPk(typesPk);
            typesList.add(types);
        }
        typesList.insertAll();

        assertEquals(10, EvoType1TxnTypesFinder.findMany(op).size());
        assertEquals(10, EvoType1TxnTypesFinder.findManyBypassCache(op).size());

        Operation op2 = EvoType1TimeTypesFinder.pk().timeAttribute().eq(Time.withMillis(10, 10, 10, 10));
        assertEquals(0, EvoType1TimeTypesFinder.findMany(op2).size());

        final EvoType1TimeTypesList typesList2 = new EvoType1TimeTypesList();
        for (int i = 0; i < 10; i++)
        {
            EvoTypesTime typesPk = createEvoTypesTime('B', true, (byte) i, "batch", Time.withMillis(10, 10, 10, 10));
            EvoType1TimeTypes types = new EvoType1TimeTypes();
            types.copyPk(typesPk);
            typesList2.add(types);
        }
        typesList2.insertAll();

        assertEquals(10, EvoType1TimeTypesFinder.findMany(op2).size());
        assertEquals(10, EvoType1TimeTypesFinder.findManyBypassCache(op2).size());
    }

    public void testType2BatchInsert()
    {
        Operation op = EvoType2TxnTypesBFinder.pk().stringAttribute().eq("batch");
        assertEquals(0, EvoType2TxnTypesBFinder.findMany(op).size());

        final EvoType2TxnTypesBList typesList = new EvoType2TxnTypesBList();
        for (int i = 0; i < 10; i++)
        {
            EvoTypesLeaf typesPk = createEvoTypesLeaf('B', true, (byte) i, "batch");
            EvoType2TxnTypesB types = new EvoType2TxnTypesB();
            types.copyPk(typesPk);
            typesList.add(types);
        }
        typesList.insertAll();

        assertEquals(10, EvoType2TxnTypesBFinder.findMany(op).size());
        assertEquals(10, EvoType2TxnTypesBFinder.findManyBypassCache(op).size());
    }

    public void testType3BatchInsert()
    {
        Operation op = EvoType3TxnTypesBFinder.pk().stringAttribute().eq("batch");
        assertEquals(0, EvoType3TxnTypesBFinder.findMany(op).size());

        final EvoType3TxnTypesBList typesList = new EvoType3TxnTypesBList();
        for (int i = 0; i < 10; i++)
        {
            EvoTypesLeaf typesPk = createEvoTypesLeaf('B', true, (byte) i, "batch");
            EvoType3TxnTypesB types = new EvoType3TxnTypesB();
            types.copyPk(typesPk);
            typesList.add(types);
        }
        typesList.insertAll();

        assertEquals(10, EvoType3TxnTypesBFinder.findMany(op).size());
        assertEquals(10, EvoType3TxnTypesBFinder.findManyBypassCache(op).size());
    }

    public void testType1DatedBatchInsert()
    {
        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        Operation op = EvoType1DatedTxnTypesFinder.pk().stringAttribute().eq("batch").and(EvoType1DatedTxnTypesFinder.businessDate().eq(businessDate));
        assertEquals(0, EvoType1DatedTxnTypesFinder.findMany(op).size());

        final EvoType1DatedTxnTypesList typesList = new EvoType1DatedTxnTypesList();
        for (int i = 0; i < 10; i++)
        {
            EvoTypesLeaf typesPk = createEvoTypesLeaf('B', true, (byte) i, "batch");
            EvoType1DatedTxnTypes types = new EvoType1DatedTxnTypes(businessDate);
            types.copyPk(typesPk);
            typesList.add(types);
        }
        typesList.insertAll();

        assertEquals(10, EvoType1DatedTxnTypesFinder.findMany(op).size());
        assertEquals(10, EvoType1DatedTxnTypesFinder.findManyBypassCache(op).size());
    }

    public void testType2DatedBatchInsert()
    {
        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        Operation op = EvoType2DatedTxnTypesBFinder.pk().stringAttribute().eq("batch").and(EvoType2DatedTxnTypesBFinder.businessDate().eq(businessDate));
        assertEquals(0, EvoType2DatedTxnTypesBFinder.findMany(op).size());

        final EvoType2DatedTxnTypesBList typesList = new EvoType2DatedTxnTypesBList();
        for (int i = 0; i < 10; i++)
        {
            EvoTypesLeaf typesPk = createEvoTypesLeaf('B', true, (byte) i, "batch");
            EvoType2DatedTxnTypesB types = new EvoType2DatedTxnTypesB(businessDate);
            types.copyPk(typesPk);
            typesList.add(types);
        }
        typesList.insertAll();

        assertEquals(10, EvoType2DatedTxnTypesBFinder.findMany(op).size());
        assertEquals(10, EvoType2DatedTxnTypesBFinder.findManyBypassCache(op).size());
    }

//    public void testType3DatedBatchInsert()
//    {
//        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
//        Operation op = EvoType3DatedTxnTypesBFinder.pk().stringAttribute().eq("batch").and(EvoType3DatedTxnTypesBFinder.businessDate().eq(businessDate));
//        assertEquals(0, EvoType3DatedTxnTypesBFinder.findMany(op).size());
//
//        final EvoType3DatedTxnTypesBList typesList = new EvoType3DatedTxnTypesBList();
//        for (int i = 0; i < 10; i++)
//        {
//            EvoTypesLeaf typesPk = createEvoTypesLeaf('B', true, (byte) i, "batch");
//            EvoType3DatedTxnTypesB types = new EvoType3DatedTxnTypesB(businessDate);
//            types.copyPk(typesPk);
//            typesList.add(types);
//        }
//        typesList.insertAll();
//
//        assertEquals(10, EvoType3DatedTxnTypesBFinder.findMany(op).size());
//        assertEquals(10, EvoType3DatedTxnTypesBFinder.findManyBypassCache(op).size());
//    }

    protected EvoTypesLeaf createEvoTypesLeaf(char type, boolean boolean0, byte byte0, String string0)
    {
        Date now = new Date();
        Timestamp nowTimestamp = new Timestamp(now.getTime());
        return new EvoTypesLeaf.Builder(type).withBoolean(boolean0).withByte(byte0).withDate(now).withDouble(-1.1d)
            .withFloat(-2.2f).withInt(-3).withLong(-4l).withShort((short) -5).withString(string0).withTimestamp(nowTimestamp)
            .withBigDecimal(new BigDecimal("11111111.11")).build();
    }

    protected EvoTypesTime createEvoTypesTime(char type, boolean boolean0, byte byte0, String string0, Time time)
    {
        Date now = new Date();
        Timestamp nowTimestamp = new Timestamp(now.getTime());
        return new EvoTypesTime.Builder(type).withBoolean(boolean0).withByte(byte0).withDate(now).withTime(time).withDouble(-1.1d)
            .withFloat(-2.2f).withInt(-3).withLong(-4l).withShort((short) -5).withString(string0).withTimestamp(nowTimestamp)
            .withBigDecimal(new BigDecimal("11111111.11")).build();
    }

    public void testType1Update()
    {
        final EvoTypesRoot root = this.createEvoTypesRoot('B', false, (byte) 1, "update");
        assertNull(EvoType1TxnTypesFinder.findOne(EvoType1TxnTypesFinder.rootEvo().eq(root)));

        final EvoType1TxnTypes newTypes = EvoType1TxnTypesFinder.findOne(
                EvoType1TxnTypesFinder.pk().charAttribute().eq('B').and(
                EvoType1TxnTypesFinder.pk().booleanAttribute().eq(true)));
        assertNotNull(newTypes);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                newTypes.copyRootEvo(root);
                return null;
            }
        });

        assertNotNull(EvoType1TxnTypesFinder.findOne(EvoType1TxnTypesFinder.rootEvo().eq(root)));
        assertNotNull(EvoType1TxnTypesFinder.findOneBypassCache(EvoType1TxnTypesFinder.rootEvo().eq(root)));

        final EvoTypesRootWithTime time = this.createEvoTypesRootWithTime('B', false, (byte) 1, "update", Time.withMillis(1, 1, 1, 1));
        assertNull(EvoType1TimeTypesFinder.findOne(EvoType1TimeTypesFinder.rootEvo().eq(time)));

        final EvoType1TimeTypes newTypesTime = EvoType1TimeTypesFinder.findOne(
                EvoType1TimeTypesFinder.pk().charAttribute().eq('B').and(
                        EvoType1TimeTypesFinder.pk().timeAttribute().eq(Time.withMillis(1, 25, 30, 0))));
        assertNotNull(newTypesTime);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                newTypesTime.copyRootEvo(time);
                return null;
            }
        });

        assertNotNull(EvoType1TimeTypesFinder.findOne(EvoType1TimeTypesFinder.rootEvo().eq(time)));
        assertNotNull(EvoType1TimeTypesFinder.findOneBypassCache(EvoType1TimeTypesFinder.rootEvo().eq(time)));
    }

    public void testType2Update()
    {
        final EvoTypesRoot root = this.createEvoTypesRoot('B', false, (byte) 1, "update");
        assertNull(EvoType2TxnTypesBFinder.findOneBypassCache(EvoType2TxnTypesBFinder.rootEvo().eq(root)));

        final EvoType2TxnTypes newTypes = EvoType2TxnTypesBFinder.findOneBypassCache(
                EvoType2TxnTypesBFinder.pk().charAttribute().eq('B').and(
                EvoType2TxnTypesBFinder.pk().booleanAttribute().eq(false)));
        assertNotNull(newTypes);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                newTypes.copyRootEvo(root);
                return null;
            }
        });

        assertNotNull(EvoType2TxnTypesBFinder.findOne(EvoType2TxnTypesBFinder.rootEvo().eq(root)));
        assertNotNull(EvoType2TxnTypesBFinder.findOneBypassCache(EvoType2TxnTypesBFinder.rootEvo().eq(root)));
    }

    public void testType3Update()
    {
        final EvoTypesRoot root = this.createEvoTypesRoot('B', false, (byte) 1, "update");
        assertNull(EvoType3TxnTypesBFinder.findOneBypassCache(EvoType3TxnTypesBFinder.rootEvo().eq(root)));

        final EvoType3TxnTypesB newTypes = EvoType3TxnTypesBFinder.findOneBypassCache(
                EvoType3TxnTypesBFinder.pk().charAttribute().eq('B').and(
                EvoType3TxnTypesBFinder.pk().booleanAttribute().eq(false)));
        assertNotNull(newTypes);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                newTypes.copyRootEvo(root);
                return null;
            }
        });

        assertNotNull(EvoType3TxnTypesBFinder.findOne(EvoType3TxnTypesBFinder.rootEvo().eq(root)));
        assertNotNull(EvoType3TxnTypesBFinder.findOneBypassCache(EvoType3TxnTypesBFinder.rootEvo().eq(root)));
    }

    public void testType1DatedUpdate()
    {
        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        final EvoTypesRoot root = this.createEvoTypesRoot('B', false, (byte) 1, "update");
        Operation op = EvoType1DatedTxnTypesFinder.rootEvo().eq(root).and(EvoType1DatedTxnTypesFinder.businessDate().eq(businessDate));
        assertNull(EvoType1DatedTxnTypesFinder.findOneBypassCache(op));

        final EvoType1DatedTxnTypes types = EvoType1DatedTxnTypesFinder.findOneBypassCache(
                EvoType1DatedTxnTypesFinder.pk().charAttribute().eq('B').and(
                EvoType1DatedTxnTypesFinder.pk().booleanAttribute().eq(false)).and(
                EvoType1DatedTxnTypesFinder.businessDate().eq(businessDate)));
        assertNotNull(types);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.copyRootEvo(root);
                return null;
            }

        });

        assertNotNull(EvoType1DatedTxnTypesFinder.findOne(op));
        assertNotNull(EvoType1DatedTxnTypesFinder.findOneBypassCache(op));
    }

    public void testType2DatedUpdate()
    {
        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        final EvoTypesRoot root = this.createEvoTypesRoot('B', false, (byte) 1, "update");
        Operation op = EvoType2DatedTxnTypesBFinder.rootEvo().eq(root).and(EvoType2DatedTxnTypesBFinder.businessDate().eq(businessDate));
        assertNull(EvoType2DatedTxnTypesBFinder.findOneBypassCache(op));

        final EvoType2DatedTxnTypesB types = EvoType2DatedTxnTypesBFinder.findOneBypassCache(
                EvoType2DatedTxnTypesBFinder.pk().charAttribute().eq('B').and(
                EvoType2DatedTxnTypesBFinder.pk().booleanAttribute().eq(false)).and(
                EvoType2DatedTxnTypesBFinder.businessDate().eq(businessDate)));
        assertNotNull(types);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.copyRootEvo(root);
                return null;
            }
        });

        assertNotNull(EvoType2DatedTxnTypesBFinder.findOne(op));
        assertNotNull(EvoType2DatedTxnTypesBFinder.findOneBypassCache(op));
    }

//    public void testType3DatedUpdate()
//    {
//        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
//        final EvoTypesRoot root = this.createEvoTypesRoot('B', false, (byte) 1, "update");
//        Operation op = EvoType3DatedTxnTypesBFinder.rootEvo().eq(root).and(EvoType3DatedTxnTypesBFinder.businessDate().eq(businessDate));
//        assertNull(EvoType3DatedTxnTypesBFinder.findOneBypassCache(op));
//
//        final EvoType3DatedTxnTypesB types = EvoType3DatedTxnTypesBFinder.findOneBypassCache(
//                EvoType3DatedTxnTypesBFinder.pk().charAttribute().eq('B').and(
//                EvoType3DatedTxnTypesBFinder.pk().booleanAttribute().eq(false)).and(
//                EvoType3DatedTxnTypesBFinder.businessDate().eq(businessDate)));
//        assertNotNull(types);
//
//        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
//        {
//            public Object executeTransaction(MithraTransaction tx) throws Throwable
//            {
//                types.copyRootEvo(root);
//                return null;
//            }
//        });
//
//        assertNotNull(EvoType3DatedTxnTypesBFinder.findOne(op));
//        assertNotNull(EvoType3DatedTxnTypesBFinder.findOneBypassCache(op));
//    }

    public void testType1DatedUpdateUntil()
    {
        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        final EvoTypesRoot root = this.createEvoTypesRoot('B', false, (byte) 1, "update");
        Operation op = EvoType1DatedTxnTypesFinder.rootEvo().eq(root).and(EvoType1DatedTxnTypesFinder.businessDate().eq(businessDate));
        assertNull(EvoType1DatedTxnTypesFinder.findOneBypassCache(op));

        final EvoType1DatedTxnTypes types = EvoType1DatedTxnTypesFinder.findOneBypassCache(
                EvoType1DatedTxnTypesFinder.pk().charAttribute().eq('B').and(
                EvoType1DatedTxnTypesFinder.pk().booleanAttribute().eq(false)).and(
                EvoType1DatedTxnTypesFinder.businessDate().eq(businessDate)));
        assertNotNull(types);

        final Timestamp newBusinessDateEnd = Timestamp.valueOf("2007-09-12 18:30:00.0");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.copyRootEvoUntil(root, newBusinessDateEnd);
                return null;
            }
        });

        assertEquals(newBusinessDateEnd, EvoType1DatedTxnTypesFinder.findOne(op).getBusinessDateTo());
        assertEquals(newBusinessDateEnd, EvoType1DatedTxnTypesFinder.findOneBypassCache(op).getBusinessDateTo());
    }

    public void testType2DatedUpdateUntil()
    {
        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        final EvoTypesRoot root = this.createEvoTypesRoot('B', false, (byte) 1, "update");
        Operation op = EvoType2DatedTxnTypesBFinder.rootEvo().eq(root).and(EvoType2DatedTxnTypesBFinder.businessDate().eq(businessDate));
        assertNull(EvoType2DatedTxnTypesBFinder.findOneBypassCache(op));

        final EvoType2DatedTxnTypesB types = EvoType2DatedTxnTypesBFinder.findOneBypassCache(
                EvoType2DatedTxnTypesBFinder.pk().charAttribute().eq('B').and(
                EvoType2DatedTxnTypesBFinder.pk().booleanAttribute().eq(false)).and(
                EvoType2DatedTxnTypesBFinder.businessDate().eq(businessDate)));
        assertNotNull(types);

        final Timestamp newBusinessDateEnd = Timestamp.valueOf("2007-09-12 18:30:00.0");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.copyRootEvoUntil(root, newBusinessDateEnd);
                return null;
            }
        });

        assertEquals(newBusinessDateEnd, EvoType2DatedTxnTypesBFinder.findOne(op).getBusinessDateTo());
        assertEquals(newBusinessDateEnd, EvoType2DatedTxnTypesBFinder.findOneBypassCache(op).getBusinessDateTo());
    }

//    public void testType3DatedUpdateUntil()
//    {
//        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
//        final EvoTypesRoot root = this.createEvoTypesRoot('B', false, (byte) 1, "update");
//        Operation op = EvoType3DatedTxnTypesBFinder.rootEvo().eq(root).and(EvoType3DatedTxnTypesBFinder.businessDate().eq(businessDate));
//        assertNull(EvoType3DatedTxnTypesBFinder.findOneBypassCache(op));
//
//        final EvoType3DatedTxnTypesB types = EvoType3DatedTxnTypesBFinder.findOneBypassCache(
//                EvoType3DatedTxnTypesBFinder.pk().charAttribute().eq('B').and(
//                EvoType3DatedTxnTypesBFinder.pk().booleanAttribute().eq(false)).and(
//                EvoType3DatedTxnTypesBFinder.businessDate().eq(businessDate)));
//        assertNotNull(types);
//
//        final Timestamp newBusinessDateEnd = Timestamp.valueOf("2007-09-12 18:30:00.0");
//        MithraTransaction tx = MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
//        {
//            public Object executeTransaction(MithraTransaction tx) throws Throwable
//            {
//                types.copyRootEvoUntil(root, newBusinessDateEnd);
//                return null;
//            }
//        });
//
//        assertEquals(newBusinessDateEnd, EvoType3DatedTxnTypesBFinder.findOne(op).getBusinessDateTo());
//        assertEquals(newBusinessDateEnd, EvoType3DatedTxnTypesBFinder.findOneBypassCache(op).getBusinessDateTo());
//    }

    public void testType1DatedIncrement()
    {
        final BigDecimal value1= new BigDecimal("144444444444444.444");
        final BigDecimal value2= new BigDecimal("244444444444444.444");
        final BigDecimal value3= new BigDecimal("34444444.44");
        Operation op = EvoType1DatedTxnTypesFinder.pk().charAttribute().eq('B').and(
            EvoType1DatedTxnTypesFinder.pk().booleanAttribute().eq(false)).and(
            EvoType1DatedTxnTypesFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")));

        final EvoType1DatedTxnTypes types = EvoType1DatedTxnTypesFinder.findOneBypassCache(op);
        assertEquals(1.1d, types.getRootEvo().getDoubleAttribute());
        assertEquals(value1, types.getRootEvo().getBigDecimalAttribute());
        assertEquals(value2, types.getRootEvo().getNestedEvo().getBigDecimalAttribute());
        assertEquals(value3, types.getRootEvo().getNestedEvo().getLeafEvo().getBigDecimalAttribute());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.getRootEvo().incrementDoubleAttribute(6.0d);
                types.getRootEvo().incrementBigDecimalAttribute(value1);
                types.getRootEvo().getNestedEvo().incrementBigDecimalAttribute(value2);
                types.getRootEvo().getNestedEvo().getLeafEvo().incrementBigDecimalAttribute(value3);
                return null;
            }
        });

        BigDecimal incrementedValue1 = new BigDecimal("288888888888888.888");
        BigDecimal incrementedValue2 = new BigDecimal("488888888888888.888");
        BigDecimal incrementedValue3 = new BigDecimal("68888888.88");
        assertEquals(7.1d, EvoType1DatedTxnTypesFinder.findOne(op).getRootEvo().getDoubleAttribute());
        assertEquals(7.1d, EvoType1DatedTxnTypesFinder.findOneBypassCache(op).getRootEvo().getDoubleAttribute());

        assertEquals(incrementedValue1, EvoType1DatedTxnTypesFinder.findOne(op).getRootEvo().getBigDecimalAttribute());
        assertEquals(incrementedValue1, EvoType1DatedTxnTypesFinder.findOneBypassCache(op).getRootEvo().getBigDecimalAttribute());
        assertEquals(incrementedValue2, EvoType1DatedTxnTypesFinder.findOne(op).getRootEvo().getNestedEvo().getBigDecimalAttribute());
        assertEquals(incrementedValue2, EvoType1DatedTxnTypesFinder.findOneBypassCache(op).getRootEvo().getNestedEvo().getBigDecimalAttribute());
        assertEquals(incrementedValue3, EvoType1DatedTxnTypesFinder.findOne(op).getRootEvo().getNestedEvo().getLeafEvo().getBigDecimalAttribute());
        assertEquals(incrementedValue3, EvoType1DatedTxnTypesFinder.findOneBypassCache(op).getRootEvo().getNestedEvo().getLeafEvo().getBigDecimalAttribute());
    }

    public void testType2DatedIncrement()
    {
        Operation op = EvoType2DatedTxnTypesBFinder.pk().charAttribute().eq('B').and(
            EvoType2DatedTxnTypesBFinder.pk().booleanAttribute().eq(false)).and(
            EvoType2DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")));
        final EvoType2DatedTxnTypesB types = EvoType2DatedTxnTypesBFinder.findOneBypassCache(op);
        assertEquals(1.1d, types.getRootEvo().getDoubleAttribute());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.getRootEvo().incrementDoubleAttribute(6.0d);
                return null;
            }
        });

        assertEquals(7.1d, EvoType2DatedTxnTypesBFinder.findOne(op).getRootEvo().getDoubleAttribute());
        assertEquals(7.1d, EvoType2DatedTxnTypesBFinder.findOneBypassCache(op).getRootEvo().getDoubleAttribute());
    }

//    public void testType3DatedIncrement()
//    {
//        Operation op = EvoType3DatedTxnTypesBFinder.pk().charAttribute().eq('B').and(
//            EvoType3DatedTxnTypesBFinder.pk().booleanAttribute().eq(false)).and(
//            EvoType3DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")));
//        final EvoType3DatedTxnTypesB types = EvoType3DatedTxnTypesBFinder.findOneBypassCache(op);
//        assertEquals(1.1d, types.getRootEvo().getDoubleAttribute());
//
//        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
//        {
//            public Object executeTransaction(MithraTransaction tx) throws Throwable
//            {
//                types.getRootEvo().incrementDoubleAttribute(6.0d);
//                return null;
//            }
//        });
//
//        assertEquals(7.1d, EvoType3DatedTxnTypesBFinder.findOne(op).getRootEvo().getDoubleAttribute());
//        assertEquals(7.1d, EvoType3DatedTxnTypesBFinder.findOneBypassCache(op).getRootEvo().getDoubleAttribute());
//    }

    public void testType1DatedIncrementUntil()
    {
        final BigDecimal value1= new BigDecimal("144444444444444.444");
        final BigDecimal value2= new BigDecimal("244444444444444.444");
        final BigDecimal value3= new BigDecimal("34444444.44");
        Operation op = EvoType1DatedTxnTypesFinder.pk().charAttribute().eq('B').and(
            EvoType1DatedTxnTypesFinder.pk().booleanAttribute().eq(false)).and(
            EvoType1DatedTxnTypesFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")));
        final EvoType1DatedTxnTypes types = EvoType1DatedTxnTypesFinder.findOneBypassCache(op);
        assertEquals(1.1d, types.getRootEvo().getDoubleAttribute());
        assertEquals(value1, types.getRootEvo().getBigDecimalAttribute());
        assertEquals(value2, types.getRootEvo().getNestedEvo().getBigDecimalAttribute());
        assertEquals(value3, types.getRootEvo().getNestedEvo().getLeafEvo().getBigDecimalAttribute());

        final Timestamp newBusinessDateEnd = Timestamp.valueOf("2007-09-12 18:30:00.0");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.getRootEvo().incrementDoubleAttributeUntil(6.0d, newBusinessDateEnd);
                types.getRootEvo().incrementBigDecimalAttributeUntil(value1, newBusinessDateEnd);
                types.getRootEvo().getNestedEvo().incrementBigDecimalAttributeUntil(value2, newBusinessDateEnd);
                types.getRootEvo().getNestedEvo().getLeafEvo().incrementBigDecimalAttributeUntil(value3, newBusinessDateEnd);
                return null;
            }
        });

        assertEquals(7.1d, EvoType1DatedTxnTypesFinder.findOne(op).getRootEvo().getDoubleAttribute());
        assertEquals(newBusinessDateEnd, EvoType1DatedTxnTypesFinder.findOne(op).getBusinessDateTo());
        assertEquals(7.1d, EvoType1DatedTxnTypesFinder.findOneBypassCache(op).getRootEvo().getDoubleAttribute());
        assertEquals(newBusinessDateEnd, EvoType1DatedTxnTypesFinder.findOneBypassCache(op).getBusinessDateTo());

        BigDecimal incrementedValue1 = new BigDecimal("288888888888888.888");
        BigDecimal incrementedValue2 = new BigDecimal("488888888888888.888");
        BigDecimal incrementedValue3 = new BigDecimal("68888888.88");
        assertEquals(incrementedValue1, EvoType1DatedTxnTypesFinder.findOneBypassCache(op).getRootEvo().getBigDecimalAttribute());
        assertEquals(incrementedValue2, EvoType1DatedTxnTypesFinder.findOneBypassCache(op).getRootEvo().getNestedEvo().getBigDecimalAttribute());
        assertEquals(incrementedValue3, EvoType1DatedTxnTypesFinder.findOneBypassCache(op).getRootEvo().getNestedEvo().getLeafEvo().getBigDecimalAttribute());

        Operation op2 = EvoType1DatedTxnTypesFinder.pk().charAttribute().eq('B').and(
            EvoType1DatedTxnTypesFinder.pk().booleanAttribute().eq(false)).and(
            EvoType1DatedTxnTypesFinder.businessDate().eq(Timestamp.valueOf("2007-09-13 00:00:00.0")));

        final EvoType1DatedTxnTypes types2 = EvoType1DatedTxnTypesFinder.findOneBypassCache(op2);
        assertEquals(value1, types2.getRootEvo().getBigDecimalAttribute());
        assertEquals(value2, types2.getRootEvo().getNestedEvo().getBigDecimalAttribute());
        assertEquals(value3, types2.getRootEvo().getNestedEvo().getLeafEvo().getBigDecimalAttribute());
    }

    public void testType2DatedIncrementUntil()
    {
        Operation op = EvoType2DatedTxnTypesBFinder.pk().charAttribute().eq('B').and(
            EvoType2DatedTxnTypesBFinder.pk().booleanAttribute().eq(false)).and(
            EvoType2DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")));
        final EvoType2DatedTxnTypesB types = EvoType2DatedTxnTypesBFinder.findOneBypassCache(op);
        assertEquals(1.1d, types.getRootEvo().getDoubleAttribute());

        final Timestamp newBusinessDateEnd = Timestamp.valueOf("2007-09-12 18:30:00.0");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.getRootEvo().incrementDoubleAttributeUntil(6.0d, newBusinessDateEnd);
                return null;
            }
        });

        assertEquals(7.1d, EvoType2DatedTxnTypesBFinder.findOne(op).getRootEvo().getDoubleAttribute());
        assertEquals(newBusinessDateEnd, EvoType2DatedTxnTypesBFinder.findOne(op).getBusinessDateTo());
        assertEquals(7.1d, EvoType2DatedTxnTypesBFinder.findOneBypassCache(op).getRootEvo().getDoubleAttribute());
        assertEquals(newBusinessDateEnd, EvoType2DatedTxnTypesBFinder.findOneBypassCache(op).getBusinessDateTo());
    }

//    public void testType3DatedIncrementUntil()
//    {
//        Operation op = EvoType3DatedTxnTypesBFinder.pk().charAttribute().eq('B').and(
//            EvoType3DatedTxnTypesBFinder.pk().booleanAttribute().eq(false)).and(
//            EvoType3DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")));
//        final EvoType3DatedTxnTypesB types = EvoType3DatedTxnTypesBFinder.findOneBypassCache(op);
//        assertEquals(1.1d, types.getRootEvo().getDoubleAttribute());
//
//        final Timestamp newBusinessDateEnd = Timestamp.valueOf("2007-09-12 18:30:00.0");
//        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
//        {
//            public Object executeTransaction(MithraTransaction tx) throws Throwable
//            {
//                types.getRootEvo().incrementDoubleAttributeUntil(6.0d, newBusinessDateEnd);
//                return null;
//            }
//        });
//
//        assertEquals(7.1d, EvoType3DatedTxnTypesBFinder.findOne(op).getRootEvo().getDoubleAttribute());
//        assertEquals(newBusinessDateEnd, EvoType3DatedTxnTypesBFinder.findOne(op).getBusinessDateTo());
//        assertEquals(7.1d, EvoType3DatedTxnTypesBFinder.findOneBypassCache(op).getRootEvo().getDoubleAttribute());
//        assertEquals(newBusinessDateEnd, EvoType3DatedTxnTypesBFinder.findOneBypassCache(op).getBusinessDateTo());
//    }

    protected EvoTypesRoot createEvoTypesRoot(char type, boolean boolean0, byte byte0, String string0)
    {
        Date now = new Date();
        Timestamp nowTimestamp = new Timestamp(now.getTime());
        return new EvoTypesRoot.Builder(type).withBoolean(boolean0).withByte(byte0).withDate(null).withDouble(-1.1d)
            .withFloat(-2.2f).withInt(-3).withLong(-4l).withShort((short) -5).withString(string0).withTimestamp(nowTimestamp).build();
    }

    protected EvoTypesRootWithTime createEvoTypesRootWithTime(char type, boolean boolean0, byte byte0, String string0, Time time)
    {
        Date now = new Date();
        Timestamp nowTimestamp = new Timestamp(now.getTime());
        return new EvoTypesRootWithTime.Builder(type).withBoolean(boolean0).withByte(byte0).withDate(null).withTime(time).withDouble(-1.1d)
            .withFloat(-2.2f).withInt(-3).withLong(-4l).withShort((short) -5).withString(string0).withTimestamp(nowTimestamp).build();
    }

    public void testType1BatchUpdate()
    {
        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "batch");
        Operation op = EvoType1TxnTypesFinder.rootEvo().eq(root);
        assertEquals(0, EvoType1TxnTypesFinder.findMany(op).size());
        final EvoType1TxnTypesList typesList = EvoType1TxnTypesFinder.findMany(EvoType1TxnTypesFinder.all());
        assertEquals(4, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                typesList.copyRootEvo(root);
                return null;
            }
        });

        assertEquals(4, EvoType1TxnTypesFinder.findMany(op).size());
        assertEquals(4, EvoType1TxnTypesFinder.findManyBypassCache(op).size());

        final EvoTypesRootWithTime root2 = this.createEvoTypesRootWithTime('A', false, (byte) 1, "batch", Time.withMillis(10, 11, 12, 13));
        Operation op2 = EvoType1TimeTypesFinder.rootEvo().eq(root2);
        assertEquals(0, EvoType1TimeTypesFinder.findMany(op2).size());
        final EvoType1TimeTypesList typesList2 = EvoType1TimeTypesFinder.findMany(EvoType1TimeTypesFinder.all());
        assertEquals(2, typesList2.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                typesList2.copyRootEvo(root2);
                return null;
            }
        });

        assertEquals(2, EvoType1TimeTypesFinder.findMany(op2).size());
        assertEquals(2, EvoType1TimeTypesFinder.findManyBypassCache(op2).size());
    }

    public void testType2BatchUpdate()
    {
        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "batch");
        Operation op = EvoType2TxnTypesAFinder.rootEvo().eq(root);
        assertEquals(0, EvoType2TxnTypesAFinder.findMany(op).size());
        final EvoType2TxnTypesAList typesList = EvoType2TxnTypesAFinder.findMany(EvoType2TxnTypesAFinder.all());
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                typesList.copyRootEvo(root);
                return null;
            }
        });

        assertEquals(2, EvoType2TxnTypesAFinder.findMany(op).size());
        assertEquals(2, EvoType2TxnTypesAFinder.findManyBypassCache(op).size());
    }

    public void testType3BatchUpdate()
    {
        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "batch");
        Operation op = EvoType3TxnTypesAFinder.rootEvo().eq(root);
        assertEquals(0, EvoType3TxnTypesAFinder.findMany(op).size());
        final EvoType3TxnTypesAList typesList = EvoType3TxnTypesAFinder.findMany(EvoType3TxnTypesAFinder.all());
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                typesList.copyRootEvo(root);
                return null;
            }
        });

        assertEquals(2, EvoType3TxnTypesAFinder.findMany(op).size());
        assertEquals(2, EvoType3TxnTypesAFinder.findManyBypassCache(op).size());
    }

    public void testType1DatedBatchUpdate()
    {
        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "batch");
        Operation op = EvoType1DatedTxnTypesFinder.rootEvo().eq(root).and(EvoType1DatedTxnTypesFinder.businessDate().eq(businessDate));
        assertEquals(0, EvoType1DatedTxnTypesFinder.findMany(op).size());
        final EvoType1DatedTxnTypesList typesList = EvoType1DatedTxnTypesFinder.findMany(EvoType1DatedTxnTypesFinder.businessDate().eq(businessDate));
        assertEquals(4, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                typesList.copyRootEvo(root);
                return null;
            }
        });

        assertEquals(4, EvoType1DatedTxnTypesFinder.findMany(op).size());
        assertEquals(4, EvoType1DatedTxnTypesFinder.findManyBypassCache(op).size());
    }

    public void testType2DatedBatchUpdate()
    {
        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "batch");
        Operation op = EvoType2DatedTxnTypesAFinder.rootEvo().eq(root).and(EvoType2DatedTxnTypesAFinder.businessDate().eq(businessDate));
        assertEquals(0, EvoType2DatedTxnTypesAFinder.findMany(op).size());
        final EvoType2DatedTxnTypesAList typesList = EvoType2DatedTxnTypesAFinder.findMany(EvoType2DatedTxnTypesAFinder.businessDate().eq(businessDate));
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                typesList.copyRootEvo(root);
                return null;
            }
        });

        assertEquals(2, EvoType2DatedTxnTypesAFinder.findMany(op).size());
        assertEquals(2, EvoType2DatedTxnTypesAFinder.findManyBypassCache(op).size());
    }

//    public void testType3DatedBatchUpdate()
//    {
//        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
//        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "batch");
//        Operation op = EvoType3DatedTxnTypesAFinder.rootEvo().eq(root).and(EvoType3DatedTxnTypesAFinder.businessDate().eq(businessDate));
//        assertEquals(0, EvoType3DatedTxnTypesAFinder.findMany(op).size());
//        final EvoType3DatedTxnTypesAList typesList = EvoType3DatedTxnTypesAFinder.findMany(EvoType3DatedTxnTypesAFinder.businessDate().eq(businessDate));
//        assertEquals(2, typesList.size());
//
//        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
//        {
//            public Object executeTransaction(MithraTransaction tx) throws Throwable
//            {
//                typesList.copyRootEvo(root);
//                return null;
//            }
//        });
//
//        assertEquals(2, EvoType3DatedTxnTypesAFinder.findMany(op).size());
//        assertEquals(2, EvoType3DatedTxnTypesAFinder.findManyBypassCache(op).size());
//    }

    public void testType1MultiUpdate()
    {
        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "multi");
        Operation op = EvoType1TxnTypesFinder.rootEvo().eq(root);
        assertEquals(0, EvoType1TxnTypesFinder.findMany(op).size());
        final EvoType1TxnTypesList typesList = EvoType1TxnTypesFinder.findMany(EvoType1TxnTypesFinder.all());
        assertEquals(4, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (EvoType1TxnTypes types : typesList)
                {
                    types.copyRootEvo(root);
                }
                return null;
            }
        });

        assertEquals(4, EvoType1TxnTypesFinder.findMany(op).size());
        assertEquals(4, EvoType1TxnTypesFinder.findManyBypassCache(op).size());

        final EvoTypesRootWithTime time = this.createEvoTypesRootWithTime('A', false, (byte) 1, "multi", Time.withMillis(2, 4, 6, 8));
        Operation op2 = EvoType1TimeTypesFinder.rootEvo().eq(time);
        assertEquals(0, EvoType1TimeTypesFinder.findMany(op2).size());
        final EvoType1TimeTypesList typesList2 = EvoType1TimeTypesFinder.findMany(EvoType1TimeTypesFinder.all());
        assertEquals(2, typesList2.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (EvoType1TimeTypes types : typesList2)
                {
                    types.copyRootEvo(time);
                }
                return null;
            }
        });

        assertEquals(2, EvoType1TimeTypesFinder.findMany(op2).size());
        assertEquals(2, EvoType1TimeTypesFinder.findManyBypassCache(op2).size());
    }

    public void testType2MultiUpdate()
    {
        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "multi");
        Operation op = EvoType2TxnTypesAFinder.rootEvo().eq(root);
        assertEquals(0, EvoType2TxnTypesAFinder.findMany(op).size());
        final EvoType2TxnTypesAList typesList = EvoType2TxnTypesAFinder.findMany(EvoType2TxnTypesAFinder.all());
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (EvoType2TxnTypes types : typesList)
                {
                    types.copyRootEvo(root);
                }
                return null;
            }
        });

        assertEquals(2, EvoType2TxnTypesAFinder.findMany(op).size());
        assertEquals(2, EvoType2TxnTypesAFinder.findManyBypassCache(op).size());
    }

    public void testType3MultiUpdate()
    {
        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "multi");
        Operation op = EvoType3TxnTypesAFinder.rootEvo().eq(root);
        assertEquals(0, EvoType3TxnTypesAFinder.findMany(op).size());
        final EvoType3TxnTypesAList typesList = EvoType3TxnTypesAFinder.findMany(EvoType3TxnTypesAFinder.all());
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (EvoType3TxnTypesA types : typesList)
                {
                    types.copyRootEvo(root);
                }
                return null;
            }
        });

        assertEquals(2, EvoType3TxnTypesAFinder.findMany(op).size());
        assertEquals(2, EvoType3TxnTypesAFinder.findManyBypassCache(op).size());
    }

    public void testType1DatedMultiUpdate()
    {
        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "multi");
        Operation op = EvoType1DatedTxnTypesFinder.rootEvo().eq(root).and(EvoType1DatedTxnTypesFinder.businessDate().eq(businessDate));
        assertEquals(0, EvoType1DatedTxnTypesFinder.findMany(op).size());
        final EvoType1DatedTxnTypesList typesList = EvoType1DatedTxnTypesFinder.findMany(EvoType1DatedTxnTypesFinder.businessDate().eq(businessDate));
        assertEquals(4, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (EvoType1DatedTxnTypes types : typesList)
                {
                    types.copyRootEvo(root);
                }
                return null;
            }
        });

        assertEquals(4, EvoType1DatedTxnTypesFinder.findMany(op).size());
        assertEquals(4, EvoType1DatedTxnTypesFinder.findManyBypassCache(op).size());
    }

    public void testType2DatedMultiUpdate()
    {
        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "multi");
        Operation op = EvoType2DatedTxnTypesAFinder.rootEvo().eq(root).and(EvoType2DatedTxnTypesAFinder.businessDate().eq(businessDate));
        assertEquals(0, EvoType2DatedTxnTypesAFinder.findMany(op).size());
        final EvoType2DatedTxnTypesAList typesList = EvoType2DatedTxnTypesAFinder.findMany(EvoType2DatedTxnTypesAFinder.businessDate().eq(businessDate));
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (EvoType2DatedTxnTypes types : typesList)
                {
                    types.copyRootEvo(root);
                }
                return null;
            }
        });

        assertEquals(2, EvoType2DatedTxnTypesAFinder.findMany(op).size());
        assertEquals(2, EvoType2DatedTxnTypesAFinder.findManyBypassCache(op).size());
    }

//    public void testType3DatedMultiUpdate()
//    {
//        final Timestamp businessDate = Timestamp.valueOf("2007-09-10 00:00:00.0");
//        final EvoTypesRoot root = this.createEvoTypesRoot('A', false, (byte) 1, "multi");
//        Operation op = EvoType3DatedTxnTypesAFinder.rootEvo().eq(root).and(EvoType3DatedTxnTypesAFinder.businessDate().eq(businessDate));
//        assertEquals(0, EvoType3DatedTxnTypesAFinder.findMany(op).size());
//        final EvoType3DatedTxnTypesAList typesList = EvoType3DatedTxnTypesAFinder.findMany(EvoType3DatedTxnTypesAFinder.businessDate().eq(businessDate));
//        assertEquals(2, typesList.size());
//
//        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
//        {
//            public Object executeTransaction(MithraTransaction tx) throws Throwable
//            {
//                for (EvoType3DatedTxnTypesA types : typesList)
//                {
//                    types.copyRootEvo(root);
//                }
//                return null;
//            }
//        });
//
//        assertEquals(2, EvoType3DatedTxnTypesAFinder.findMany(op).size());
//        assertEquals(2, EvoType3DatedTxnTypesAFinder.findManyBypassCache(op).size());
//    }

    public void testType1Delete()
    {
        Operation op = EvoType1TxnTypesFinder.pk().charAttribute().eq('B').and(EvoType1TxnTypesFinder.pk().booleanAttribute().eq(false));
        final EvoType1TxnTypes types = EvoType1TxnTypesFinder.findOneBypassCache(op);
        assertNotNull(types);

        types.delete();

        assertNull(EvoType1TxnTypesFinder.findOne(op));
        assertNull(EvoType1TxnTypesFinder.findOneBypassCache(op));

        Operation op2 = EvoType1TimeTypesFinder.pk().charAttribute().eq('B').and(EvoType1TimeTypesFinder.pk().timeAttribute().eq(Time.withMillis(1, 25, 30, 0)));
        final EvoType1TimeTypes types2 = EvoType1TimeTypesFinder.findOneBypassCache(op2);
        assertNotNull(types2);

        types2.delete();

        assertNull(EvoType1TimeTypesFinder.findOne(op2));
        assertNull(EvoType1TimeTypesFinder.findOneBypassCache(op2));
    }

    public void testType2Delete()
    {
        Operation op = EvoType2TxnTypesBFinder.pk().charAttribute().eq('B').and(EvoType2TxnTypesBFinder.pk().booleanAttribute().eq(false));
        final EvoType2TxnTypesB types = EvoType2TxnTypesBFinder.findOneBypassCache(op);
        assertNotNull(types);

        types.delete();

        assertNull(EvoType2TxnTypesBFinder.findOne(op));
        assertNull(EvoType2TxnTypesBFinder.findOneBypassCache(op));
    }

    public void testType3Delete()
    {
        Operation op = EvoType3TxnTypesBFinder.pk().charAttribute().eq('B').and(EvoType3TxnTypesBFinder.pk().booleanAttribute().eq(false));
        final EvoType3TxnTypesB types = EvoType3TxnTypesBFinder.findOneBypassCache(op);
        assertNotNull(types);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                types.delete();
                return null;
            }
        });

        assertNull(EvoType3TxnTypesBFinder.findOne(op));
        assertNull(EvoType3TxnTypesBFinder.findOneBypassCache(op));
    }

    public void testType1BatchDelete()
    {
        final EvoType1TxnTypesList typesList = EvoType1TxnTypesFinder.findMany(EvoType1TxnTypesFinder.all());
        assertEquals(4, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (EvoType1TxnTypes types : typesList)
                {
                    types.delete();
                }
                return null;
            }
        });

        assertEquals(0, EvoType1TxnTypesFinder.findMany(EvoType1TxnTypesFinder.all()).size());
        assertEquals(0, EvoType1TxnTypesFinder.findManyBypassCache(EvoType1TxnTypesFinder.all()).size());

        final EvoType1TimeTypesList typesList2 = EvoType1TimeTypesFinder.findMany(EvoType1TimeTypesFinder.all());
        assertEquals(2, typesList2.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (EvoType1TimeTypes types : typesList2)
                {
                    types.delete();
                }
                return null;
            }
        });

        assertEquals(0, EvoType1TimeTypesFinder.findMany(EvoType1TimeTypesFinder.all()).size());
        assertEquals(0, EvoType1TimeTypesFinder.findManyBypassCache(EvoType1TimeTypesFinder.all()).size());
    }

    public void testType2BatchDelete()
    {
        final EvoType2TxnTypesBList typesList = EvoType2TxnTypesBFinder.findMany(EvoType2TxnTypesBFinder.all());
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (EvoType2TxnTypesB types : typesList)
                {
                    types.delete();
                }
                return null;
            }
        });

        assertEquals(0, EvoType2TxnTypesBFinder.findMany(EvoType2TxnTypesBFinder.all()).size());
        assertEquals(0, EvoType2TxnTypesBFinder.findManyBypassCache(EvoType2TxnTypesBFinder.all()).size());
    }

    public void testType3BatchDelete()
    {
        final EvoType3TxnTypesBList typesList = EvoType3TxnTypesBFinder.findMany(EvoType3TxnTypesBFinder.all());
        assertEquals(2, typesList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (EvoType3TxnTypesB types : typesList)
                {
                    types.delete();
                }
                return null;
            }
        });

        assertEquals(0, EvoType3TxnTypesBFinder.findMany(EvoType3TxnTypesBFinder.all()).size());
        assertEquals(0, EvoType3TxnTypesBFinder.findManyBypassCache(EvoType3TxnTypesBFinder.all()).size());
    }

    public void testType1OperationBasedDelete()
    {
        final EvoType1TxnTypesList typesList = EvoType1TxnTypesFinder.findMany(EvoType1TxnTypesFinder.all());
        assertEquals(4, typesList.size());

        typesList.deleteAll();

        assertEquals(0, EvoType1TxnTypesFinder.findMany(EvoType1TxnTypesFinder.all()).size());
        assertEquals(0, EvoType1TxnTypesFinder.findManyBypassCache(EvoType1TxnTypesFinder.all()).size());

        final EvoType1TimeTypesList typesList2 = EvoType1TimeTypesFinder.findMany(EvoType1TimeTypesFinder.all());
        assertEquals(2, typesList2.size());

        typesList2.deleteAll();

        assertEquals(0, EvoType1TimeTypesFinder.findMany(EvoType1TimeTypesFinder.all()).size());
        assertEquals(0, EvoType1TimeTypesFinder.findManyBypassCache(EvoType1TimeTypesFinder.all()).size());
    }

    public void testType2OperationBasedDelete()
    {
        final EvoType2TxnTypesBList typesList = EvoType2TxnTypesBFinder.findMany(EvoType2TxnTypesBFinder.all());
        assertEquals(2, typesList.size());

        typesList.deleteAll();

        assertEquals(0, EvoType2TxnTypesBFinder.findMany(EvoType2TxnTypesBFinder.all()).size());
        assertEquals(0, EvoType2TxnTypesBFinder.findManyBypassCache(EvoType2TxnTypesBFinder.all()).size());
    }

    public void testType3OperationBasedDelete()
    {
        final EvoType3TxnTypesBList typesList = EvoType3TxnTypesBFinder.findMany(EvoType3TxnTypesBFinder.all());
        assertEquals(2, typesList.size());

        typesList.deleteAll();

        assertEquals(0, EvoType3TxnTypesBFinder.findMany(EvoType3TxnTypesBFinder.all()).size());
        assertEquals(0, EvoType3TxnTypesBFinder.findManyBypassCache(EvoType3TxnTypesBFinder.all()).size());
    }

    public void testType1ReadOnlyPolymorphicEvo()
    {
        EvoType1ReadOnlyTypes readOnlyTypes = EvoType1ReadOnlyTypesFinder.findOneBypassCache(
            EvoType1ReadOnlyTypesFinder.pk().charAttribute().eq('A').and(
            EvoType1ReadOnlyTypesFinder.pk().booleanAttribute().eq(true)));
        EvoTypesRoot readOnlyRootEvo = readOnlyTypes.getRootEvo();
        EvoTypesNested readOnlyNestedEvo = readOnlyRootEvo.getNestedEvo();
        EvoTypesLeaf readOnlyLeafEvo = readOnlyNestedEvo.getLeafEvo();
        assertTrue(readOnlyRootEvo instanceof EvoTypesRootA);
        assertTrue(readOnlyNestedEvo instanceof EvoTypesNestedA);
        assertTrue(readOnlyLeafEvo instanceof EvoTypesLeafA);

        try
        {
            readOnlyRootEvo.setCharAttribute('B');
            readOnlyNestedEvo.setCharAttribute('B');
            readOnlyLeafEvo.setCharAttribute('B');
            fail("Do not allow mutation of persisted read-only objects.");
        }
        catch (MithraBusinessException mbe)
        {
            assertTrue(readOnlyRootEvo instanceof EvoTypesRootA);
            assertTrue(readOnlyNestedEvo instanceof EvoTypesNestedA);
            assertTrue(readOnlyLeafEvo instanceof EvoTypesLeafA);
        }

        EvoType1ReadOnlyTypes newReadOnlyTypes = new EvoType1ReadOnlyTypesA();
        newReadOnlyTypes.copyRootEvo(this.createPolymorphicEvoTypesRootA());
        assertTrue(newReadOnlyTypes.getRootEvo() instanceof EvoTypesRootA);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedA);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);

        newReadOnlyTypes.getRootEvo().setCharAttribute('B');
        newReadOnlyTypes.getRootEvo().getNestedEvo().setCharAttribute('B');
        newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo().setCharAttribute('B');
        assertTrue(newReadOnlyTypes.getRootEvo() instanceof EvoTypesRootB);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedB);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafB);
    }

    public void testType1TxnPolymorphicEvo()
    {
        EvoType1TxnTypes txnTypes = EvoType1TxnTypesFinder.findOneBypassCache(
            EvoType1TxnTypesFinder.pk().charAttribute().eq('B').and(
            EvoType1TxnTypesFinder.pk().booleanAttribute().eq(false)));
        EvoTypesRoot txnRootEvo = txnTypes.getRootEvo();
        EvoTypesNested txnNestedEvo = txnRootEvo.getNestedEvo();
        EvoTypesLeaf txnLeafEvo = txnNestedEvo.getLeafEvo();
        assertTrue(txnRootEvo instanceof EvoTypesRootB);
        assertTrue(txnNestedEvo instanceof EvoTypesNestedB);
        assertTrue(txnLeafEvo instanceof EvoTypesLeafB);

        txnRootEvo.setCharAttribute('A');
        txnNestedEvo.setCharAttribute('A');
        txnLeafEvo.setCharAttribute('A');
        assertTrue(txnTypes.getRootEvo() instanceof EvoTypesRootA);
        assertTrue(txnTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedA);
        assertTrue(txnTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);
    }

    public void testType1DatedReadOnlyPolymorphicEvo()
    {
        EvoType1DatedReadOnlyTypes readOnlyTypes = EvoType1DatedReadOnlyTypesFinder.findOneBypassCache(
            EvoType1DatedReadOnlyTypesFinder.pk().charAttribute().eq('A').and(
            EvoType1DatedReadOnlyTypesFinder.pk().booleanAttribute().eq(true)).and(
            EvoType1DatedReadOnlyTypesFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType1DatedReadOnlyTypesFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")))));
        EvoTypesLeaf readOnlyLeafEvo = readOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo();
        assertTrue(readOnlyLeafEvo instanceof EvoTypesLeafA);

        try
        {
            readOnlyLeafEvo.setCharAttribute('B');
            fail("Do not allow mutation of persisted read-only objects.");
        }
        catch (MithraBusinessException mbe)
        {
        }

        EvoType1DatedReadOnlyTypes newReadOnlyTypes = new EvoType1DatedReadOnlyTypesA(Timestamp.valueOf("2007-09-10 00:00:00.0"), Timestamp.valueOf("2007-10-15 00:00:00.0"));
        EvoTypesLeafA evoTypesLeafA = new EvoTypesLeafA();
        newReadOnlyTypes.getRootEvo().getNestedEvo().copyLeafEvo(evoTypesLeafA);
        assertNotNull(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo());
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);

        newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo().setCharAttribute('B');
        assertNotNull(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo());
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafB);
    }

    public void testType1DatedTxnPolymorphicEvo()
    {
        final EvoType1DatedTxnTypes txnTypes = EvoType1DatedTxnTypesFinder.findOneBypassCache(
            EvoType1DatedTxnTypesFinder.pk().charAttribute().eq('B').and(
            EvoType1DatedTxnTypesFinder.pk().booleanAttribute().eq(true)).and(
            EvoType1DatedTxnTypesFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType1DatedTxnTypesFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()))));
        final EvoTypesRoot txnRootEvo = txnTypes.getRootEvo();
        final EvoTypesNested txnNestedEvo = txnRootEvo.getNestedEvo();
        final EvoTypesLeaf txnLeafEvo = txnNestedEvo.getLeafEvo();
        assertTrue(txnRootEvo instanceof EvoTypesRootB);
        assertTrue(txnNestedEvo instanceof EvoTypesNestedB);
        assertTrue(txnLeafEvo instanceof EvoTypesLeafB);

        final EvoTypesRoot evoTypesRoot = this.createPolymorphicEvoTypesRootA();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                txnTypes.copyRootEvo(evoTypesRoot);
                return null;
            }
        });

        assertTrue(txnTypes.getRootEvo() instanceof EvoTypesRootA);
        assertTrue(txnTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedA);
        assertTrue(txnTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                txnRootEvo.setCharAttribute('B');
                txnNestedEvo.setCharAttribute('B');
                txnLeafEvo.setCharAttribute('B');
                return null;
            }
        });

        assertTrue(txnTypes.getRootEvo() instanceof EvoTypesRootB);
        assertTrue(txnTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedB);
        assertTrue(txnTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafB);
    }

    public void testType2ReadOnlyPolymorphicEvo()
    {
        EvoType2ReadOnlyTypes readOnlyTypes = EvoType2ReadOnlyTypesAFinder.findOneBypassCache(
            EvoType2ReadOnlyTypesAFinder.pk().charAttribute().eq('A').and(
            EvoType2ReadOnlyTypesAFinder.pk().booleanAttribute().eq(true)));
        EvoTypesRoot readOnlyRootEvo = readOnlyTypes.getRootEvo();
        EvoTypesNested readOnlyNestedEvo = readOnlyRootEvo.getNestedEvo();
        EvoTypesLeaf readOnlyLeafEvo = readOnlyNestedEvo.getLeafEvo();
        assertTrue(readOnlyRootEvo instanceof EvoTypesRootA);
        assertTrue(readOnlyNestedEvo instanceof EvoTypesNestedA);
        assertTrue(readOnlyLeafEvo instanceof EvoTypesLeafA);

        try
        {
            readOnlyRootEvo.setCharAttribute('B');
            readOnlyNestedEvo.setCharAttribute('B');
            readOnlyLeafEvo.setCharAttribute('B');
            fail("Do not allow mutation of persisted read-only objects.");
        }
        catch (MithraBusinessException mbe)
        {
            assertTrue(readOnlyRootEvo instanceof EvoTypesRootA);
            assertTrue(readOnlyNestedEvo instanceof EvoTypesNestedA);
            assertTrue(readOnlyLeafEvo instanceof EvoTypesLeafA);
        }

        EvoType2ReadOnlyTypesA newReadOnlyTypes = new EvoType2ReadOnlyTypesA();
        newReadOnlyTypes.copyRootEvo(this.createPolymorphicEvoTypesRootA());
        assertTrue(newReadOnlyTypes.getRootEvo() instanceof EvoTypesRootA);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedA);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);

        newReadOnlyTypes.getRootEvo().setCharAttribute('B');
        newReadOnlyTypes.getRootEvo().getNestedEvo().setCharAttribute('B');
        newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo().setCharAttribute('B');
        assertTrue(newReadOnlyTypes.getRootEvo() instanceof EvoTypesRootB);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedB);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafB);
    }

    public void testType2TxnPolymorphicEvo()
    {
        EvoType2TxnTypes txnTypes = EvoType2TxnTypesBFinder.findOneBypassCache(
            EvoType2TxnTypesBFinder.pk().charAttribute().eq('B').and(
            EvoType2TxnTypesBFinder.pk().booleanAttribute().eq(false)));
        EvoTypesRoot txnRootEvo = txnTypes.getRootEvo();
        EvoTypesNested txnNestedEvo = txnRootEvo.getNestedEvo();
        EvoTypesLeaf txnLeafEvo = txnNestedEvo.getLeafEvo();
        assertTrue(txnRootEvo instanceof EvoTypesRootB);
        assertTrue(txnNestedEvo instanceof EvoTypesNestedB);
        assertTrue(txnLeafEvo instanceof EvoTypesLeafB);

        txnRootEvo.setCharAttribute('A');
        txnNestedEvo.setCharAttribute('A');
        txnLeafEvo.setCharAttribute('A');
        assertTrue(txnTypes.getRootEvo() instanceof EvoTypesRootA);
        assertTrue(txnTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedA);
        assertTrue(txnTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);
    }

    public void testType2DatedReadOnlyPolymorphicEvo()
    {
        EvoType2DatedReadOnlyTypes readOnlyTypes = EvoType2DatedReadOnlyTypesAFinder.findOneBypassCache(
            EvoType2DatedReadOnlyTypesAFinder.pk().charAttribute().eq('A').and(
            EvoType2DatedReadOnlyTypesAFinder.pk().booleanAttribute().eq(true)).and(
            EvoType2DatedReadOnlyTypesAFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType2DatedReadOnlyTypesAFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")))));
        EvoTypesLeaf readOnlyLeafEvo = readOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo();
        assertTrue(readOnlyLeafEvo instanceof EvoTypesLeafA);

        try
        {
            readOnlyLeafEvo.setCharAttribute('B');
            fail("Do not allow mutation of persisted read-only objects.");
        }
        catch (MithraBusinessException mbe)
        {
        }

        EvoType2DatedReadOnlyTypes newReadOnlyTypes = new EvoType2DatedReadOnlyTypesA(Timestamp.valueOf("2007-09-10 00:00:00.0"), Timestamp.valueOf("2007-10-15 00:00:00.0"));
        EvoTypesLeafA evoTypesLeafA = new EvoTypesLeafA();
        newReadOnlyTypes.getRootEvo().getNestedEvo().copyLeafEvo(evoTypesLeafA);
        assertNotNull(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo());
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);

        newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo().setCharAttribute('B');
        assertNotNull(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo());
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafB);
    }

    public void testType2DatedTxnPolymorphicEvo()
    {
        final EvoType2DatedTxnTypes txnTypes = EvoType2DatedTxnTypesBFinder.findOneBypassCache(
            EvoType2DatedTxnTypesBFinder.pk().charAttribute().eq('B').and(
            EvoType2DatedTxnTypesBFinder.pk().booleanAttribute().eq(true)).and(
            EvoType2DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
            EvoType2DatedTxnTypesBFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()))));
        final EvoTypesRoot txnRootEvo = txnTypes.getRootEvo();
        final EvoTypesNested txnNestedEvo = txnRootEvo.getNestedEvo();
        final EvoTypesLeaf txnLeafEvo = txnNestedEvo.getLeafEvo();
        assertTrue(txnRootEvo instanceof EvoTypesRootB);
        assertTrue(txnNestedEvo instanceof EvoTypesNestedB);
        assertTrue(txnLeafEvo instanceof EvoTypesLeafB);

        final EvoTypesRoot evoTypesRoot = this.createPolymorphicEvoTypesRootA();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                txnTypes.copyRootEvo(evoTypesRoot);
                return null;
            }
        });

        assertTrue(txnTypes.getRootEvo() instanceof EvoTypesRootA);
        assertTrue(txnTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedA);
        assertTrue(txnTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                txnRootEvo.setCharAttribute('B');
                txnNestedEvo.setCharAttribute('B');
                txnLeafEvo.setCharAttribute('B');
                return null;
            }
        });

        assertTrue(txnTypes.getRootEvo() instanceof EvoTypesRootB);
        assertTrue(txnTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedB);
        assertTrue(txnTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafB);
    }

    public void testType3ReadOnlyPolymorphicEvo()
    {
        EvoType3ReadOnlyTypesA readOnlyTypes = EvoType3ReadOnlyTypesAFinder.findOneBypassCache(
            EvoType3ReadOnlyTypesAFinder.pk().charAttribute().eq('A').and(
            EvoType3ReadOnlyTypesAFinder.pk().booleanAttribute().eq(true)));
        EvoTypesRoot readOnlyRootEvo = readOnlyTypes.getRootEvo();
        EvoTypesNested readOnlyNestedEvo = readOnlyRootEvo.getNestedEvo();
        EvoTypesLeaf readOnlyLeafEvo = readOnlyNestedEvo.getLeafEvo();
        assertTrue(readOnlyRootEvo instanceof EvoTypesRootA);
        assertTrue(readOnlyNestedEvo instanceof EvoTypesNestedA);
        assertTrue(readOnlyLeafEvo instanceof EvoTypesLeafA);

        try
        {
            readOnlyRootEvo.setCharAttribute('B');
            readOnlyNestedEvo.setCharAttribute('B');
            readOnlyLeafEvo.setCharAttribute('B');
            fail("Do not allow mutation of persisted read-only objects.");
        }
        catch (MithraBusinessException mbe)
        {
            assertTrue(readOnlyRootEvo instanceof EvoTypesRootA);
            assertTrue(readOnlyNestedEvo instanceof EvoTypesNestedA);
            assertTrue(readOnlyLeafEvo instanceof EvoTypesLeafA);
        }

        EvoType3ReadOnlyTypesA newReadOnlyTypes = new EvoType3ReadOnlyTypesA();
        newReadOnlyTypes.copyRootEvo(this.createPolymorphicEvoTypesRootA());
        assertTrue(newReadOnlyTypes.getRootEvo() instanceof EvoTypesRootA);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedA);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);

        newReadOnlyTypes.getRootEvo().setCharAttribute('B');
        newReadOnlyTypes.getRootEvo().getNestedEvo().setCharAttribute('B');
        newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo().setCharAttribute('B');
        assertTrue(newReadOnlyTypes.getRootEvo() instanceof EvoTypesRootB);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedB);
        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafB);
    }

    public void testType3TxnPolymorphicEvo()
    {
        EvoType3TxnTypesB txnTypes = EvoType3TxnTypesBFinder.findOneBypassCache(
            EvoType3TxnTypesBFinder.pk().charAttribute().eq('B').and(
            EvoType3TxnTypesBFinder.pk().booleanAttribute().eq(false)));
        EvoTypesRoot txnRootEvo = txnTypes.getRootEvo();
        EvoTypesNested txnNestedEvo = txnRootEvo.getNestedEvo();
        EvoTypesLeaf txnLeafEvo = txnNestedEvo.getLeafEvo();
        assertTrue(txnRootEvo instanceof EvoTypesRootB);
        assertTrue(txnNestedEvo instanceof EvoTypesNestedB);
        assertTrue(txnLeafEvo instanceof EvoTypesLeafB);

        txnRootEvo.setCharAttribute('A');
        txnNestedEvo.setCharAttribute('A');
        txnLeafEvo.setCharAttribute('A');
        assertTrue(txnTypes.getRootEvo() instanceof EvoTypesRootA);
        assertTrue(txnTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedA);
        assertTrue(txnTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);
    }

//    public void testType3DatedReadOnlyPolymorphicEvo()
//    {
//        EvoType3DatedReadOnlyTypes readOnlyTypes = EvoType3DatedReadOnlyTypesAFinder.findOneBypassCache(
//            EvoType3DatedReadOnlyTypesAFinder.pk().charAttribute().eq('A').and(
//            EvoType3DatedReadOnlyTypesAFinder.pk().booleanAttribute().eq(true)).and(
//            EvoType3DatedReadOnlyTypesAFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
//            EvoType3DatedReadOnlyTypesAFinder.processingDate().eq(Timestamp.valueOf("2007-10-15 00:00:00.0")))));
//        EvoTypesLeaf readOnlyLeafEvo = readOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo();
//        assertTrue(readOnlyLeafEvo instanceof EvoTypesLeafA);
//
//        try
//        {
//            readOnlyLeafEvo.setCharAttribute('B');
//            fail("Do not allow mutation of persisted read-only objects.");
//        }
//        catch (MithraBusinessException mbe)
//        {
//        }
//
//        EvoType3DatedReadOnlyTypes newReadOnlyTypes = new EvoType3DatedReadOnlyTypesA(Timestamp.valueOf("2007-09-10 00:00:00.0"), Timestamp.valueOf("2007-10-15 00:00:00.0"));
//        EvoTypesLeafA evoTypesLeafA = new EvoTypesLeafA();
//        newReadOnlyTypes.getRootEvo().getNestedEvo().copyLeafEvo(evoTypesLeafA);
//        assertNotNull(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo());
//        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);
//
//        newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo().setCharAttribute('B');
//        assertNotNull(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo());
//        assertTrue(newReadOnlyTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafB);
//    }

//    public void testType3DatedTxnPolymorphicEvo()
//    {
//        final EvoType3DatedTxnTypes txnTypes = EvoType3DatedTxnTypesBFinder.findOneBypassCache(
//            EvoType3DatedTxnTypesBFinder.pk().charAttribute().eq('B').and(
//            EvoType3DatedTxnTypesBFinder.pk().booleanAttribute().eq(true)).and(
//            EvoType3DatedTxnTypesBFinder.businessDate().eq(Timestamp.valueOf("2007-09-10 00:00:00.0")).and(
//            EvoType3DatedTxnTypesBFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()))));
//        final EvoTypesRoot txnRootEvo = txnTypes.getRootEvo();
//        final EvoTypesNested txnNestedEvo = txnRootEvo.getNestedEvo();
//        final EvoTypesLeaf txnLeafEvo = txnNestedEvo.getLeafEvo();
//        assertTrue(txnRootEvo instanceof EvoTypesRootB);
//        assertTrue(txnNestedEvo instanceof EvoTypesNestedB);
//        assertTrue(txnLeafEvo instanceof EvoTypesLeafB);
//
//        final EvoTypesRoot evoTypesRoot = this.createPolymorphicEvoTypesRootA();
//
//        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
//        {
//            public Object executeTransaction(MithraTransaction tx) throws Throwable
//            {
//                txnTypes.copyRootEvo(evoTypesRoot);
//                return null;
//            }
//        });
//
//        assertTrue(txnTypes.getRootEvo() instanceof EvoTypesRootA);
//        assertTrue(txnTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedA);
//        assertTrue(txnTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafA);
//
//        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
//        {
//            public Object executeTransaction(MithraTransaction tx) throws Throwable
//            {
//                txnRootEvo.setCharAttribute('B');
//                txnNestedEvo.setCharAttribute('B');
//                txnLeafEvo.setCharAttribute('B');
//                return null;
//            }
//        });
//
//        assertTrue(txnTypes.getRootEvo() instanceof EvoTypesRootB);
//        assertTrue(txnTypes.getRootEvo().getNestedEvo() instanceof EvoTypesNestedB);
//        assertTrue(txnTypes.getRootEvo().getNestedEvo().getLeafEvo() instanceof EvoTypesLeafB);
//    }

    private EvoTypesRoot createPolymorphicEvoTypesRootA()
    {
        EvoTypesRootA evoTypesRootA = new EvoTypesRootA();
        EvoTypesNestedA evoTypesNestedA = new EvoTypesNestedA();
        EvoTypesLeafA evoTypesLeafA = new EvoTypesLeafA();
        evoTypesNestedA.copyLeafEvo(evoTypesLeafA);
        evoTypesRootA.copyNestedEvo(evoTypesNestedA);
        return evoTypesRootA;
    }
}
