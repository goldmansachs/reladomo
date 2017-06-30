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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.primitive.*;
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.ParaDesk;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.ParaDeskList;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.AtomicNotEqualityOperation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.math.BigDecimal;

public class TestNotIn
extends TestSqlDatatypes
{

    public void testBasicNotInRetrieval()
    throws SQLException
    {
        String sql;
        List desks;

        //Boolean
        BooleanHashSet boolSet = new BooleanHashSet();
        boolSet.add(true);
        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN not in ( 1 ) ";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().notIn(boolSet));
        this.genericRetrievalTest(sql, desks);
        assertTrue(desks.size() > 0);

        boolSet = new BooleanHashSet();
        boolSet.add(false);
        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN not in ( 0 ) ";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().notIn(boolSet));
        this.genericRetrievalTest(sql, desks);
        assertTrue(desks.size() > 0);

        //Byte
        ByteHashSet byteSet = new ByteHashSet();
        byteSet.add((byte) 10);
        byteSet.add((byte) 20);
        byteSet.add((byte) 30);
        byteSet.add((byte) 40);
        byteSet.add((byte) 50);
        byteSet.add((byte) 127);
        byteSet.add((byte) -127);
        byteSet.add((byte) -128);
        byteSet.add((byte) 127);
        sql = "select * from PARA_DESK where LOCATION_BYTE not in (10, 20, 30, 40, 50, 127, -127, -128, 127)";
        desks = new ParaDeskList(ParaDeskFinder.locationByte().notIn(byteSet));
        this.genericRetrievalTest(sql, desks);

        //Char
        CharHashSet charSet = new CharHashSet();
        charSet.clear();
        charSet.add('O');
        charSet.add('P');
        charSet.add('G');
        charSet.add('T');
        sql = "select * from PARA_DESK where STATUS_CHAR not in ( 'O', 'P', 'G', 'T' )";
        desks = new ParaDeskList(ParaDeskFinder.statusChar().notIn(charSet));
        this.genericRetrievalTest(sql, desks);

        //Date
        HashSet objectSet = new HashSet();
        objectSet.add(getDawnOfTime());
        objectSet.add(getTestDate());
        sql = "select * from PARA_DESK where CLOSED_DATE not in ('1900-01-01' , '1981-06-08' )";
        desks = new ParaDeskList(ParaDeskFinder.closedDate().notIn(objectSet));
        this.genericRetrievalTest(sql, desks);

        //Double
        Connection connection = this.getConnection();
        DoubleHashSet doubleSet = new DoubleHashSet();
        doubleSet.add(4000000000.0);
        doubleSet.add(677673542.3);
        sql = "select * from PARA_DESK where SIZE_DOUBLE not in ( ?, ? ) ";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setDouble(1, 677673542.3);
        ps.setDouble(2, 4000000000.0);
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().notIn(doubleSet));
        this.genericRetrievalTest(ps, desks, connection);

        //Float
        connection = this.getConnection();
        FloatHashSet floatSet = new FloatHashSet();
        floatSet.add((float) 4000000000.0);
        floatSet.add((float) 677673542.3);
        sql = "select * from PARA_DESK where MAX_FLOAT not in ( ?, ? ) ";
        ps = connection.prepareStatement(sql);
        ps.setFloat(1, (float) 677673542.3);
        ps.setFloat(2, (float) 4000000000.0);
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().notIn(floatSet));
        this.genericRetrievalTest(ps, desks, connection);

        //Integer
        IntHashSet IntHashSet = new IntHashSet();
        IntHashSet.add(5);
        IntHashSet.add(100);
        IntHashSet.add(200);
        IntHashSet.add(300);
        IntHashSet.add(400);
        IntHashSet.add(500);
        IntHashSet.add(600);
        IntHashSet.add(700);
        IntHashSet.add(800);
        IntHashSet.add(900);
        sql = "select * from PARA_DESK where TAG_INT not in (5, 100, 200, 300, 400, 500, 600, 700, 800, 900)";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().notIn(IntHashSet));
        this.genericRetrievalTest(sql, desks);

        //Long
        LongHashSet longSet = new LongHashSet();
        longSet.add(1000000);
        longSet.add(2000000);
        sql = "select * from PARA_DESK where CONNECTION_LONG not in (1000000, 2000000)";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().notIn(longSet));
        this.genericRetrievalTest(sql, desks);

        //Short
        ShortHashSet shortSet = new ShortHashSet();
        shortSet.add((short) 1000);
        shortSet.add((short) 2000);
        sql = "select * from PARA_DESK where MIN_SHORT not in (1000, 2000)";
        desks = new ParaDeskList(ParaDeskFinder.minShort().notIn(shortSet));
        this.genericRetrievalTest(sql, desks);

        //String
        objectSet.clear();
        objectSet.add("rnd");
        objectSet.add("cap");
        objectSet.add("lsd");
        objectSet.add("zzz");
        sql = "select * from PARA_DESK where DESK_ID_STRING not in ('rnd', 'cap', 'lsd', 'zzz')";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().notIn(objectSet));
        this.genericRetrievalTest(sql, desks);

        //Timestamp
        objectSet.clear();
        objectSet.add(new Timestamp(getDawnOfTime().getTime()));
        objectSet.add(getTestTimestamp());
        sql = "select * from PARA_DESK where CREATE_TIMESTAMP not in ( '1900-01-01 00:00:00.0' , '1981-06-08 02:01:00.0' )";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().notIn(objectSet));
        this.genericRetrievalTest(sql, desks);

        objectSet.clear();
        objectSet.add(InfinityTimestamp.getParaInfinity());
        objectSet.add(getTestTimestamp());
        sql = "select * from PARA_DESK where CREATE_TIMESTAMP not in ( '9999-12-01 23:59:00.0' , '1981-06-08 02:01:00.0' )";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().notIn(objectSet));
        this.genericRetrievalTest(sql, desks);
    }

    public void testNullableBoolean() throws SQLException
    {
        ParaDesk paraDesk = new ParaDesk();
        paraDesk.setActiveBooleanNull();
        paraDesk.setDeskIdString("Nul");
        this.insertTestData(FastList.newListWith(paraDesk));

        BooleanHashSet boolSet = new BooleanHashSet();;
        boolSet.add(true);
        boolSet.add(false);
        String sql = "select * from PARA_DESK where ACTIVE_BOOLEAN not in ( 0 , 1 ) ";
        List desks = new ParaDeskList(ParaDeskFinder.activeBoolean().notIn(boolSet));
        this.genericRetrievalTest(sql, desks, 0);
        assertTrue(desks.size() == 0);

        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN in ( 0, 1 ) ";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().in(boolSet));
        this.genericRetrievalTest(sql, desks);
        assertTrue(desks.size() > 0);
    }

    public void testHugeNotInClause()
    {
        IntHashSet IntHashSet = new IntHashSet();
        for (int i=0;i<1007;i++)
        {
            IntHashSet.add(i);
        }
        new ParaDeskList(ParaDeskFinder.tagInt().notIn(IntHashSet).and(ParaDeskFinder.createTimestamp().eq(new Timestamp(getDawnOfTime().getTime())))).forceResolve();
        // the test here is that the SQL generated is correct. Will die with exception if not.

        DoubleHashSet doubleSet = new DoubleHashSet();
        doubleSet.add(10.54);
        doubleSet.add(10032.23);
        for(int i=0;i<1000;i++)
        {
            doubleSet.add(i);
        }
        ParaDeskList list = new ParaDeskList(ParaDeskFinder.sizeDouble().notIn(doubleSet));
        Set<String> ids = new HashSet<String>();
        for(int i=0;i<list.size();i++)
        {
            assertTrue(list.get(i).getSizeDouble() != 10.54);
            assertTrue(list.get(i).getSizeDouble() != 10032.23);
            ids.add(list.get(i).getDeskIdString());
        }
        assertEquals(ids.size(), list.size());
    }

    public void testSetLengthZero()
    {
        assertTrue(ParaDeskFinder.activeBoolean().notIn(new BooleanHashSet()) instanceof All);
        assertTrue(ParaDeskFinder.closedDate().notIn(new HashSet()) instanceof All);
        assertTrue(ParaDeskFinder.connectionLong().notIn(new LongHashSet()) instanceof All);
        assertTrue(ParaDeskFinder.createTimestamp().notIn(new HashSet()) instanceof All);
        assertTrue(ParaDeskFinder.deskIdString().notIn(new HashSet()) instanceof All);
        assertTrue(ParaDeskFinder.locationByte().notIn(new ByteHashSet()) instanceof All);
        assertTrue(ParaDeskFinder.maxFloat().notIn(new FloatHashSet()) instanceof All);
        assertTrue(ParaDeskFinder.minShort().notIn(new ShortHashSet()) instanceof All);
        assertTrue(ParaDeskFinder.sizeDouble().notIn(new DoubleHashSet()) instanceof All);
        assertTrue(ParaDeskFinder.statusChar().notIn(new CharHashSet()) instanceof All);
        assertTrue(ParaDeskFinder.tagInt().notIn(new IntHashSet()) instanceof All);
        assertTrue(ParaDeskFinder.bigDouble().notIn(new HashSet<BigDecimal>()) instanceof All);
    }

    public void testSetLengthOne()
    {
        BooleanHashSet set = new BooleanHashSet();
        set.add(true);
        assertTrue(ParaDeskFinder.activeBoolean().notIn(set) instanceof AtomicNotEqualityOperation);
        HashSet set2 = new HashSet();
        set2.add(new java.util.Date());
        assertTrue(ParaDeskFinder.closedDate().notIn(set2) instanceof AtomicNotEqualityOperation);
        LongHashSet set3 = new LongHashSet();
        set3.add(1);
        assertTrue(ParaDeskFinder.connectionLong().notIn(set3) instanceof AtomicNotEqualityOperation);
        HashSet set4 = new HashSet();
        set4.add(new Timestamp(System.currentTimeMillis()));
        assertTrue(ParaDeskFinder.createTimestamp().notIn(set4) instanceof AtomicNotEqualityOperation);
        HashSet set5 = new HashSet();
        set5.add("test");
        assertTrue(ParaDeskFinder.deskIdString().notIn(set5) instanceof AtomicNotEqualityOperation);
        ByteHashSet set6 = new ByteHashSet();
        set6.add((byte)1);
        assertTrue(ParaDeskFinder.locationByte().notIn(set6) instanceof AtomicNotEqualityOperation);
        FloatHashSet set7 = new FloatHashSet();
        set7.add(1.0f);
        assertTrue(ParaDeskFinder.maxFloat().notIn(set7) instanceof AtomicNotEqualityOperation);
        ShortHashSet set8 = new ShortHashSet();
        set8.add((short)1);
        assertTrue(ParaDeskFinder.minShort().notIn(set8) instanceof AtomicNotEqualityOperation);
        DoubleHashSet set9 = new DoubleHashSet();
        set9.add(1.0d);
        assertTrue(ParaDeskFinder.sizeDouble().notIn(set9) instanceof AtomicNotEqualityOperation);
        CharHashSet set10 = new CharHashSet();
        set10.add('x');
        assertTrue(ParaDeskFinder.statusChar().notIn(set10) instanceof AtomicNotEqualityOperation);
        IntHashSet set11 = new IntHashSet();
        set11.add(1);
        assertTrue(ParaDeskFinder.tagInt().notIn(set11) instanceof AtomicNotEqualityOperation);
        Set<BigDecimal> set12 = new HashSet<BigDecimal>(1);
        set12.add(BigDecimal.ONE);
        assertTrue(ParaDeskFinder.bigDouble().notIn(set12) instanceof AtomicNotEqualityOperation);
    }

}
