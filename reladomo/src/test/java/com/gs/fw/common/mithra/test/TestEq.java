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
/*
 * File:        : $Source$
 *
 *
 *
 */

import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.ParaDeskList;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.ParaDesk;
import com.gs.fw.common.mithra.finder.Operation;

import java.util.List;
import java.sql.Timestamp;
import java.sql.SQLException;

public class TestEq
extends TestSqlDatatypes
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            ParaDesk.class,
        };
    }

    public void testBasicEqRetreival()
    throws SQLException
    {
        String sql;
        List desks;

        //Boolean
        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN = FALSE";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().eq(false));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN = TRUE";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().eq(true));
        this.genericRetrievalTest(sql, desks);

        //Byte
        byte bite = 127;
        sql = "select * from PARA_DESK where LOCATION_BYTE = 127";
        desks = new ParaDeskList(ParaDeskFinder.locationByte().eq(bite));
        this.genericRetrievalTest(sql, desks);

        //Char
        sql = "select * from PARA_DESK where STATUS_CHAR = 'O'";
        desks = new ParaDeskList(ParaDeskFinder.statusChar().eq('O'));
        this.genericRetrievalTest(sql, desks);

        //Date
        sql = "select * from PARA_DESK where CLOSED_DATE = '1900-01-01'";
        desks = new ParaDeskList(ParaDeskFinder.closedDate().eq(getDawnOfTime()));
        this.genericRetrievalTest(sql, desks);

        //Double
        sql = "select * from PARA_DESK where SIZE_DOUBLE = 4000000000.0";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().eq(4000000000.0));
        this.genericRetrievalTest(sql, desks);

        //Float
        sql = "select * from PARA_DESK where MAX_FLOAT = 4000000000.0";
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().eq((float) 4000000000.0));
        this.genericRetrievalTest(sql, desks);

        //Integer
        sql = "select * from PARA_DESK where TAG_INT = 100";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().eq(100));
        this.genericRetrievalTest(sql, desks);

        //Long
        sql = "select * from PARA_DESK where CONNECTION_LONG = 1000000";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().eq(1000000));
        this.genericRetrievalTest(sql, desks);

        //Short
        sql = "select * from PARA_DESK where MIN_SHORT = 100";
        desks = new ParaDeskList(ParaDeskFinder.minShort().eq((short) 100));
        this.genericRetrievalTest(sql, desks);

        //String
        sql = "select * from PARA_DESK where DESK_ID_STRING = 'rnd'";
        ParaDesk paraDesk = ParaDeskFinder.findOne(ParaDeskFinder.deskIdString().eq("rnd"));
        desks = new ParaDeskList();
        desks.add(paraDesk);
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING = 'rnd' and DESK_ID_STRING = 'abc'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().eq("rnd").and(ParaDeskFinder.deskIdString().eq("abc")));
        this.genericRetrievalTest(sql, desks, 0);

        //Timestamp
        sql = "select * from PARA_DESK where CREATE_TIMESTAMP = '1900-01-01 00:00:00.0'";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().eq(getDawnOfTime()));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where CREATE_TIMESTAMP = '1981-06-08 02:01:00.0'";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().eq(getTestTimestamp()));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where CREATE_TIMESTAMP = '9999-12-01 23:59:00.0'";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().eq(InfinityTimestamp.getParaInfinity()));
        this.genericRetrievalTest(sql, desks);
    }

    public void testEqOnDifferentAttributes()
    throws SQLException
    {
        String sql = "select * from PARA_DESK where DESK_ID_STRING = 'rnd' and CREATE_TIMESTAMP = '1981-06-08 02:01:00.0'";
        List desks = new ParaDeskList(ParaDeskFinder.deskIdString().eq("rnd").and(ParaDeskFinder.createTimestamp().eq(getTestTimestamp())));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN = TRUE and DESK_ID_STRING = 'abc'";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().eq(true).and(ParaDeskFinder.deskIdString().eq("abc")));
        this.genericRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN = FALSE and DESK_ID_STRING = 'abc'";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().eq(false).and(ParaDeskFinder.deskIdString().eq("abc")));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where TAG_INT = 100 and CONNECTION_LONG = 2000000 and CREATE_TIMESTAMP = '9999-12-01 23:59:00.0' and ACTIVE_BOOLEAN = TRUE";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().eq(100).and(ParaDeskFinder.connectionLong().eq(2000000)).and(ParaDeskFinder.createTimestamp().eq(InfinityTimestamp.getParaInfinity())).and(ParaDeskFinder.activeBoolean().eq(true)));
        this.genericRetrievalTest(sql, desks);
    }


    public void testEqGreaterThanOnSameAttribute()
    throws SQLException
    {
        String sql;
        List desks;

        //Byte
        sql = "select * from PARA_DESK where LOCATION_BYTE > -127 and LOCATION_BYTE = 127";
        desks = new ParaDeskList(ParaDeskFinder.locationByte().eq((byte) 127).and(ParaDeskFinder.locationByte().greaterThan((byte) -127)));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where LOCATION_BYTE > -127 and LOCATION_BYTE = -127";
        desks = new ParaDeskList(ParaDeskFinder.locationByte().eq((byte) -127).and(ParaDeskFinder.locationByte().greaterThan((byte) -127)));
        this.exactRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where LOCATION_BYTE > -127 and LOCATION_BYTE = -128";
        desks = new ParaDeskList(ParaDeskFinder.locationByte().eq((byte) -128).and(ParaDeskFinder.locationByte().greaterThan((byte) -127)));
        this.exactRetrievalTest(sql, desks, 0);

        //Char
        sql = "select * from PARA_DESK where STATUS_CHAR > 'O' and STATUS_CHAR = 'P'";
        desks = new ParaDeskList(ParaDeskFinder.statusChar().eq('P').and(ParaDeskFinder.statusChar().greaterThan('O')));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where STATUS_CHAR > 'O' and STATUS_CHAR = 'O'";
        desks = new ParaDeskList(ParaDeskFinder.statusChar().eq('O').and(ParaDeskFinder.statusChar().greaterThan('O')));
        this.exactRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where STATUS_CHAR > 'O' and STATUS_CHAR = 'M'";
        desks = new ParaDeskList(ParaDeskFinder.statusChar().eq('N').and(ParaDeskFinder.statusChar().greaterThan('O')));
        this.exactRetrievalTest(sql, desks, 0);

        //Date
        sql = "select * from PARA_DESK where CLOSED_DATE > '1981-06-08' and CLOSED_DATE = '9999-12-01'";
        desks = new ParaDeskList(ParaDeskFinder.closedDate().eq(InfinityTimestamp.getParaInfinity()).and(ParaDeskFinder.closedDate().greaterThan(getTestDate())));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where CLOSED_DATE > '1981-06-08' and CLOSED_DATE = '1981-06-08'";
        desks = new ParaDeskList(ParaDeskFinder.closedDate().eq(getTestDate()).and(ParaDeskFinder.closedDate().greaterThan(getTestDate())));
        this.exactRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where CLOSED_DATE > '1981-06-08' and CLOSED_DATE = '1900-01-01'";
        desks = new ParaDeskList(ParaDeskFinder.closedDate().eq(getDawnOfTime()).and(ParaDeskFinder.closedDate().greaterThan(getTestDate())));
        this.exactRetrievalTest(sql, desks, 0);

        //Double
        sql = "select * from PARA_DESK where SIZE_DOUBLE > 4000000000.0 and SIZE_DOUBLE = 5464565234435.9";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().eq(5464565234435.9).and(ParaDeskFinder.sizeDouble().greaterThan(4000000000.0)));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where SIZE_DOUBLE > 4000000000.0 and SIZE_DOUBLE = 4000000000.0";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().eq(4000000000.0).and(ParaDeskFinder.sizeDouble().greaterThan(4000000000.0)));
        this.exactRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where SIZE_DOUBLE > 4000000000.0 and SIZE_DOUBLE = 1564654.34";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().eq(1564654.34).and(ParaDeskFinder.sizeDouble().greaterThan(4000000000.0)));
        this.exactRetrievalTest(sql, desks, 0);

        //Float
//        sql = "select * from PARA_DESK where MAX_FLOAT > 36546.43 and MAX_FLOAT = 999999.9";
//        desks = new ParaDeskList(ParaDeskFinder.maxFloat().eq((float) 999999.9).and(ParaDeskFinder.maxFloat().greaterThan((float) 36546.43)));
//        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where MAX_FLOAT > 36546.43 and MAX_FLOAT = 36546.43";
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().eq((float) 36546.43).and(ParaDeskFinder.maxFloat().greaterThan((float) 36546.43)));
        this.exactRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where MAX_FLOAT > 36546.43 and MAX_FLOAT = 23423.234234234";
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().eq((float) 23423.234234234).and(ParaDeskFinder.maxFloat().greaterThan((float) 36546.43)));
        this.exactRetrievalTest(sql, desks, 0);

        //Int
        sql = "select * from PARA_DESK where TAG_INT > 10 and TAG_INT = 100";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().eq(100).and(ParaDeskFinder.tagInt().greaterThan(10)));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where TAG_INT > 10 and TAG_INT = 10";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().eq(10).and(ParaDeskFinder.tagInt().greaterThan(10)));
        this.exactRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where TAG_INT > 10 and TAG_INT = 5";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().eq(5).and(ParaDeskFinder.tagInt().greaterThan(10)));
        this.exactRetrievalTest(sql, desks, 0);

        //Long
        sql = "select * from PARA_DESK where CONNECTION_LONG > 2000000 and CONNECTION_LONG = 3000000 ";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().eq(3000000).and(ParaDeskFinder.connectionLong().greaterThan(2000000)));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where CONNECTION_LONG > 2000000 and CONNECTION_LONG = 2000000 ";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().eq(2000000).and(ParaDeskFinder.connectionLong().greaterThan(2000000)));
        this.exactRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where CONNECTION_LONG > 2000000 and CONNECTION_LONG = 1000000 ";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().eq(1000000).and(ParaDeskFinder.connectionLong().greaterThan(2000000)));
        this.exactRetrievalTest(sql, desks, 0);

        //Short
        sql = "select * from PARA_DESK where MIN_SHORT > 2000 and MIN_SHORT = 3000 ";
        desks = new ParaDeskList(ParaDeskFinder.minShort().eq((short) 3000).and(ParaDeskFinder.minShort().greaterThan((short) 2000)));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where MIN_SHORT > 2000 and MIN_SHORT = 2000 ";
        desks = new ParaDeskList(ParaDeskFinder.minShort().eq((short) 2000).and(ParaDeskFinder.minShort().greaterThan((short) 2000)));
        this.exactRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where MIN_SHORT > 2000 and MIN_SHORT = 1000 ";
        desks = new ParaDeskList(ParaDeskFinder.minShort().eq((short) 1000).and(ParaDeskFinder.minShort().greaterThan((short) 2000)));
        this.exactRetrievalTest(sql, desks, 0);

        //String
        sql = "select * from PARA_DESK where DESK_ID_STRING > 'rnd' and DESK_ID_STRING = 'usd'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().eq("usd").and(ParaDeskFinder.deskIdString().greaterThan("rnd")));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING > 'rnd' and DESK_ID_STRING = 'rnd'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().eq("rnd").and(ParaDeskFinder.deskIdString().greaterThan("rnd")));
        this.exactRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where DESK_ID_STRING > 'rnd' and DESK_ID_STRING = 'abc'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().eq("abc").and(ParaDeskFinder.deskIdString().greaterThan("rnd")));
        this.exactRetrievalTest(sql, desks, 0);

        //Timestamp
        sql = "select * from PARA_DESK where CREATE_TIMESTAMP > '1981-06-08 02:01:00.0' and CREATE_TIMESTAMP = '9999-12-01 23:59:00.0'";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().eq(InfinityTimestamp.getParaInfinity()).and(ParaDeskFinder.createTimestamp().greaterThan(getTestTimestamp())));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where CREATE_TIMESTAMP > '1981-06-08 02:01:00.0' and CREATE_TIMESTAMP = '1981-06-08 02:01:00.0'";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().eq(getTestTimestamp()).and(ParaDeskFinder.createTimestamp().greaterThan(getTestTimestamp())));
        this.exactRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where CREATE_TIMESTAMP > '1981-06-08 02:01:00.0' and CREATE_TIMESTAMP = '1900-01-01 00:00:00.0'";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().eq(new Timestamp(getDawnOfTime().getTime())).and(ParaDeskFinder.createTimestamp().greaterThan(getTestTimestamp())));
        this.exactRetrievalTest(sql, desks, 0);

        Operation op =ParaDeskFinder.statusChar().eq(ParaDeskFinder.statusChar());
        desks = new ParaDeskList(op);
        assertTrue(desks.size() == (new ParaDeskList(ParaDeskFinder.all())).size());




    }
}
