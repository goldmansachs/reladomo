

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
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.AndOperation;

import java.sql.SQLException;
import java.util.List;

public class TestNotEq
extends TestSqlDatatypes
{
    public void testBasicNotEqRetreival()
    throws SQLException
    {
        String sql;
        List desks;

        //Boolean
        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN <> 0";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().notEq(false));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN <> 1";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().notEq(true));
        this.genericRetrievalTest(sql, desks);

        //Byte
        byte bite = 127;
        sql = "select * from PARA_DESK where LOCATION_BYTE <> 127";
        desks = new ParaDeskList(ParaDeskFinder.locationByte().notEq(bite));
        this.genericRetrievalTest(sql, desks);

        //Char
        sql = "select * from PARA_DESK where STATUS_CHAR <> 'O'";
        desks = new ParaDeskList(ParaDeskFinder.statusChar().notEq('O'));
        this.genericRetrievalTest(sql, desks);

        //Date
        sql = "select * from PARA_DESK where CLOSED_DATE <> '1900-01-01'";
        desks = new ParaDeskList(ParaDeskFinder.closedDate().notEq(getDawnOfTime()));
        this.genericRetrievalTest(sql, desks);

        //Double
        sql = "select * from PARA_DESK where SIZE_DOUBLE <> 4000000000.0";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().notEq(4000000000.0));
        this.genericRetrievalTest(sql, desks);

        //Float
        sql = "select * from PARA_DESK where MAX_FLOAT <> 4000000000.0";
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().notEq((float) 4000000000.0));
        this.genericRetrievalTest(sql, desks);

        //Integer
        sql = "select * from PARA_DESK where TAG_INT <> 100";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().notEq(100));
        this.genericRetrievalTest(sql, desks);

        //Long
        sql = "select * from PARA_DESK where CONNECTION_LONG <> 1000000";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().notEq(1000000));
        this.genericRetrievalTest(sql, desks);

        //Short
        sql = "select * from PARA_DESK where MIN_SHORT <> 100";
        desks = new ParaDeskList(ParaDeskFinder.minShort().notEq((short) 100));
        this.genericRetrievalTest(sql, desks);

        //String
        sql = "select * from PARA_DESK where DESK_ID_STRING <> 'rnd'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().notEq("rnd"));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where DESK_ID_STRING <> 'rnd' and DESK_ID_STRING <> 'abc'";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().notEq("rnd").and(ParaDeskFinder.deskIdString().notEq("abc")));
        this.genericRetrievalTest(sql, desks, 0);

        //Timestamp
        sql = "select * from PARA_DESK where CREATE_TIMESTAMP <> '1900-01-01 00:00:00.0'";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().notEq(getDawnOfTime()));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where CREATE_TIMESTAMP <> '1981-06-08 02:01:00.0'";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().notEq(getTestTimestamp()));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where CREATE_TIMESTAMP <> '9999-12-01 23:59:00.0'";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().notEq(InfinityTimestamp.getParaInfinity()));
        this.genericRetrievalTest(sql, desks);
    }

    public void testNotEqAndEqOnSameAttribute()
    throws SQLException
    {
        String sql = "select * from PARA_DESK where DESK_ID_STRING <> 'rnd' and DESK_ID_STRING = 'moh'";
        List desks = new ParaDeskList(ParaDeskFinder.deskIdString().notEq("rnd").and(ParaDeskFinder.deskIdString().eq("moh")));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where TAG_INT <> 100 and TAG_INT = 10";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().notEq(100).and(ParaDeskFinder.tagInt().eq(10)));
        this.genericRetrievalTest(sql, desks);
    }

    public void testNotEqOnDifferentAttributes()
    throws SQLException
    {
        String sql = "select * from PARA_DESK where DESK_ID_STRING <> 'rnd' and CREATE_TIMESTAMP <> '1981-06-08 02:01:00.0'";
        List desks = new ParaDeskList(ParaDeskFinder.deskIdString().notEq("rnd").and(ParaDeskFinder.createTimestamp().notEq(getTestTimestamp())));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN <> 1 and DESK_ID_STRING <> 'abc'";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().notEq(true).and(ParaDeskFinder.deskIdString().notEq("abc")));
        this.genericRetrievalTest(sql, desks, 0);

        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN <> 0 and DESK_ID_STRING <> 'abc'";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().notEq(false).and(ParaDeskFinder.deskIdString().notEq("abc")));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where TAG_INT <> 100 and CONNECTION_LONG <> 2000000 and CREATE_TIMESTAMP <> '9999-12-01 23:59:00' and ACTIVE_BOOLEAN <> 0";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().notEq(100).and(ParaDeskFinder.connectionLong().notEq(2000000)).and(ParaDeskFinder.createTimestamp().notEq(InfinityTimestamp.getParaInfinity())).and(ParaDeskFinder.activeBoolean().notEq(false)));
        this.genericRetrievalTest(sql, desks);
    }

    // todo: fix complex None operation
    public void xtestNoneWithDated()
    {
        Operation op = new None(AuditedOrderItemFinder.orderId());
        op = new AndOperation(op, AuditedOrderItemFinder.discountPrice().eq(1));
        AuditedOrderItemList orderItemList = AuditedOrderItemFinder.findMany(op);
        orderItemList.setBypassCache(true);
        orderItemList.forceResolve();
    }
}
