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

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.test.domain.FullyCachedTinyBalance;
import com.gs.fw.common.mithra.test.domain.SpecialAccount;
import com.gs.fw.common.mithra.test.domain.TimezoneTest;
import com.gs.fw.common.mithra.test.domain.TimezoneTestFinder;
import com.gs.fw.common.mithra.util.MithraTimestamp;

import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;



public class TestSerializationAcrossTimezones extends RemoteMithraServerTestCase
{
    private TimeZone originalDefault = TimeZone.getDefault();

    @Override
    protected void setDefaultServerTimezone()
    {
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
    }

    @Override
    protected TimeZone getServerDatabaseTimezone()
    {
        return TimeZone.getTimeZone("America/Los_Angeles");
    }

    @Override
    protected void setUp() throws Exception
    {
        TimeZone asiaTimezone = TimeZone.getTimeZone("Asia/Tokyo");
        TimeZone.setDefault(asiaTimezone);
        MithraTimestamp.zSetDefaultTimezone(asiaTimezone);
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        TimeZone.setDefault(originalDefault);
    }

    protected Class[] getRestrictedClassList()
    {
        Class[] result = new Class[3];
        result[0] = TimezoneTest.class;
        result[1] = FullyCachedTinyBalance.class;
        result[2] = SpecialAccount.class;
        return result;
    }

    public void testAggregateData()
    {
        AggregateList aggList = new AggregateList(TimezoneTestFinder.all());
        aggList.addGroupBy("defDt", TimezoneTestFinder.insensitiveDate());
        aggList.addGroupBy("dbDt", TimezoneTestFinder.databaseDate());
        aggList.addGroupBy("utcDt", TimezoneTestFinder.utcDate());
        aggList.addGroupBy("defStp", TimezoneTestFinder.insensitiveTimestamp());
        aggList.addGroupBy("dbStp", TimezoneTestFinder.databaseTimestamp());
        aggList.addGroupBy("utcStp", TimezoneTestFinder.utcTimestamp());

        assertEquals(1, aggList.size());

        AggregateData aggData = aggList.get(0);

        Date insensitiveDate = aggData.getAttributeAsDate("defDt");
        Date databaseDate = aggData.getAttributeAsDate("dbDt");
        Date utcDate = aggData.getAttributeAsDate("utcDt");
        Timestamp insensitiveTimestamp = aggData.getAttributeAsTimestamp("defStp");
        Timestamp databaseTimestamp = aggData.getAttributeAsTimestamp("dbStp");
        Timestamp utcTimestamp = aggData.getAttributeAsTimestamp("utcStp");

        TimezoneTest.assertDatesAndTimestamps(insensitiveDate, utcDate, databaseDate, insensitiveTimestamp, utcTimestamp, databaseTimestamp, getServerDatabaseTimezone());
    }

    public void testNormalRetrieval()
    {
        TimezoneTest timezoneTest = TimezoneTestFinder.findOne(TimezoneTestFinder.all());

        TimezoneTest.assertDatesAndTimestamps(timezoneTest.getInsensitiveDate(), timezoneTest.getUtcDate(), timezoneTest.getDatabaseDate(),
                timezoneTest.getInsensitiveTimestamp(), timezoneTest.getUtcTimestamp(), timezoneTest.getDatabaseTimestamp(),
                getServerDatabaseTimezone());
    }
}
