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

import com.gs.fw.common.mithra.test.domain.TimezoneTest;
import com.gs.fw.common.mithra.test.domain.TimezoneTestFinder;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.TimeZone;



public class TestTimezoneConversionNewYork extends TestTimezoneConversion
{

    protected TimeZone getDatabaseTimeZone()
    {
        return TimeZone.getTimeZone("America/New_York");
    }

//    public void testTimestampProblem()
//    throws Exception
//    {
//        PreparedStatement ps;
//        Connection con;
//        ResultSet rs;
//
//        String sql = "select INSENSISTIVE_DATE, DATABASE_DATE, UTC_DATE from TIMEZONE_TEST WHERE TIMEZONE_TEST_ID = 1";
//        con = getDBConnection();
//        con.setAutoCommit(false);
//
//        ps = con.prepareStatement(sql);
//        rs = ps.executeQuery();
//
//        assertTrue(rs.next());
//
//        Timestamp t1 = rs.getTimestamp(1);
//        Timestamp t2 = rs.getTimestamp(2);
//        Timestamp t3 = rs.getTimestamp(3);
//
//        rs.close();
//        ps.close();
//        con.close();
//
//        assertTrue(t1 != t2);
//        assertTrue(t2 != t3);
//        assertTrue(t3 != t1);
//    }
//    private Connection getDBConnection()
//    throws Exception
//    {
//        Class.forName("org.h2.Driver");
//        Connection conn = DriverManager.getConnection("jdbc:h2:mem:A", "sa", "");
//        return conn;
//    }

    public void testBitemporalInfinity() throws Exception
    {
        this.checkBitemporalInfinity(TimeZone.getTimeZone("America/New_York"));
    }

    // Do not include this performance test in the commit check build - it can be uncommented on adhoc basis as required
//    public void testFixInfinityPerformanceNewYork() throws Exception
//    {
//        this.checkFixInfinityPerformance(TimeZone.getTimeZone("America/New_York"));
//    }

    public void testReadInNewYork()
    {
        checkRead(TimeZone.getTimeZone("America/New_York"));
    }

    public void testReadAggregateInNewYork()
    {
        checkReadAggregate(TimeZone.getTimeZone("America/New_York"));
    }

    public void testQueryNewYork() throws ParseException
    {
        checkQuery(TimeZone.getTimeZone("America/New_York"));
    }

    public void testUpdateNewYork() throws ParseException
    {
        checkUpdate(TimeZone.getTimeZone("America/New_York"));
    }

    public void testUpdatePkNewYork() throws ParseException
    {
        checkUpdateWithPk(TimeZone.getTimeZone("America/New_York"));
    }

    public void testBatchUpdatePkNewYork() throws ParseException
    {
        checkBatchUpdateWithPk(TimeZone.getTimeZone("America/New_York"));
    }

    public void testInsertNewYork() throws ParseException
    {
        checkInsert(TimeZone.getTimeZone("America/New_York"));
    }

    public void testBatchInsertNewYork() throws ParseException
    {
        checkBatchInsert(TimeZone.getTimeZone("America/New_York"));
    }

    public void testDeleteNewYork() throws ParseException
    {
        checkDelete(TimeZone.getTimeZone("America/New_York"));
    }

    public void testBatchDeleteNewYork() throws ParseException
    {
        checkBatchDelete(TimeZone.getTimeZone("America/New_York"));
    }

    public void testRefreshNewYork() throws ParseException
    {
        checkRefresh(TimeZone.getTimeZone("America/New_York"));
    }

    public void testAmericasDstSwitch()
    {
        long eightPm = 1362877200000L; //2013-03-09 20:00:00.000 EST
        checkInsertRead(eightPm, 1000, true);
        checkInsertRead(eightPm + 3600000, 1001, true);
        checkInsertRead(eightPm + 2*3600000, 1002, true);
        checkInsertRead(eightPm + 3*3600000, 1003, true);
        checkInsertRead(eightPm + 4*3600000, 1004, true);
        checkInsertRead(eightPm + 5*3600000, 1005, true);
        checkInsertRead(eightPm + 6*3600000, 1006, true);
        checkInsertRead(eightPm + 7*3600000, 1007, true);
        checkInsertRead(eightPm + 8*3600000, 1008, true);
        checkInsertRead(eightPm + 9*3600000, 1009, true);
        checkInsertRead(eightPm + 10*3600000, 1010, true);
        checkInsertRead(eightPm + 11*3600000, 1011, true);
    }

    public void testAmericasEstSwitch()
    {
        long eightPm = 1351994400000L; //2012-11-03 20:00:00.000 DST
        checkInsertRead(eightPm, 1000, true);
        checkInsertRead(eightPm + 3600000, 1001, true);
        checkInsertRead(eightPm + 2*3600000, 1002, true);
        checkInsertRead(eightPm + 3*3600000, 1003, true);
        checkInsertRead(eightPm + 4*3600000, 1004, false);
        checkInsertRead(eightPm + 5*3600000, 1005, true);
        checkInsertRead(eightPm + 6*3600000, 1006, true);
        checkInsertRead(eightPm + 7*3600000, 1007, true);
        checkInsertRead(eightPm + 8*3600000, 1008, true);
        checkInsertRead(eightPm + 9*3600000, 1009, true);
        checkInsertRead(eightPm + 10*3600000, 1010, true);
        checkInsertRead(eightPm + 11*3600000, 1011, true);
    }

    private void checkInsertRead(long time, int id, boolean checkDbTime)
    {
        Timestamp nearSwitch = new Timestamp(time);
        TimezoneTest timezoneTest = new TimezoneTest();
        timezoneTest.setTimezoneTestId(id);
        timezoneTest.setDatabaseTimestamp(nearSwitch);
        timezoneTest.setUtcTimestamp(nearSwitch);
        timezoneTest.insert();
        TimezoneTest afterInsert = TimezoneTestFinder.findOneBypassCache(TimezoneTestFinder.timezoneTestId().eq(id));

        if (checkDbTime)
        {
            assertEquals(time, afterInsert.getDatabaseTimestamp().getTime());
        }
        assertEquals(time, afterInsert.getUtcTimestamp().getTime());
    }
}
