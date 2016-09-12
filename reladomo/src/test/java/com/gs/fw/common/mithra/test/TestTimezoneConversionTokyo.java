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

import java.text.ParseException;
import java.util.TimeZone;



public class TestTimezoneConversionTokyo extends TestTimezoneConversion
{

    protected TimeZone getDatabaseTimeZone()
    {
        return TimeZone.getTimeZone("Asia/Tokyo");
    }

    public void testBitemporalInfinity() throws Exception
    {
        this.checkBitemporalInfinity(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    // Do not include this performance test in the commit check build - it can be uncommented on adhoc basis as required
//    public void testFixInfinityPerformanceTokyo() throws Exception
//    {
//        this.checkFixInfinityPerformance(TimeZone.getTimeZone("Asia/Tokyo"));
//    }

    public void testReadTokyo()
    {
        checkRead(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    public void testReadAggregateTokyo()
    {
        checkReadAggregate(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    public void testQueryTokyo() throws ParseException
    {
        checkQuery(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    public void testUpdateTokyo() throws ParseException
    {
        checkUpdate(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    public void testUpdatePkTokyo() throws ParseException
    {
        checkUpdateWithPk(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    public void testBatchUpdatePkTokyo() throws ParseException
    {
        checkBatchUpdateWithPk(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    public void testInsertTokyo() throws ParseException
    {
        checkInsert(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    public void testBatchInsertTokyo() throws ParseException
    {
        checkBatchInsert(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    public void testDeleteTokyo() throws ParseException
    {
        checkDelete(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    public void testBatchDeleteTokyo() throws ParseException
    {
        checkBatchDelete(TimeZone.getTimeZone("Asia/Tokyo"));
    }

    public void testRefreshTokyo() throws ParseException
    {
        checkRefresh(TimeZone.getTimeZone("Asia/Tokyo"));
    }

}
