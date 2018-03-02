
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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.cache.FullDatedCache;
import com.gs.fw.common.mithra.test.domain.AuditedGroupData;
import com.gs.fw.common.mithra.test.domain.AuditedGroupDatabaseObject;
import com.gs.fw.common.mithra.test.domain.AuditedGroupFinder;
import junit.framework.Assert;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;



public class FullDatedCacheTest extends TestCase
{

    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void testDuplicate() throws Exception
    {
        FullDatedCache cache = new FullDatedCache(AuditedGroupFinder.getPrimaryKeyAttributes(), AuditedGroupFinder.getAsOfAttributes(), new AuditedGroupDatabaseObject(), AuditedGroupFinder.getImmutableAttributes());
        Timestamp processingDateFrom = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp processingDateFromToCreateDuplicate = new Timestamp(timestampFormat.parse("2002-01-03 10:00:00").getTime());
        for (int i = 0; i < 5; i++)
        {
            AuditedGroupData data = AuditedGroupDatabaseObject.allocateOnHeapData();
            data.setId(i);
            data.setProcessingDateFrom(processingDateFrom);
            data.setProcessingDateTo(AuditedGroupFinder.processingDate().getInfinityDate());

            cache.putDatedData(data);
        }
        for (int i = 0; i < 5; i++)
        {
            AuditedGroupData data = AuditedGroupDatabaseObject.allocateOnHeapData();
            data.setId(i);
            data.setProcessingDateFrom(processingDateFromToCreateDuplicate);
            data.setProcessingDateTo(AuditedGroupFinder.processingDate().getInfinityDate());
            cache.putDatedData(data);
        }
        for (int i = 50; i < 55; i++)
        {
            AuditedGroupData data = AuditedGroupDatabaseObject.allocateOnHeapData();
            data.setId(i);
            data.setProcessingDateFrom(processingDateFrom);
            data.setProcessingDateTo(AuditedGroupFinder.processingDate().getInfinityDate());

            cache.putDatedData(data);
        }
        Assert.assertEquals(15, cache.size());
        List<Object> duplicates = cache.collectMilestoningOverlaps();
        Assert.assertEquals(10, duplicates.size());
        for (int i = 0; i < 10; i = i + 2)
        {
            AuditedGroupData originalData = (AuditedGroupData) duplicates.get(i);
            AuditedGroupData duplicateData = (AuditedGroupData) duplicates.get(i + 1);
            Assert.assertEquals(originalData.getId(), duplicateData.getId());
        }
    }


}