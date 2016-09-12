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

package com.gs.fw.common.mithra.test.cacheloader;


import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.cacheloader.DateCluster;
import com.gs.fw.common.mithra.cacheloader.DateClusterCreator;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.util.List;

public class DateLoadClusterTest extends TestCase
{
    private static final Timestamp PPYE = Timestamp.valueOf("2011-12-30 23:59:00.0");
    private static final Timestamp PYE = Timestamp.valueOf("2012-12-31 23:59:00.0");

    private static final Timestamp PPBD1 = Timestamp.valueOf("2013-05-29 23:59:00.0");
    private static final Timestamp PBD1 = Timestamp.valueOf("2013-05-30 23:59:00.0");

    private static final Timestamp PPBD2 = Timestamp.valueOf("2013-04-25 23:59:00.0");
    private static final Timestamp PBD2 = Timestamp.valueOf("2013-04-26 23:59:00.0");

    public void testEmptyInputListResultsInEmptyOutputList() throws Exception
    {
        List<Timestamp> inputDates = FastList.newListWith();
        List<DateCluster> cluster = DateClusterCreator.createMultiDateClusters(inputDates);
        assertEquals(FastList.newListWith(), cluster);
    }

    public void testSingleInputDateResultsInSingleBusinessDateCluster() throws Exception
    {
        List<Timestamp> inputDates = FastList.newListWith(PPYE);
        assertEquals(FastList.newListWith(newDateCluster(PPYE)), DateClusterCreator.createMultiDateClusters(inputDates));
    }

    public void testTwoDatesWithinDistanceResultInMultiBusinessDateCluster() throws Exception
    {
        List<Timestamp> inputDates = FastList.newListWith(PPBD1, PBD1);
        assertEquals(FastList.newListWith(newDateCluster(PPBD1, PBD1)), DateClusterCreator.createMultiDateClusters(inputDates));
    }

    public void testTwoDatesOutsideDistanceResultInTwoSingleBusinessDateClusters() throws Exception
    {
        List<Timestamp> inputDates = FastList.newListWith(PPYE, PYE);
        assertEquals(FastList.newListWith(newDateCluster(PPYE), newDateCluster(PYE)), DateClusterCreator.createMultiDateClusters(inputDates));
    }

    public void testMultipleMultiBusinessDateClusters() throws Exception
    {
        List<Timestamp> inputDates = FastList.newListWith(PBD2, PPBD1, PPBD2, PBD1);
        assertEquals(FastList.newListWith(newDateCluster(PPBD2, PBD2), newDateCluster(PPBD1, PBD1)), DateClusterCreator.createMultiDateClusters(inputDates));
    }

    public void testSingleAndMultiMixture() throws Exception
    {
        List<Timestamp> inputDates = FastList.newListWith(PBD2, PYE, PPBD1, PPYE, PPBD2, PBD1);
        assertEquals(FastList.newListWith(newDateCluster(PPYE), newDateCluster(PYE), newDateCluster(PPBD2, PBD2), newDateCluster(PPBD1, PBD1)),
                DateClusterCreator.createMultiDateClusters(inputDates));
    }

    public void testSingleDateCreation() throws Exception
    {
        List<Timestamp> inputDates = FastList.newListWith(PBD2, PYE, PPBD1, PPYE, PPBD2, PBD1);
        assertEquals(FastList.newListWith(newDateCluster(PPYE), newDateCluster(PYE), newDateCluster(PPBD2), newDateCluster(PBD2), newDateCluster(PPBD1), newDateCluster(PBD1)),
                DateClusterCreator.createSingleDateClusters(inputDates));
    }

    private static DateCluster newDateCluster(Timestamp... dates)
    {
        return new DateCluster(FastList.newListWith(dates));
    }
}
