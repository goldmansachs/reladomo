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

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.util.StatisticCounter;
import com.gs.fw.finder.Navigation;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.util.InternalList;

import com.gs.collections.impl.list.mutable.FastList;
import java.util.List;



public class DeepRelationshipUtility
{
    private static final Logger logger = LoggerFactory.getLogger(DeepRelationshipUtility.class);

    private static final DeepRelationshipUtility instance = new DeepRelationshipUtility();

    private static final InternalList EMPTY_INTERNAL_LIST = new InternalList(0);

    protected static int MAX_SIMPLIFIED_IN = 1000;

    private DeepRelationshipUtility()
    {
        // singleton
    }

    public static DeepRelationshipUtility getInstance()
    {
        return instance;
    }

    public static void setMaxSimplifiedIn(int max)
    {
        MAX_SIMPLIFIED_IN = max;
    }

    public static Logger getLogger()
    {
        return logger;
    }

    public static Map<RelatedFinder, StatisticCounter> zAddAllDependentNavigationsStatsForDelete(RelatedFinder finder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        List<RelatedFinder> dependentFinders = finder.getDependentRelationshipFinders();
        for (int i = 0, size = dependentFinders.size(); i < size; i++)
        {
            RelatedFinder dependentFinder = dependentFinders.get(i);
            zAddToNavigationStats(dependentFinder, true, navigationStats);
            if (!navigationStats.containsKey(dependentFinder))
            {
                zAddAllDependentNavigationsStatsForDelete(dependentFinder, navigationStats);
            }
        }
        return navigationStats;
    }

    public static void zAddToNavigationStats(RelatedFinder finder, boolean isHit, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        if (!(finder instanceof Navigation))
        {
            throw new MithraBusinessException("Unexpected: RelatedFinder does not implement Navigation");
        }

        StatisticCounter stat = navigationStats.get(finder);
        if (stat == null)
        {
            stat = new StatisticCounter();
            navigationStats.put(finder, stat);
        }
        stat.registerHit(isHit);
    }
}
