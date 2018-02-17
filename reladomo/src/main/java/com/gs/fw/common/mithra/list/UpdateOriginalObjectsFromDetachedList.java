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

package com.gs.fw.common.mithra.list;

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import com.gs.fw.finder.Navigation;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;



public class UpdateOriginalObjectsFromDetachedList<T extends MithraTransactionalObject> implements TransactionalCommand
{
    private List<T> toUpdateOrInsertList;
    private List<T> toDeleteList;
    private MithraList<T> list;

    public UpdateOriginalObjectsFromDetachedList(List<T> toUpdateOrInsertList, List<T> toDeleteList, MithraList<T> list)
    {
        this.toUpdateOrInsertList = toUpdateOrInsertList;
        this.toDeleteList = toDeleteList;
        this.list = list;
    }

    public Object executeTransaction(MithraTransaction tx) throws Throwable
    {
        Map<RelatedFinder, StatisticCounter> navigationStats = this.findNavigatedRelationships();
        this.setDetachedModeForUpdateCounters(navigationStats, true);
        refresh(tx, navigationStats);
        update();
        delete();
        insert();
        this.setDetachedModeForUpdateCounters(navigationStats, false);
        return null;
    }

    private Map<RelatedFinder, StatisticCounter> findNavigatedRelationships()
    {
        Map<RelatedFinder, StatisticCounter> navigationStats = UnifiedMap.newMap();
        if (this.toUpdateOrInsertList != null)
        {
            for (int i = 0, size = this.toUpdateOrInsertList.size(); i < size; i++)
            {
                T obj = this.toUpdateOrInsertList.get(i);
                obj.zAddNavigatedRelationshipsStats(obj.zGetPortal().getFinder(), navigationStats);
            }
        }
        if (this.toDeleteList != null)
        {
            for (int i = 0, size = this.toDeleteList.size(); i < size; i++)
            {
                T obj = this.toDeleteList.get(i);
                obj.zAddNavigatedRelationshipsStats(obj.zGetPortal().getFinder(), navigationStats);
            }
        }
        return retainOnlyStatsWithHits(navigationStats);
    }

    public static Map<RelatedFinder, StatisticCounter> retainOnlyStatsWithHits(Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        for (Iterator<RelatedFinder> finderIterator  = navigationStats.keySet().iterator(); finderIterator.hasNext();)
        {
            StatisticCounter stat = navigationStats.get(finderIterator.next());
            if (stat.getHits() == 0)
            {
                finderIterator.remove();
            }
        }
        return navigationStats;
    }

    private void setDetachedModeForUpdateCounters(Map<RelatedFinder, StatisticCounter> navigationStats, boolean detachedMode)
    {
        for (RelatedFinder navigatedRelationship : navigationStats.keySet())
        {
            navigatedRelationship.getMithraObjectPortal().getPerClassUpdateCountHolder().setUpdateCountDetachedMode(detachedMode);
        }
    }

    private void insert()
    {
        if (toUpdateOrInsertList != null)
        {
            for(int i=0;i<toUpdateOrInsertList.size();i++)
            {
                MithraTransactionalObject obj = toUpdateOrInsertList.get(i);
                if (!obj.zIsDetached())
                {
                    obj.copyDetachedValuesToOriginalOrInsertIfNew();
                }
            }
        }
    }

    private void delete()
    {
        if (this.toDeleteList != null)
        {
            for(int i=0;i<toDeleteList.size();i++)
            {
                MithraTransactionalObject obj = toDeleteList.get(i);
                obj.copyDetachedValuesToOriginalOrInsertIfNew();
            }
        }
    }

    private void update()
    {
        if (toUpdateOrInsertList != null)
        {
            for(int i=0;i<toUpdateOrInsertList.size();i++)
            {
                MithraTransactionalObject obj = toUpdateOrInsertList.get(i);
                if (obj.zIsDetached())
                {
                    obj.copyDetachedValuesToOriginalOrInsertIfNew();
                }
            }
        }
    }

    private void refresh(MithraTransaction tx, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        if (list != null && (this.toDeleteList != null || this.toUpdateOrInsertList != null))
        {
            boolean optimisticRelevant = navigationStats.isEmpty();
            if (optimisticRelevant)
            {
                Operation op = list.getOperation();
                if (op == null)
                {
                    optimisticRelevant = !list.isEmpty() && list.get(0).zGetPortal().getTxParticipationMode(tx).isOptimisticLocking();
                }
                else
                {
                    optimisticRelevant = op.getResultObjectPortal().getTxParticipationMode(tx).isOptimisticLocking();
                }

            }
            if (!navigationStats.isEmpty())
            {
                for (RelatedFinder navigatedRelationship : navigationStats.keySet())
                {
                    StatisticCounter stats = navigationStats.get(navigatedRelationship);

                    if (this.deepFetchIsEfficientToUse(stats))
                    {
                        this.list.deepFetch((Navigation) navigatedRelationship);
                    }
                }
            }
            if (!optimisticRelevant)
            {
                Operation op = list.getOperation();
                if (op == null)
                {
                    list.forceRefresh();
                }
                else
                {
                    list.forceResolve();
                }
            }
        }
    }

    private static final float HIT_RATE_THRESHOLD = 0.80f;
    private static final int ITEM_COUNT_THRESHOLD = 10000;

    private boolean deepFetchIsEfficientToUse(StatisticCounter stats)
    {
        if (stats.getHits() == 1)
        {
            return false;
        }
        if (stats.getHitRate() > HIT_RATE_THRESHOLD)
        {
            return true;
        }
        if (stats.getTotal() < ITEM_COUNT_THRESHOLD)
        {
            return true;
        }
        return stats.getHits() > 10;
    }
}
