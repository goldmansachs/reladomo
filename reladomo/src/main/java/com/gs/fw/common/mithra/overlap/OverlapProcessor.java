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

package com.gs.fw.common.mithra.overlap;


import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import com.gs.fw.common.mithra.util.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class OverlapProcessor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(com.gs.fw.common.mithra.overlap.OverlapProcessor.class.getName());

    private final String source;
    private final MithraObjectPortal mithraObjectPortal;
    private final OverlapHandler overlapHandler;
    private Operation operation;

    public OverlapProcessor(MithraObjectPortal mithraObjectPortal, String source, OverlapHandler overlapHandler)
    {
        this.source = source;
        this.mithraObjectPortal = mithraObjectPortal;
        this.overlapHandler = overlapHandler;
    }

    public OverlapProcessor(MithraObjectPortal mithraObjectPortal, String source, OverlapHandler overlapHandler, Operation operation)
    {
        this(mithraObjectPortal, source, overlapHandler);
        this.operation = operation;
    }

    public void process()
    {
        AsOfAttribute[] asOfAttributes = this.mithraObjectPortal.getFinder().getAsOfAttributes();
        if (asOfAttributes == null)
        {
            throw new IllegalArgumentException("No asOf attributes for " + this.mithraObjectPortal.getBusinessClassName());
        }
        try
        {
            this.overlapHandler.overlapProcessingStarted(this.getConnectionManager(), this.mithraObjectPortal.getBusinessClassName());
            this.detectOverlaps(this.getConnectionManager(), asOfAttributes);
            this.overlapHandler.overlapProcessingFinished(this.getConnectionManager(), this.mithraObjectPortal.getBusinessClassName());
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    private Object getConnectionManager()
    {
        return this.mithraObjectPortal.getDatabaseObject().getConnectionManager();
    }

    private void detectOverlaps(Object connectionManager, AsOfAttribute[] asOfAttributes) throws SQLException
    {
        long startTime = System.currentTimeMillis();
        List<MithraDataObject> dataObjects = loadMithraDataObjects();
        long loadTime = System.currentTimeMillis() - startTime;
        LOGGER.info("Loaded " + dataObjects.size() + " records in " + loadTime + "ms");

        Attribute[] primaryKeyAttributes = this.mithraObjectPortal.getFinder().getPrimaryKeyAttributes();
        OrderBy orderBy = null;
        for (Attribute attribute : primaryKeyAttributes)
        {
            orderBy = orderBy == null ? attribute.ascendingOrderBy() : orderBy.and(attribute.ascendingOrderBy());
        }
        Collections.sort(dataObjects, orderBy);

        MithraDataObject previousDataObject = null;
        List<MithraDataObject> primaryKeyMatches = FastList.newList();
        for (MithraDataObject dataObject : dataObjects)
        {
            if (previousDataObject != null && !dataObject.hasSamePrimaryKeyIgnoringAsOfAttributes(previousDataObject))
            {
                detectAndReportOverlaps(connectionManager, asOfAttributes, primaryKeyMatches);
                primaryKeyMatches.clear();
            }
            primaryKeyMatches.add(dataObject);
            previousDataObject = dataObject;
        }
        if (!primaryKeyMatches.isEmpty())
        {
            detectAndReportOverlaps(connectionManager, asOfAttributes, primaryKeyMatches);
        }
    }

    private List<MithraDataObject> loadMithraDataObjects() throws SQLException
    {
        RelatedFinder finder = this.mithraObjectPortal.getFinder();
        MithraList mithraDataList = finder.findManyBypassCache(this.operation==null?equalsEdgePoinOperation():this.operation);
        final List<MithraDataObject> mithraDataObjects = new FastList<MithraDataObject>();
        mithraDataList.forEachWithCursor(
                new DoWhileProcedure()
                {
                    @Override
                    public boolean execute(Object object)
                    {
                        mithraDataObjects.add(((MithraObject) object).zGetCurrentData().copy());
                        return true;
                    }
                },
                new Filter()
                {
                    @Override
                    public boolean matches(Object o)
                    {
                        return true;
                    }
                }
        );
        return mithraDataObjects;
    }

    private Operation equalsEdgePoinOperation()
    {
        AsOfAttribute[] asOfAttributes = this.mithraObjectPortal.getFinder().getAsOfAttributes();
        Operation op = asOfAttributes[0].equalsEdgePoint();
        for(int i=1;i<asOfAttributes.length;i++)
        {
            op = op.and(asOfAttributes[i].equalsEdgePoint());
        }
        return op;
    }

    private void detectAndReportOverlaps(Object xaConnectionManager, AsOfAttribute[] asOfAttributes, List<MithraDataObject> primaryKeyMatches)
    {
        List<MithraDataObject> overlaps = collectOverlaps(primaryKeyMatches, asOfAttributes);
        if (!overlaps.isEmpty())
        {
            this.overlapHandler.overlapsDetected(xaConnectionManager, overlaps, this.mithraObjectPortal.getBusinessClassName());
        }
    }

    private static List<MithraDataObject> collectOverlaps(List<MithraDataObject> primaryKeyMatches, AsOfAttribute[] asOfAttributes)
    {
        Set<MithraDataObject> overlapSet = UnifiedSet.newSet();
        for (int i = 0; i < primaryKeyMatches.size(); i++)
        {
            MithraDataObject object = primaryKeyMatches.get(i);
            if (!AsOfAttribute.isMilestoningValid(object, asOfAttributes))
            {
                overlapSet.add(object);
            }
        }
        for (int i = 0; i < primaryKeyMatches.size() - 1; i++)
        {
            for (int j = i + 1; j < primaryKeyMatches.size(); j++)
            {
                MithraDataObject object1 = primaryKeyMatches.get(i);
                MithraDataObject object2 = primaryKeyMatches.get(j);
                if (AsOfAttribute.isMilestoningOverlap(object1, object2, asOfAttributes))
                {
                    overlapSet.add(object1);
                    overlapSet.add(object2);
                }
            }
        }
        return FastList.newList(overlapSet);
    }
}
