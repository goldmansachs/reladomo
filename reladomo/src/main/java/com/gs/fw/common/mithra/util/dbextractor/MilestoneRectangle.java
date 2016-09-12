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

package com.gs.fw.common.mithra.util.dbextractor;

import java.sql.*;
import java.util.*;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.*;
import org.slf4j.*;


/**
 * Represents an object and its milestoning as a geometric shape to facilitate milestone merging. 
 */
public class MilestoneRectangle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MilestoneRectangle.class);
    private static final Comparator<MilestoneRectangle> MILESTONE_RECTANGLE_COMPARATOR = new Comparator<MilestoneRectangle>()
    {
        @Override
        public int compare(MilestoneRectangle o1, MilestoneRectangle o2)
        {
            if (o1.in > o2.in) return -1;
            if (o1.in < o2.in) return 1;
            if (o1.from > o2.from) return -1;
            if (o1.from < o2.from) return 1;
            return 0;
        }
    };

    private final Object data;
    private final long from;
    private final long thru;
    private final long in;
    private final long out;

    public MilestoneRectangle(Object data, long from, long thru, long in, long out)
    {
        this.data = data;
        this.from = from;
        this.thru = thru;
        this.in = in;
        this.out = out;
    }

    private MilestoneRectangle(Object data, AsOfAttribute[] asOfAttributes)
    {
        this(
                data,
                asOfAttributes != null && asOfAttributes.length > 0 ? asOfAttributes[0].getFromAttribute().valueOf(data).getTime() : -1,
                asOfAttributes != null && asOfAttributes.length > 0 ? asOfAttributes[0].getToAttribute().valueOf(data).getTime() : -1,
                asOfAttributes != null && asOfAttributes.length > 1 ? asOfAttributes[1].getFromAttribute().valueOf(data).getTime() : -1,
                asOfAttributes != null && asOfAttributes.length > 1 ? asOfAttributes[1].getToAttribute().valueOf(data).getTime() : -1);
    }

    private void fragment(MilestoneRectangle rect, Stack<MilestoneRectangle> mergeStack)
    {
        this.pushLeftFragment(rect, mergeStack);
        this.pushBottomFragment(rect, mergeStack);
        this.pushTopFragment(rect, mergeStack);
        this.pushRightFragment(rect, mergeStack);
    }

    private void pushLeftFragment(MilestoneRectangle rect, Stack<MilestoneRectangle> mergeStack)
    {
        if (this.from < rect.from)
        {
            mergeStack.push(new MilestoneRectangle(this.data, this.from, rect.from, this.in, this.out));
        }
    }

    private void pushBottomFragment(MilestoneRectangle rect, Stack<MilestoneRectangle> mergeStack)
    {
        if (this.in < rect.in)
        {
            mergeStack.push(new MilestoneRectangle(this.data, Math.max(this.from, rect.from), Math.min(this.thru, rect.thru), this.in, rect.in));
        }
    }

    private void pushTopFragment(MilestoneRectangle rect, Stack<MilestoneRectangle> mergeStack)
    {
        if (this.out > rect.out)
        {
            mergeStack.push(new MilestoneRectangle(this.data, Math.max(this.from, rect.from), Math.min(this.thru, rect.thru), rect.out, this.out));
        }
    }

    private void pushRightFragment(MilestoneRectangle rect, Stack<MilestoneRectangle> mergeStack)
    {
        if (this.thru > rect.thru)
        {
            mergeStack.push(new MilestoneRectangle(this.data, rect.thru, this.thru, this.in, this.out));
        }
    }

    public boolean intersects(MilestoneRectangle that)
    {
        return intersects(this.from, this.thru, that.from, that.thru) && intersects(this.in, this.out, that.in, that.out);
    }

    private static boolean intersects(long a1, long b1, long a2, long b2)
    {
        if (a1 < 0 && b1 < 0 && a1 < 0 && b2 < 0) return true;
        if (a1 >= b2) return false;
        if (b1 <= a2) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return String.valueOf('[') + this.data + ',' + this.from + ',' + this.thru + ',' + this.in + ',' + this.out + ']';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        MilestoneRectangle that = (MilestoneRectangle) o;

        if (from != that.from)
        {
            return false;
        }
        if (in != that.in)
        {
            return false;
        }
        if (out != that.out)
        {
            return false;
        }
        if (thru != that.thru)
        {
            return false;
        }
        if (data != null ? !data.equals(that.data) : that.data != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = data != null ? data.hashCode() : 0;
        result = 31 * result + (int) (from ^ (from >>> 32));
        result = 31 * result + (int) (thru ^ (thru >>> 32));
        result = 31 * result + (int) (in ^ (in >>> 32));
        result = 31 * result + (int) (out ^ (out >>> 32));
        return result;
    }

    public static List<MilestoneRectangle> fromMithraData(RelatedFinder finder, List mithraData)
    {
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        List<MilestoneRectangle> rectangles = FastList.newList();
        for (Object data : mithraData)
        {
            rectangles.add(new MilestoneRectangle(data, asOfAttributes));
                   
        }
        if (MilestoneRectangle.merge(rectangles).size() != rectangles.size())
        {
            MithraDataObject dataObject = (MithraDataObject) rectangles.get(0).data;
            LOGGER.warn("Invalid milestoning of " + dataObject.getClass().getSimpleName() + '[' + dataObject.zGetPrintablePrimaryKey() + ']');
        }
        return rectangles;
    }

    public static List<MithraDataObject> toMithraData(RelatedFinder finder, List<MilestoneRectangle> rectangles)
    {
        List<MithraDataObject> mithraDataObjects = FastList.newList();
        for (MilestoneRectangle rectangle : rectangles)
        {
            mithraDataObjects.add(rectangle.getMithraDataCopyWithNewMilestones(finder));
        }
        return mithraDataObjects;
    }

    public Object getOriginalMithraData()
    {
        return this.data;
    }

    public MithraDataObject getMithraDataCopyWithNewMilestones(RelatedFinder finder)
    {
        MithraDataObject data = ((MithraDataObject) this.data).copy();
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        if (asOfAttributes != null)
        {
            asOfAttributes[0].getFromAttribute().setValue(data, new Timestamp(this.from));
            asOfAttributes[0].getToAttribute().setValue(data, new Timestamp(this.thru));
            if (asOfAttributes.length > 1)
            {
                asOfAttributes[1].getFromAttribute().setValue(data, new Timestamp(this.in));
                asOfAttributes[1].getToAttribute().setValue(data, new Timestamp(this.out));
            }
        }
        return data;
    }

    public static void merge(Object newObjectOrList, Object existingObjectOrList, RelatedFinder finder, List<MithraDataObject> mergedData)
    {
        List<MilestoneRectangle> dataToMerge = FastList.newList();
        addRectangles(newObjectOrList, finder, dataToMerge);
        addRectangles(existingObjectOrList, finder, dataToMerge);
        mergedData.addAll(toMithraData(finder, merge(dataToMerge)));
    }

    /*
     * Takes a list of rectangles and cleans up any overlapping ranges by giving precedence to rectangles closest to
     * the start of the list and fragmenting those towards the end of the list where necessary. It is assumed that the 
     * data objects in each rectangle share the same dateless primary key. Any data differences will be handled by
     * giving precedence to objects nearest the start of the list.
     */
    public static List<MilestoneRectangle> merge(List<MilestoneRectangle> input)
    {
        Stack<MilestoneRectangle> mergeStack = new Stack<MilestoneRectangle>();
        for (MilestoneRectangle rectangle : input)
        {
            if (rectangle.isValid())
            {
                mergeStack.push(rectangle);
            }
            else
            {
                MithraDataObject dataObject = (MithraDataObject) rectangle.data;
                LOGGER.warn("Invalid milestoning of " + dataObject.getClass().getSimpleName() + '[' + dataObject.zGetPrintablePrimaryKey() + ']');
            }
        }
        List<MilestoneRectangle> merged = FastList.newList(input.size());
        while (!mergeStack.isEmpty())
        {
            MilestoneRectangle next = mergeStack.pop();

            boolean fragmented = false;
            for(int i = mergeStack.size() - 1; !fragmented && i >=0; i--)
            {
                MilestoneRectangle rect = mergeStack.get(i);
                if (next.intersects(rect))
                {
                    next.fragment(rect, mergeStack);
                    fragmented = true;
                }
            }
            if (!fragmented)
            {
                merged.add(next);
            }
        }
        return merged;
    }

    private boolean isValid()
    {
        boolean fromValid = (this.from < 0 && this.thru < 0) ? true : this.from < this.thru;
        boolean inValid = (this.in < 0 && this.out < 0) ? true : this.in < this.out;
        return fromValid && inValid;
    }

    private static void addRectangles(Object objectOrList, RelatedFinder finder, List<MilestoneRectangle> dataToMerge)
    {
        if (objectOrList instanceof List)
        {
            List<MilestoneRectangle> rectangles = fromMithraData(finder, (List) objectOrList);
            Collections.sort(rectangles, MILESTONE_RECTANGLE_COMPARATOR);
            dataToMerge.addAll(rectangles);
        }
        else if (objectOrList != null)
        {
            dataToMerge.add(new MilestoneRectangle(objectOrList, finder.getAsOfAttributes()));
        }
    }

    private static List<MilestoneRectangle> fromMithraObjects(RelatedFinder finder, List<? extends MithraDatedObject> mithraObjects)
    {
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        List<MilestoneRectangle> rectangles = FastList.newList();
        for (MithraDatedObject object : mithraObjects)
        {
            rectangles.add(new MilestoneRectangle(object.zGetCurrentData(), asOfAttributes));

        }
        if (MilestoneRectangle.merge(rectangles).size() != rectangles.size())
        {
            MithraDataObject dataObject = (MithraDataObject) rectangles.get(0).data;
            LOGGER.warn("Invalid milestoning of " + dataObject.getClass().getSimpleName() + '[' + dataObject.zGetPrintablePrimaryKey() + ']');
        }
        return rectangles;
    }

    public static List<MithraDatedObject> toMithraObjects(RelatedFinder finder, List<MilestoneRectangle> rectangles)
    {
        List<MithraDataObject> mithraDataObjects = toMithraData(finder, rectangles);

        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        Timestamp[] asOfValues = new Timestamp[asOfAttributes.length];
        List<MithraDatedObject> mithraObjects = FastList.newList(mithraDataObjects.size());
        for (MithraDataObject data : mithraDataObjects)
        {
			for(int i=0;i<asOfAttributes.length;i++)
            {
                asOfValues[i] = asOfAttributes[i].getInfinityDate();
            }

			MithraDatedObject object = finder.getMithraObjectPortal().getMithraDatedObjectFactory()
			        .createObject(data, asOfValues);
            mithraObjects.add(object);
        }
        return mithraObjects;
    }

    public static List<MithraDatedObject> mergeObjects(List<? extends MithraDatedObject> newObjectList,
	        List<? extends MithraDatedObject> existingObjectList, RelatedFinder finder)
    {
        List<MilestoneRectangle> dataToMerge = FastList.newList();
        addRectanglesFromObjects(newObjectList, finder, dataToMerge);
        addRectanglesFromObjects(existingObjectList, finder, dataToMerge);
        return toMithraObjects(finder, merge(dataToMerge));
    }

    private static void addRectanglesFromObjects(List<? extends MithraDatedObject> objectList, RelatedFinder finder, List<MilestoneRectangle> dataToMerge)
    {
        List<MilestoneRectangle> rectangles = fromMithraObjects(finder, objectList);
        Collections.sort(rectangles, MILESTONE_RECTANGLE_COMPARATOR);
        dataToMerge.addAll(rectangles);
    }
}
