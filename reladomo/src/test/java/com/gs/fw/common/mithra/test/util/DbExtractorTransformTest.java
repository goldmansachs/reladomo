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

package com.gs.fw.common.mithra.test.util;

import java.io.*;
import java.util.*;

import com.gs.collections.impl.list.mutable.*;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.test.*;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.dbextractor.*;



public class DbExtractorTransformTest extends MithraTestAbstract
{
    public void testMithraTestDataExtractor() throws Exception
	{
        File otherFile = new File(DbExtractorTest.COMPARE_PATH + "DbExtractorTransformTest_other.txt");
        File targetCopy = new File(DbExtractorTest.OUTPUT_PATH + "DbExtractorTransformTest_target.txt");
        targetCopy.delete();
        MithraTestAbstract.copyFile(otherFile, targetCopy);

        DbExtractor extractor = new DbExtractor(targetCopy.getCanonicalPath(), false);

        extractor.setTransformer(new FileTransformer());
        extractor.addDataFrom(DbExtractorTest.COMPARE_PATH + "DbExtractorTransformTest_source.txt");

        extractor.setTransformer(new DatabaseTransformer());
        extractor.addClassToFile(TrialFinder.getFinderInstance(), TrialFinder.trialId().eq("001N"));

        DbExtractorTest.diffFiles(new File(DbExtractorTest.COMPARE_PATH + "DbExtractorTransformTest_expected.txt"), targetCopy);
    }

    private static class DatabaseTransformer implements MithraDataTransformer
    {
        @Override
        public Map<RelatedFinder, List<MithraDataObject>> transform(Map<RelatedFinder, List<MithraDataObject>> allMergedData)
        {
            List<MithraDataObject> mithraDataObjects = allMergedData.get(TrialFinder.getFinderInstance());
            for (MithraDataObject object : mithraDataObjects)
            {
                TrialData trial = TrialData.class.cast(object);
                trial.setTrialId(trial.getTrialId());
                trial.setDescription("Transformed " + trial.getDescription());
            }
            return allMergedData;
        }
    }

    private static class FileTransformer implements MithraDataTransformer
    {
        @Override
        public Map<RelatedFinder, List<MithraDataObject>> transform(Map<RelatedFinder, List<MithraDataObject>> allMergedData)
        {
            allMergedData.put(OrderFinder.getFinderInstance(), FastList.<MithraDataObject>newList());

            // from source file
            List<MithraDataObject> dataObjects = allMergedData.remove(OrderWithExtraColumnFinder.getFinderInstance());
            for (MithraDataObject object : dataObjects)
            {
                OrderWithExtraColumnData dataObject = OrderWithExtraColumnData.class.cast(object);
                OrderData order = new OrderData();
                order.setOrderId(dataObject.getOrderId());
                order.setOrderDate(dataObject.getOrderDate());
                order.setUserId(dataObject.getUserId());
                order.setDescription(dataObject.getDescription());
                order.setState(dataObject.getState());
                order.setTrackingId(dataObject.getTrackingId());
                allMergedData.get(OrderFinder.getFinderInstance()).add(order);
            }
            dataObjects = allMergedData.remove(OrderWithMissingColumnFinder.getFinderInstance());
            for (MithraDataObject object : dataObjects)
            {
                OrderWithMissingColumnData dataObject = OrderWithMissingColumnData.class.cast(object);
                OrderData order = new OrderData();
                order.setOrderId(dataObject.getOrderId());
                order.setOrderDate(dataObject.getOrderDate());
                order.setUserId(dataObject.getUserId());
                order.setDescription(dataObject.getDescription());
                order.setState(dataObject.getState());
                order.setTrackingId("125");
                allMergedData.get(OrderFinder.getFinderInstance()).add(order);
            }

            dataObjects = allMergedData.remove(OrderDenormalizedFinder.getFinderInstance());
            for (MithraDataObject object : dataObjects)
            {
                OrderDenormalizedData dataObject = OrderDenormalizedData.class.cast(object);
                OrderData order = new OrderData();
                order.setOrderId(dataObject.getOrderId());
                order.setOrderDate(dataObject.getOrderDate());
                order.setUserId(dataObject.getUserId());
                order.setDescription(dataObject.getDescription());
                order.setState(dataObject.getState());
                order.setTrackingId(dataObject.getTrackingId());
                allMergedData.get(OrderFinder.getFinderInstance()).add(order);

                OrderItemData orderItem = new OrderItemData();
                orderItem.setId(order.getOrderId());
                orderItem.setOrderId(2);
                orderItem.setProductId(dataObject.getProductId());
                orderItem.setQuantity(dataObject.getQuantity());
                orderItem.setOriginalPrice(dataObject.getOriginalPrice());
                orderItem.setDiscountPrice(dataObject.getDiscountPrice());
                orderItem.setState(order.getState());
                allMergedData.get(OrderItemFinder.getFinderInstance()).add(orderItem);
            }

            List<MithraDataObject> filteredObjects = FastList.newList();
            dataObjects = allMergedData.get(OrderItemFinder.getFinderInstance());
            for (MithraDataObject object : dataObjects)
            {
                if (OrderItemData.class.cast(object).getId() < 3)
                {
                    filteredObjects.add(object);
                }
            }
            allMergedData.put(OrderItemFinder.getFinderInstance(), filteredObjects);

            filteredObjects = FastList.newList();
            dataObjects = allMergedData.get(OrderFinder.getFinderInstance());
            for (MithraDataObject object : dataObjects)
            {
                if (OrderData.class.cast(object).getOrderId() != 2)
                {
                    filteredObjects.add(object);
                }
            }
            allMergedData.put(OrderFinder.getFinderInstance(), filteredObjects);

            return allMergedData;
        }
    }
}