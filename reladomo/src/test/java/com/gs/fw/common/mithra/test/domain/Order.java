
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

package com.gs.fw.common.mithra.test.domain;

import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerialize;


public class Order extends OrderAbstract
{
    public static final String CANCELLED = "cancelled";

    public Order()
    {
        super();
    }

    /* for testing only. equals should not be implemented like this for production code*/
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof OrderAbstract))
        {
            return false;
        }

        final OrderAbstract orderAbstract = (OrderAbstract) obj;

        return !this.zGetNonTxData().changed(orderAbstract.zGetNonTxData());
    }

    @ReladomoSerialize(serialViews = {SerialView.Shorter.class, SerialView.HandPicked.class})
    public int hashCode()
    {
        return this.getOrderId();
    }

    @ReladomoSerialize(serialViews = {SerialView.Longer.class})
    public String getTrackedDescription()
    {
        return this.getDescription()+" "+this.getTrackingId();
    }

    protected void setUserIdAndDescriptionImpl(int userId, String description, MithraTransaction mithraTransaction) throws IllegalArgumentException
    {
        this.setUserId(userId);
        if (description.length() < this.getDescription().length()) throw new IllegalArgumentException("new description must be more descriptive (longer)!");
        this.setDescription(description);
    }

    protected void setUserIdCancelAndFailImpl(int userId, MithraTransaction mithraTransaction)
    {
        this.setUserId(userId);
        this.setTrackingId(null);
        this.cancel();
    }

    protected void cancelImpl(MithraTransaction mithraTransaction)
    {
        this.setStateAndDescription(CANCELLED, CANCELLED);
        if(this.getTrackingId() == null) throw new IllegalArgumentException("tracking id can't be null for cancelled orders");
        this.setTrackingId("void");
    }

    protected void cancelAndFailImpl(MithraTransaction mithraTransaction)
    {
        this.setTrackingId(null);
        this.setStateAndDescription(CANCELLED, CANCELLED);
        if(this.getTrackingId() == null) throw new IllegalArgumentException("tracking id can't be null for cancelled orders");
    }

    protected void setStateAndDescriptionImpl(String state, String description, MithraTransaction mithraTransaction)
    {
        this.setState(state);
        this.setDescription(description);
    }

    protected void cancelOrderAndOrderItemsImpl(MithraTransaction mithraTransaction)
    {
        this.cancel();
        this.cancelOrderItems();
    }

    protected void cancelOrderItemsImpl(MithraTransaction mithraTransaction)
    {
        OrderItemList orderItems = this.getItems();
        for (int i = 0; i < orderItems.size(); i++)
        {
            orderItems.getOrderItemAt(i).setState(CANCELLED);
        }
    }

    public boolean isInMemory()
    {
        return this.isInMemoryAndNotInserted();
    }
}
