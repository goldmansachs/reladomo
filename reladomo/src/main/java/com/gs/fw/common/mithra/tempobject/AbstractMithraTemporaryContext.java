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

package com.gs.fw.common.mithra.tempobject;

import com.gs.fw.common.mithra.MithraObjectPortal;


public abstract class AbstractMithraTemporaryContext implements MithraTemporaryContext
{

    private MithraObjectPortal portal;
    private MithraTemporaryDatabaseObject dbObject;
    private TempContextContainer container;
    private boolean isDestroyed;
    private boolean isSingleThreaded = false;

    protected AbstractMithraTemporaryContext(TempContextContainer container)
    {
        this.container = container;
    }

    protected TempContextContainer getContainer()
    {
        return container;
    }

    public boolean isDestroyed()
    {
        return isDestroyed;
    }

    protected void setDestroyed(boolean destroyed)
    {
        isDestroyed = destroyed;
    }

    public void init(MithraObjectPortal portal, MithraTemporaryDatabaseObject dbObject)
    {
        this.portal = portal;
        this.dbObject = dbObject;
        this.createTable();
    }

    protected abstract void createTable();

    protected MithraTemporaryDatabaseObject getDbObject()
    {
        return dbObject;
    }

    public MithraObjectPortal getMithraObjectPortal()
    {
        return portal;
    }

    @Override
    public void cleanupAndRecreate()
    {
        //nothing to do
    }

    @Override
    public void markSingleThreaded()
    {
        this.isSingleThreaded = true;
    }

    public boolean isSingleThreaded()
    {
        return isSingleThreaded;
    }
}
