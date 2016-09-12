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

package com.gs.fw.common.mithra.util;

import com.gs.fw.common.mithra.MithraObjectPortal;

import java.util.Arrays;


public class PortalArraySet implements PortalSetLikeIdentityList
{

    private int size;
    private MithraObjectPortal[] portals;

    public PortalArraySet()
    {
        this.size = 0;
        this.portals = new MithraObjectPortal[6];
    }

    public PortalArraySet(MithraObjectPortal p1, MithraObjectPortal p2, MithraObjectPortal p3,
            MithraObjectPortal p4, MithraObjectPortal p5)
    {
        this.size = 5;
        this.portals = new MithraObjectPortal[8];
        this.portals[0] = p1;
        this.portals[1] = p2;
        this.portals[2] = p3;
        this.portals[3] = p4;
        this.portals[4] = p5;
    }

    public int size()
    {
        return this.size;
    }

    public MithraObjectPortal get(int i)
    {
        return portals[i];
    }

    public PortalSetLikeIdentityList add(MithraObjectPortal portal)
    {
        for(int i=0;i<size;i++)
        {
            if (portal == portals[i]) return this;
        }
        if (size == portals.length)
        {
            MithraObjectPortal[] newPortals = new MithraObjectPortal[portals.length+4];
            System.arraycopy(portals, 0, newPortals, 0, portals.length);
        }
        portals[size] = portal;
        size++;
        return this;
    }

    public PortalSetLikeIdentityList clear()
    {
        Arrays.fill(portals, 0, this.size, null);
        this.size = 0;
        return this;
    }
}
