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

import java.io.Serializable;


public class PersisterId implements Serializable
{

    private final long mithraVmId;
    private final int connectionManagerId;

    public PersisterId(int connectionManagerId)
    {
        this.connectionManagerId = connectionManagerId;
        this.mithraVmId = MithraProcessInfo.getVmId();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PersisterId that = (PersisterId) o;

        return connectionManagerId == that.connectionManagerId && mithraVmId == that.mithraVmId;

    }

    public int hashCode()
    {
        return HashUtil.combineHashes(HashUtil.hash(mithraVmId), connectionManagerId);
    }
}
