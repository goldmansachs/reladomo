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


import java.io.PrintStream;
import java.util.List;

import com.gs.fw.common.mithra.MithraDataObject;

public class OverlapReporter implements OverlapHandler
{
    private final PrintStream printStream;
    private long startTime;
    private int count;

    public OverlapReporter(PrintStream printStream)
    {
        this.printStream = printStream;
    }

    @Override
    public void overlapProcessingStarted(Object connectionManager, String mithraClassName)
    {
        this.printStream.println("Looking for overlaps for " + mithraClassName + ".");
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void overlapProcessingFinished(Object connectionManager, String mithraClassName)
    {
        this.printStream.println("Found " + this.count + " overlaps for " + mithraClassName + " in " + (System.currentTimeMillis() - this.startTime) + "ms.");
    }

    @Override
    public void overlapsDetected(Object connectionManager, List<MithraDataObject> overlaps, String mithraClassName)
    {
        for (MithraDataObject overlap : overlaps)
        {
            this.printStream.println(overlap.zGetPrintablePrimaryKey());
        }
        this.count += overlaps.size();
    }
}
