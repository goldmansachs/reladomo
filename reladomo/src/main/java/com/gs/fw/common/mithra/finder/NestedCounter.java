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



public class NestedCounter
{

    private int[] max;
    private int[] current;
    private boolean done = false;

    public NestedCounter(int[] max)
    {
        this.max = max;
        this.current = new int[max.length];
        for(int i=0;i<current.length;i++)
        {
            current[i] = 0;
        }
    }

    public boolean isDone()
    {
        return done;
    }

    public void increment()
    {
        incrementByIndex(0);
    }

    public int getCounterAt(int index)
    {
        return this.current[index];
    }

    protected void incrementByIndex(int index)
    {
        current[index]++;
        if (current[index] == max[index])
        {
            current[index] = 0;
            if (index == max.length - 1)
            {
                this.done = true;
            }
            else
            {
                incrementByIndex(index+1);
            }
        }
    }
}
