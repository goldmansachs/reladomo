

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

public interface DoUntilProcedure2<T, P>
{
    /**
     * Executes this procedure until it returns true or there is no more work to do.
     *
     * @param object an <code>Object</code> value
     * @return false if the loop must continue.
     */
    public boolean execute(T object, P param);
}
