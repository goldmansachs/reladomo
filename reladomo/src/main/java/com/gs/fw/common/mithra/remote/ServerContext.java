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

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.MithraObject;

import javax.transaction.xa.Xid;
import java.io.ObjectOutput;
import java.io.IOException;



public interface ServerContext
{

    public void execute(MithraRemoteResult runnable);

    public void serializeFullData(MithraObject object, ObjectOutput out) throws IOException;

    public ServerCursorExecutor getServerCursorExecutor();
}
