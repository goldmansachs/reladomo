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

package com.gs.fw.common.mithra.test.multivm;



public interface MultiVmTest
{

    public RemoteWorkerVm getRemoteWorkerVm();

    public void setRemoteWorkerVm(RemoteWorkerVm remoteWorkerVm);

    public void workerVmOnStartup();

    public void workerVmSetUp();

    public void workerVmTearDown();

    public void setApplicationPorts(int port1, int port2);
}
