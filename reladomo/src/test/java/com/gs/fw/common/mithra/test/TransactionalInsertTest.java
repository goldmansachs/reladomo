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

package com.gs.fw.common.mithra.test;


import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.domain.nvmdev.NvmDevNetworkDevice;
import com.gs.fw.common.mithra.test.domain.nvmdev.NvmDevNetworkDeviceFinder;
import com.gs.fw.common.mithra.test.domain.nvmdev.NvmDevVirtualLan;
import com.gs.fw.common.mithra.test.domain.nvmdev.NvmDevVirtualLanFinder;
import java.sql.SQLException;
import junit.framework.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalInsertTest extends MithraTestAbstract
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalInsertTest.class);

    @Override
    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            NvmDevNetworkDevice.class,
            NvmDevVirtualLan.class
        };
    }

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        this.createForeignKey();
    }

    @Override
    protected void tearDown() throws Exception
    {
        this.dropForeignKey();
        super.tearDown();
    }

    public void testCascadeInsert() throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            @Override
            public Object executeTransaction(final MithraTransaction tx)
            {
                NvmDevNetworkDevice networkDevice = new NvmDevNetworkDevice();
                networkDevice.setHostname("someHostName");
                networkDevice.setManagementIpAddress("some.ip.addr.ess");
                networkDevice.setNetworkDeviceId(1);

                NvmDevVirtualLan vlan1 = new NvmDevVirtualLan();
                vlan1.setVirtualLanId(1);
                vlan1.setNumber(1);
                // The MithraBusinessException is thrown during the execution of the line below.
                // The error is "Can't add to an operation based list. Make a copy of the list first."
                networkDevice.getVirtualLans().add(vlan1);

                networkDevice.cascadeInsert();
                return null;
            }
        });
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            @Override
            public Object executeTransaction(final MithraTransaction tx)
            {
                NvmDevNetworkDevice networkDevice = new NvmDevNetworkDevice();
                networkDevice.setHostname("someHostName");
                networkDevice.setManagementIpAddress("some.ip.addr.ess");
                networkDevice.setNetworkDeviceId(2);

                NvmDevVirtualLan vlan1 = new NvmDevVirtualLan();
                vlan1.setVirtualLanId(2);
                vlan1.setNumber(2);
                // The MithraBusinessException is thrown during the execution of the line below.
                // The error is "Can't add to an operation based list. Make a copy of the list first."
                networkDevice.getVirtualLans().add(vlan1);

                networkDevice.cascadeInsert();
                return null;
            }
        });
        Assert.assertEquals(2, NvmDevNetworkDeviceFinder.findMany(NvmDevNetworkDeviceFinder.all()).size());
        Assert.assertEquals(2, NvmDevVirtualLanFinder.findMany(NvmDevVirtualLanFinder.all()).size());
    }

    private void createForeignKey() throws SQLException
    {
        LOGGER.debug("Creating FK");
//        alter table nvm_discovery_virtual_lan add constraint nvm_discovery_virtual_lan_fk_0 foreign key (
//            network_device_id
//        )
//        references nvm_discovery_network_device(
//            network_device_id
//        )
        executeStatement("ALTER TABLE app.nvm_discovery_virtual_lan ADD CONSTRAINT nvm_discovery_virtual_lan_fk_0 "
                + "{FOREIGN KEY (network_device_id) REFERENCES app.nvm_discovery_network_device(network_device_id)}");
    }

    protected void dropForeignKey()
            throws SQLException
    {
        LOGGER.debug("Dropping FK");
        executeStatement("ALTER TABLE app.nvm_discovery_virtual_lan DROP CONSTRAINT nvm_discovery_virtual_lan_fk_0");
    }
}
