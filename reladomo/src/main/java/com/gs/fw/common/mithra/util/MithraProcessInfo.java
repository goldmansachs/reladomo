
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.net.InetAddress;

public final class MithraProcessInfo
{
    static private final Logger logger = LoggerFactory.getLogger(MithraProcessInfo.class.getName());

    private static final int MITHRA_VERSION_ID = MithraVersion.getMithraVersion();
    private static String PID = null;
    private static short PID_SHORT;
    private static String HOST_ADDRESS = null;
    private static byte[] IP_ADDRESS = null;
    private static long STARTUP_TIME = System.nanoTime();
    private static long MITHRA_VM_ID;
    private static boolean isVmIdGenerated;

    public static String getHostAddress()
    {
        if(HOST_ADDRESS == null)
        {
            String address = null;
            try
            {
                address = InetAddress.getLocalHost().getHostAddress();
            }
            catch (UnknownHostException e)
            {
                logger.warn("could not get host address", e);
            }
            HOST_ADDRESS = address;
        }
        return HOST_ADDRESS;
    }

    public static byte[] getIpAddress()
    {
        if(IP_ADDRESS == null)
        {
            byte[] rawBytes = null;
            try
            {
                rawBytes = InetAddress.getLocalHost().getAddress();
            }
            catch (UnknownHostException e)
            {
                logger.warn("could not get IP address");
                rawBytes = new byte[4];
                for(int i=0;i<rawBytes.length;i++)
                {
                    rawBytes[i] = (byte)(Math.random()*Byte.MAX_VALUE*2-Byte.MAX_VALUE);
                }
            }
            IP_ADDRESS = rawBytes;
        }
        return IP_ADDRESS;
    }

    public static String getPid()
    {
        if(PID == null)
        {
            String pid = ManagementFactory.getRuntimeMXBean().getName();
            if (pid != null)
            {
                int atIndex = pid.indexOf("@");
                if (atIndex > 0)
                {
                    pid = pid.substring(0, atIndex);
                }
            }
            else pid = "0";
            PID = pid;
        }
        return PID;
    }

    public static synchronized short getPidAsShort()
    {
        if (PID_SHORT == 0)
        {
            short pid = 0;
            String pidString = getPid();
            try
            {
                int parsedPid = Integer.parseInt(pidString);
                if (parsedPid > 32768) parsedPid -= 32768;
                pid = (short) parsedPid;
            }
            catch (NumberFormatException e)
            {
                // ignore
            }
            if (pid == 0)
            {
                pid = (short)((Math.random()*Short.MAX_VALUE*2 - Short.MAX_VALUE)+1);
            }
            PID_SHORT = pid;
        }
        return PID_SHORT;
    }

    public synchronized static long getVmId()
    {
        if(!isVmIdGenerated)
        {
            generateMithraVmId();
        }
        return MITHRA_VM_ID;
    }

    private static void generateMithraVmId()
    {
        long initializationTime = (STARTUP_TIME >> 16); // the first 16 bits of nano time are not that interesting

        long clientIp = getIpAsLong();

        MITHRA_VM_ID = getPidAsShort() | (clientIp << 16) | (initializationTime << 48);
        isVmIdGenerated = true;

        if(logger.isDebugEnabled())
        {
            logger.debug("Mithra VM ID generated: "+MITHRA_VM_ID);
        }
    }

    public static long getIpAsLong()
    {
        byte[] tmp = MithraProcessInfo.getIpAddress();
        long clientIp = (long) tmp[0] & 0xFF | ((long) tmp[1] & 0xFF) << 8 | ((long) tmp[2] & 0xFF) << 16
                    | ((long) tmp[3] & 0xFF) << 24;
        return clientIp;
    }

    public static int getMithraVersionId()
    {
        return MITHRA_VERSION_ID;
    }

    public static void main(String[] args)
    {
        System.out.println(Long.toHexString(getVmId()));
    }
}
