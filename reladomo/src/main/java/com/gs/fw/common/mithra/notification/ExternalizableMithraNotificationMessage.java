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

package com.gs.fw.common.mithra.notification;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import com.gs.fw.common.mithra.util.MithraProcessInfo;

import java.io.*;
import java.util.ArrayList;
import java.util.List;



public class ExternalizableMithraNotificationMessage implements Externalizable
{
    private static final Logger logger = LoggerFactory.getLogger(ExternalizableMithraNotificationMessage.class.getName());

    private List<MithraNotificationEvent> notificationEvents;
    private long mithraVmId;
    private long requestorVmId;
    private int mithraVersionId = -1;
    private String ipAddress;
    private String processId;
    private static final int MESSAGE_VERSION = 1;

    public ExternalizableMithraNotificationMessage(List<MithraNotificationEvent> notificationEvents)
    {
        this.notificationEvents = notificationEvents;
        this.mithraVersionId = MithraProcessInfo.getMithraVersionId();
        this.ipAddress = MithraProcessInfo.getHostAddress();
        this.processId = MithraProcessInfo.getPid();
    }

    public ExternalizableMithraNotificationMessage()
    {
        // for externalizable
    }

    public List<MithraNotificationEvent> getNotificationEvents()
    {
        return notificationEvents;
    }

    public long getMithraVmId()
    {
        return mithraVmId;
    }

    public void setMithraVmId(long mithraVmId)
    {
        this.mithraVmId = mithraVmId;
    }

    public long getRequestorVmId()
    {
        return requestorVmId;
    }

    public void setRequestorVmId(long requestorVmId)
    {
        this.requestorVmId = requestorVmId;
    }

    public String getProcessId()
    {
        return processId;
    }

    public String getIpAddress()
    {
        return ipAddress;
    }

    public int getMithraVersionId()
    {
        return mithraVersionId;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        try
        {
            int messageVersion = in.read();
            int tmpVersion = in.read() << 16 | in.read() << 8 | in.read();
            if (messageVersion == 0)
            {
                int high = tmpVersion / 100;
                int mid = (tmpVersion - high * 100) / 10;
                int low = tmpVersion - high * 100 - mid * 10;
                this.mithraVersionId = high << 16 | mid << 8 | low;
            }
            else
            {
                this.mithraVersionId = tmpVersion;
            }
            this.ipAddress = in.readUTF();
            this.processId = in.readUTF();
            this.mithraVmId = in.readLong();
            this.requestorVmId = in.readLong();
            int batchSize = in.readInt();
            this.notificationEvents = new ArrayList<MithraNotificationEvent>(batchSize);

            if (mithraVersionId < (9 << 16 | 2 << 8 ))
            {
                readOldNotifications(batchSize, in);
            }
            else
            {
                ReadableByteArrayInputStream rbais = new ReadableByteArrayInputStream(1024);
                for(int i = 0; i < batchSize; i++)
                {
                    int size = in.readInt();
                    if (mithraVersionId > (9 << 16 | 2 << 8 | 1))
                    {
                        int reverseSize = in.readInt();
                        if (reverseSize != Integer.reverse(size))
                        {
                            throw new IOException("Corrupt stream, got size of "+size);
                        }
                    }
                    rbais.readFromStream(in, size);
                    ObjectInputStream ois = new ObjectInputStream(rbais);
                    MithraNotificationEvent notificationEvent = new MithraNotificationEvent();
                    try
                    {
                        notificationEvent.readObject(ois);
                        this.notificationEvents.add(notificationEvent);
                        ois.close();
                    }
                    catch (IOException e)
                    {
                        logger.debug("Could not read message", e); // we ignore these
                    }
                    catch (ClassNotFoundException e)
                    {
                        logger.debug("Could not read message", e); // we ignore these
                    }
                }
            }
        }
        catch (ClassNotFoundException e)
        {
            logException(e);
            throw e;
        }
        catch (IOException e)
        {
            logException(e);
            throw e;
        }
    }

    private void readOldNotifications(int size, ObjectInput in) throws ClassNotFoundException, IOException
    {
        for(int i = 0; i < size; i++)
        {
            MithraNotificationEvent notificationEvent = new MithraNotificationEvent();
            notificationEvent.readObject(in);
            this.notificationEvents.add(notificationEvent);
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeByte(MESSAGE_VERSION);
        out.writeByte((this.mithraVersionId >> 16) & 0xFF);
        out.writeByte((this.mithraVersionId >> 8) & 0xFF);
        out.writeByte(this.mithraVersionId & 0xFF);
        out.writeUTF(this.ipAddress);
        out.writeUTF(this.processId);
        out.writeLong(this.mithraVmId);
        out.writeLong(this.requestorVmId);
        int batchSize = 0;
        if(notificationEvents != null)
        {
           batchSize = notificationEvents.size();
        }

        out.writeInt(batchSize);
        WrittableByteArrayOutputStream bos = new WrittableByteArrayOutputStream(1024);
        for(int i = 0; i < batchSize; i++)
        {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            notificationEvents.get(i).writeObject(oos);
            oos.flush();
            oos.close();
            out.writeInt(bos.size());
            out.writeInt(Integer.reverse(bos.size()));
            bos.writeToStream(out);
            bos.reset();
        }

    }

    private void logException(Throwable e)
    {
        String msg = "Could not deserialize message.";

        if (this.mithraVersionId > 0)
        {
            int localMithraVersionId = MithraProcessInfo.getMithraVersionId();
            if(localMithraVersionId != this.mithraVersionId)
            {
                msg += " The message was sent from a Mithra process with version id : "+getMithraVersionAsString(this.mithraVersionId)+" and expected version id is: "+this.getMithraVersionAsString(localMithraVersionId);
            }
            else
            {
                msg += " for mithra version: "+getMithraVersionAsString(this.mithraVersionId);
            }

        }
        if(ipAddress != null)
        {
            msg += " Sender ip: "+this.ipAddress;
        }
        if(processId != null)
        {
            msg += " Sender process ID: "+this.processId;
        }
        logger.error(msg, e);
    }

    private String getMithraVersionAsString(int versionId)
    {
        int high = (versionId >> 16) & 0xFF;
        int mid = (versionId >> 8) & 0xFF;
        int low = versionId & 0xFF;
        return high+"."+mid+"."+low;
    }
}
