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

package com.gs.fw.common.mithra.tempobject;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;
import com.gs.fw.common.mithra.util.MithraConfigurationManager;
import com.gs.fw.common.mithra.portal.MithraTransactionalPortal;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.transaction.TransactionLocal;
import com.gs.fw.common.mithra.util.ReflectionMethodCache;
import com.gs.fw.common.mithra.util.ThreadLocalRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.lang.reflect.Method;


public class TempContextContainer
{

    private static final Logger logger = LoggerFactory.getLogger(TempContextContainer.class);
    private static final Object[] NO_ARGS = {};

    private String className;
    private MithraConfigurationManager.TempObjectConfig tempConfig;
    private TransactionLocal txLocal = new TransactionLocal();
    private RelatedFinder finderInstance;

    public TempContextContainer(String className)
    {
        this.className = className;
    }

    public void setTempConfig(MithraConfigurationManager.TempObjectConfig tempConfig)
    {
        this.tempConfig = tempConfig;
    }

    protected Object invokeStaticMethod(Class classToInvoke, String methodName)
    {
        try
        {
            Method method = ReflectionMethodCache.getZeroArgMethod(classToInvoke, methodName);
            return method.invoke(null, NO_ARGS);
        }
        catch (Exception e)
        {
            String msg = "Could not invoke method " + methodName + " on class " + classToInvoke;
            logger.error(msg, e);
            throw new MithraException(msg, e);
        }
    }

    private RelatedFinder getFinderInstance()
    {
        if (finderInstance == null)
        {
            String finderClassName = className + "Finder";
            Class finderClass;
            try
            {
                finderClass = Class.forName(finderClassName);
                finderInstance = (RelatedFinder) invokeStaticMethod(finderClass, "getFinderInstance");
            }
            catch (ClassNotFoundException e)
            {
                throw new MithraBusinessException("could not find class "+finderClassName, e);
            }
        }
        return finderInstance;
    }

    public MithraObjectPortal getMithraObjectPortal()
    {
        MithraTemporaryContext mtc = getCurrentContext();
        if (mtc == null)
        {
            throw new MithraBusinessException("no temporary context found for "+className);
        }
        return mtc.getMithraObjectPortal();
    }

    public MithraTemporaryContext getCurrentContext()
    {
        MithraTemporaryContext mtc = (MithraTemporaryContext) ThreadLocalRegistry.getInstance().getThreadLocalValueFor(this);
        if (mtc == null)
        {
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            if (tx != null)
            {
                mtc = (MithraTemporaryContext) txLocal.get(tx);
            }
        }
        else if (mtc.isDestroyed())
        {
            ThreadLocalRegistry.getInstance().clear(this);
            mtc = null;
        }
        return mtc;
    }

    public TemporaryContext createTemporaryContext()
    {
        MithraTemporaryContext mtc = (MithraTemporaryContext)  ThreadLocalRegistry.getInstance().getThreadLocalValueFor(this);
        assertNull(mtc);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx != null)
        {
            mtc = (MithraTemporaryContext) txLocal.get(tx);
            assertNull(mtc);
            mtc = new MithraTxTempContext(this, tx);
            txLocal.set(tx, mtc);
            initContext(mtc);
        }
        else
        {
            mtc = new MithraNonTxTempContext(this);
            ThreadLocalRegistry.getInstance().setThreadLocalValueFor(this, mtc);
            initContext(mtc);
        }
        return mtc;
    }

    public TemporaryContext createTemporaryContext(Object sourceAttribute)
    {
        MithraTempContextWithSourceAttribute mtc = (MithraTempContextWithSourceAttribute) ThreadLocalRegistry.getInstance().getThreadLocalValueFor(this);
        if (mtc != null && !mtc.isDestroyed())
        {
            return mtc.addSourceAttribute(sourceAttribute);
        }
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx != null)
        {
            mtc = (MithraTempContextWithSourceAttribute) txLocal.get(tx);
            if (mtc != null && !mtc.isDestroyed())
            {
                ThreadLocalRegistry.getInstance().setThreadLocalValueFor(this, mtc);
                return mtc.addSourceAttribute(sourceAttribute);
            }
        }
        mtc = new MithraTempContextWithSourceAttribute(this);
        ThreadLocalRegistry.getInstance().setThreadLocalValueFor(this, mtc);
        
        initContext(mtc);
        return mtc.addSourceAttribute(sourceAttribute);
    }

    private void initContext(MithraTemporaryContext mtc)
    {
        init();
        ArrayList errors = new ArrayList(0);
        MithraDatabaseObject dbObject = tempConfig.createDatabaseObject(errors);
        if (dbObject != null)
        {
            Cache cache = ((MithraObjectDeserializer)dbObject).instantiatePartialCache(tempConfig);
            RelatedFinder finder = getFinderInstance();
            MithraObjectPortal objectPortal = new MithraTransactionalPortal((MithraObjectDeserializer) dbObject, cache, finder,
                    tempConfig.getRelationshipCacheSize(), tempConfig.getMinQueriesToKeep(), null,null, null, finder.getHierarchyDepth(),
                    (MithraObjectPersister) dbObject);
            objectPortal.setForTempObject(true);
            objectPortal.setPersisterId(tempConfig.getPersisterId());
            if (!tempConfig.getUseMultiUpdate())
            {
                objectPortal.setUseMultiUpdate(tempConfig.getUseMultiUpdate());
            }
            mtc.init(objectPortal, (MithraTemporaryDatabaseObject) dbObject);
        }
        else
        {
            int size =  errors.size();
            for (int i = 0; i < size; i++)
            {
                logger.error(errors.get(i).toString());
            }
            throw new MithraBusinessException("could not get database object for "+this.className+" see above for errors");
        }
    }

    private synchronized void init()
    {
        if (tempConfig == null)
        {
            MithraManagerProvider.getMithraManager().initializePortal(this.className); //sets the tempConfig
            if (tempConfig == null)
            {
                throw new MithraBusinessException("Could not find configuration for "+className.substring(className.lastIndexOf('.')+1)+
                        ". Did you forget to add it to the configuration XML?");
            }
        }
    }

    private void assertNull(MithraTemporaryContext mtc)
    {
        if (mtc != null && !mtc.isDestroyed())
        {
            throw new MithraBusinessException("there is already a temporary context for "+className);
        }
    }

    protected void clearCurrentContext()
    {
        // this is only called for the thread local versions
        ThreadLocalRegistry.getInstance().clear(this);
    }

    protected void associateToCurrentThread(MithraTemporaryContext context)
    {
        MithraTemporaryContext mtc = (MithraTemporaryContext)  ThreadLocalRegistry.getInstance().getThreadLocalValueFor(this);
        assertNull(mtc);
        ThreadLocalRegistry.getInstance().setThreadLocalValueFor(this, context);
    }

    protected void clearTxContext(MithraTransaction tx)
    {
        this.txLocal.remove(tx);
    }

    public String getClassName()
    {
        return className;
    }

    public void associateSourceToTransaction(MithraTempContextWithSourceAttribute context, MithraTransaction tx)
    {
        MithraTempContextWithSourceAttribute mtc = (MithraTempContextWithSourceAttribute) txLocal.get(tx);
        if (mtc != null && mtc != context)
        {
            throw new MithraBusinessException("mixing temporary contexts in multiple transactions is not supported");
        }
        if (mtc == null) this.txLocal.set(tx, context);
    }
}
