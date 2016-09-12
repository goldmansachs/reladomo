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

package com.gs.fw.common.mithra.generator;

import com.gs.fw.common.mithra.generator.metamodel.*;
import com.gs.fw.common.mithra.generator.type.JavaTypeException;
import com.gs.fw.common.mithra.generator.util.FullFileBuffer;
import com.gs.fw.common.mithra.generator.util.AutoShutdownThreadExecutor;
import com.gs.fw.common.mithra.generator.util.AwaitingThreadExecutor;
import com.gs.fw.common.mithra.generator.util.ChopAndStickResource;
import com.gs.fw.common.mithra.generator.util.SerialResource;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

public class BaseMithraGenerator
{
    public static final String DATED = "dated";
    public static final String READ_ONLY = "read-only";
    public static final String DATED_READ_ONLY = DATED+READ_ONLY;
    public static final String TRANSACTIONAL = "transactional";
    public static final String DATED_TRANSACTIONAL = DATED+TRANSACTIONAL;
    public static final String EMBEDDED_VALUE = "embedded-value";
    public static final String ENUMERATION = "enumeration";
    public static final String MITHRA_INTERFACE = "mithra-interface";

    private static final int MD5_LENGTH = 32;
    private String md5 = null;
    private CRC32 crc32 = new CRC32();
    protected Logger logger;

    private GenerationLogger generationLogger = null;
    private AwaitingThreadExecutor executor;
    private Throwable executorError;
    private static final int IO_THREADS = 1;

    private String xml;
    private String generatedDir;
    private String nonGeneratedDir;
    private boolean ignoreNonGeneratedAbstractClasses = false;
    private boolean ignoreTransactionalMethods = false;
    private boolean ignorePackageNamingConvention = false;
    private boolean defaultFinalGetters = false;
    private boolean forceOffHeap = false;
    private ThreadLocal<FullFileBuffer> fullFileBufferThreadLocal = new ThreadLocal<FullFileBuffer>();

    private ChopAndStickResource chopAndStickResource = new ChopAndStickResource(new Semaphore(Runtime.getRuntime().availableProcessors()),
            new Semaphore(IO_THREADS), new SerialResource());

    private List<MithraGeneratorImport> imports = new ArrayList<MithraGeneratorImport>();

    private Map<String, MithraObjectTypeWrapper> mithraObjects = new ConcurrentHashMap<String, MithraObjectTypeWrapper>();
    private List<MithraObjectTypeWrapper> sortedMithraObjects;
    private Map<String, MithraEmbeddedValueObjectTypeWrapper> mithraEmbeddedValueObjects = new ConcurrentHashMap<String, MithraEmbeddedValueObjectTypeWrapper>();
    private List<MithraEmbeddedValueObjectTypeWrapper> sortedMithraEmbeddedValueObjects;
    private Map<String, MithraEnumerationTypeWrapper> mithraEnumerations = new ConcurrentHashMap<String, MithraEnumerationTypeWrapper>();
    private List<MithraEnumerationTypeWrapper> sortedMithraEnumerations;
    private Map<String, MithraInterfaceType> mithraInterfaces = new ConcurrentHashMap<String, MithraInterfaceType>();
    // private List<MithraInterfaceTypeWrapper> sortedMithraInterfaces;

    public void setLogger(Logger logger)
    {
        this.logger = logger;
    }

    public void setGenerationLogger(GenerationLogger generationLogger)
    {
        this.generationLogger = generationLogger;
    }

    public GenerationLogger getGenerationLogger()
    {
        if(this.generationLogger == null)
        {
            this.generationLogger = new SimpleGenerationLogger();
        }
        return this.generationLogger;
    }

    private FullFileBuffer getFullFileBuffer()
    {
        FullFileBuffer result = fullFileBufferThreadLocal.get();
        if (result == null)
        {
            result = new FullFileBuffer();
            fullFileBufferThreadLocal.set(result);
        }
        return result;
    }

    public void setIgnorePackageNamingConvention(boolean ignorePackageNamingConvention)
    {
        this.ignorePackageNamingConvention = ignorePackageNamingConvention;
    }

    public void setForceOffHeap(boolean forceOffHeap)
    {
        this.forceOffHeap = forceOffHeap;
    }

    public String getMd5()
    {
        if (this.md5 == null)
        {
            this.md5 = "";
            InputStream is = this.getClass().getClassLoader().getResourceAsStream("com/gs/fw/common/mithra/generator/mithragen.md5");
            if (is != null)
            {
                byte[] md5Bytes = new byte[MD5_LENGTH];
                try
                {
                    this.fullyRead(is, md5Bytes);
                    this.md5 = new String(md5Bytes);
                    is.close();
                    return this.md5;
                }
                catch (IOException e)
                {
                    this.logger.error("got an IOException reading md5 file " + e.getClass().getName() + ": " + e.getMessage());
                }
            }
            else
            {
                this.logger.error("Could not find md5. Will regenerate everything");
            }
        }
        return this.md5;
    }

    private void fullyRead(InputStream is, byte[] bytes) throws IOException
    {
        int read = 0;
        while (read < bytes.length)
        {
            read += is.read(bytes, read, bytes.length - read);
        }
    }

    public String getCrc()
    {
        String result = Long.toHexString(crc32.getValue());
        while(result.length() < 8)
        {
            result = "0" + result;
        }
        return result;
    }

    public List<MithraGeneratorImport> getImports()
    {
        return this.imports;
    }

    public String getXml()
    {
        return this.xml;
    }

    public void setXml(String xml)
    {
        this.xml = xml;
    }

    public String getGeneratedDir()
    {
        return this.generatedDir;
    }

    public void setGeneratedDir(String generatedDir)
    {
        this.generatedDir = generatedDir;
    }

    public String getNonGeneratedDir()
    {
        return this.nonGeneratedDir;
    }

    public void setNonGeneratedDir(String nonGeneratedDir)
    {
        this.nonGeneratedDir = nonGeneratedDir;
    }

    public void setIgnoreNonGeneratedAbstractClasses(boolean ignoreNonGeneratedAbstractClasses)
    {
        this.ignoreNonGeneratedAbstractClasses = ignoreNonGeneratedAbstractClasses;
    }

    public void setIgnoreTransactionalMethods(boolean ignoreTransactionalMethods)
    {
        this.ignoreTransactionalMethods = ignoreTransactionalMethods;
    }

    public boolean isDefaultFinalGetters()
    {
        return this.defaultFinalGetters;
    }

    public void setDefaultFinalGetters(boolean defaultFinalGetters)
    {
        this.defaultFinalGetters = defaultFinalGetters;
    }

    public Map<String, MithraObjectTypeWrapper> getMithraObjects()
    {
        return this.mithraObjects;
    }

    public Map<String, MithraEmbeddedValueObjectTypeWrapper> getMithraEmbeddedValueObjects()
    {
        return this.mithraEmbeddedValueObjects;
    }

    public Map<String, MithraEnumerationTypeWrapper> getMithraEnumerations()
    {
        return this.mithraEnumerations;
    }

    public Map<String, MithraInterfaceType> getMithraInterfaces()
    {
        return this.mithraInterfaces;
    }

    public void parseImportedMithraXml() throws FileNotFoundException
    {
        for (MithraGeneratorImport mithraGeneratorImport : this.imports)
        {
            this.logger.info("using files imported from: " + mithraGeneratorImport.getFilename());
            MithraGeneratorImport.FileProvider fileProvider = mithraGeneratorImport.getFileProvider();
            parseMithraXml(mithraGeneratorImport.getFilename(), fileProvider.getSourceName(), fileProvider);
        }
    }

    public void parseMithraXml(String fileName, String importSource, MithraGeneratorImport.FileProvider fileProvider) throws FileNotFoundException
    {
        MithraType result;
        InputStream mithraFileIs = null;
        try
        {
            MithraGeneratorImport.FileInputStreamWithSize streamWithSize = fileProvider.getFileInputStream(fileName);
            FullFileBuffer ffb = new FullFileBuffer();
            ffb.bufferFile(streamWithSize.getInputStream(), (int) streamWithSize.getSize());
            ffb.updateCrc(crc32);
            mithraFileIs = ffb.getBufferedInputStream();
            Object obj = new MithraGeneratorUnmarshaller().parse(mithraFileIs, fileName);
            this.logger.debug(obj.getClass().getName() + ": " + MithraType.class.getName());
            result = (MithraType)obj;
            long start = System.currentTimeMillis();
            int normalObjects = this.parseMithraObjects(result, importSource, fileProvider);
            int pureObjects = this.parseMithraPureObjects(result, importSource, fileProvider);
            int tempObjects = this.parseMithraTempObjects(result, importSource, fileProvider);
            int embeddedObjects = this.parseMithraEmbeddedValueObjects(result, importSource, fileProvider);
            int enumerations = this.parseMithraEnumerations(result, importSource, fileProvider);
            int mithraInterfaceObjects = this.parseMithraInterfaceObjects(result, importSource, fileProvider);
            String msg = fileName + ": parsed ";
            msg = concatParsed(msg, normalObjects, "normal");
            msg = concatParsed(msg, pureObjects, "pure");
            msg = concatParsed(msg, tempObjects, "temp");
            msg = concatParsed(msg, embeddedObjects, "embedded");
            msg = concatParsed(msg, enumerations, "enumeration");
            msg = concatParsed(msg, mithraInterfaceObjects, "mithraInterface");
            msg += " Mithra objects in "+(System.currentTimeMillis() - start)+" ms.";
            this.logger.info(msg);
        }
        catch (MithraGeneratorParserException e)
        {
            throw new MithraGeneratorException("Unable to parse "+ fileName, e);
        }
        catch (IOException e)
        {
            throw new MithraGeneratorException("Unable to read file "+ fileName, e);
        }
        finally
        {
            closeIs(mithraFileIs);
            fileProvider.close();
        }

        this.createSortedList();
        this.createSortedEmbeddedValueObjectList();
    }

    public boolean extractMithraInterfaceRelationshipsAndSuperInterfaces()
    {
        boolean allGood = true;
        for (MithraInterfaceType mithraInterfaceType : this.mithraInterfaces.values())
        {
            List errors = mithraInterfaceType.extractRelationshipsAndSuperInterfaces(mithraInterfaces, mithraObjects);
            allGood = processErrorsIfAny(errors, allGood, mithraInterfaceType.getSourceFileName());
        }
        return allGood;
    }

    private String concatParsed(String msg, int count, String type)
    {
        if (count > 0)
        {
            msg += count + " " +type+", ";
        }
        return msg;
    }

    private int parseMithraInterfaceObjects(final MithraType mithraType, final String importSource,
                                            final MithraGeneratorImport.FileProvider fileProvider) throws FileNotFoundException
    {
        List<MithraInterfaceResourceType> mithraObjectList = mithraType.getMithraInterfaceResources();
        chopAndStickResource.resetSerialResource();
        for (int i = 0; i < mithraObjectList.size(); i++)
        {
            final MithraInterfaceResourceType mithraObjectResourceType = mithraObjectList.get(i);
            final String objectName = mithraObjectResourceType.getName();
            getExecutor().submit(new GeneratorTask(i)
            {
                public void run()
                {
                    MithraInterfaceType mithraObject = parseMithraInterfaceType(objectName, mithraInterfaces, fileProvider, this, mithraObjectResourceType.isReadOnlyInterfaces(), importSource);
                    if (mithraObject != null)
                    {
                        mithraInterfaces.put(mithraObjectResourceType.getName(), mithraObject);
                    }
                }
            });
        }

        waitForExecutorWithCheck();
        return mithraObjectList.size();
    }

    private int parseMithraObjects(final MithraType mithraType, final String importSource,
                                   final MithraGeneratorImport.FileProvider fileProvider) throws FileNotFoundException
    {
        List<MithraObjectResourceType> mithraObjectList = mithraType.getMithraObjectResources();
        chopAndStickResource.resetSerialResource();
        for (int i = 0; i <mithraObjectList.size(); i++)
        {
            final MithraObjectResourceType mithraObjectResourceType = mithraObjectList.get(i);
            final String objectName = mithraObjectResourceType.getName();
            getExecutor().submit(new GeneratorTask(i)
            {
                public void run()
                {
                    MithraBaseObjectType mithraObject = parseMithraObject(objectName, mithraObjects, fileProvider, this, "normal");
                    if (mithraObject != null)
                    {
                        String objectFileName = objectName + ".xml";
                        boolean isGenerateInterfaces = !mithraObjectResourceType.isGenerateInterfacesSet() ? mithraType.isGenerateInterfaces() : mithraObjectResourceType.isGenerateInterfaces();
                        boolean enableOffHeap = !mithraObjectResourceType.isEnableOffHeapSet() ? mithraType.isEnableOffHeap() || forceOffHeap : mithraObjectResourceType.isEnableOffHeap();
                        MithraObjectTypeWrapper wrapper = new MithraObjectTypeWrapper(mithraObject, objectFileName, importSource, isGenerateInterfaces, ignorePackageNamingConvention, BaseMithraGenerator.this.logger);
                        wrapper.setReplicated(mithraObjectResourceType.isReplicated());
                        wrapper.setIgnoreNonGeneratedAbstractClasses(ignoreNonGeneratedAbstractClasses);
                        wrapper.setIgnoreTransactionalMethods(ignoreTransactionalMethods);
                        wrapper.setReadOnlyInterfaces(mithraObjectResourceType.isReadOnlyInterfacesSet() ? mithraObjectResourceType.isReadOnlyInterfaces() : mithraType.isReadOnlyInterfaces());
                        wrapper.setDefaultFinalGetters(defaultFinalGetters);
                        wrapper.setEnableOffHeap(enableOffHeap);
                        mithraObjects.put(mithraObjectResourceType.getName(), wrapper);
                    }
                }
            });
        }
        waitForExecutorWithCheck();
        return mithraObjectList.size();
    }

    private int getAvailableProcessors()
    {
        return Runtime.getRuntime().availableProcessors();
    }

    private int parseMithraPureObjects(final MithraType mithraType, final String importSource,
                                       final MithraGeneratorImport.FileProvider fileProvider) throws FileNotFoundException
    {
        List<MithraPureObjectResourceType> mithraPureObjectList = mithraType.getMithraPureObjectResources();
        if (!mithraPureObjectList.isEmpty())
        {
            chopAndStickResource.resetSerialResource();
            for (int i=0; i< mithraPureObjectList.size();i++)
            {
                final MithraPureObjectResourceType mithraPureObjectResourceType = mithraPureObjectList.get(i);
                getExecutor().submit(new GeneratorTask(i)
                {
                    public void run()
                    {
                        String objectName = mithraPureObjectResourceType.getName();
                        MithraBaseObjectType mithraObject = parseMithraObject(objectName, mithraObjects, fileProvider, this, "pure");
                        if (mithraObject != null)
                        {
                            String objectFileName = objectName + ".xml";
                            boolean enableOffHeap = !mithraPureObjectResourceType.isEnableOffHeapSet() ? mithraType.isEnableOffHeap() || forceOffHeap : mithraPureObjectResourceType.isEnableOffHeap();
                            MithraObjectTypeWrapper wrapper = new MithraObjectTypeWrapper(mithraObject, objectFileName, importSource, false, ignorePackageNamingConvention, BaseMithraGenerator.this.logger);
                            wrapper.setIgnoreNonGeneratedAbstractClasses(ignoreNonGeneratedAbstractClasses);
                            wrapper.setIgnoreTransactionalMethods(ignoreTransactionalMethods);
                            wrapper.setPure(true);
                            wrapper.setEnableOffHeap(enableOffHeap);
                            mithraObjects.put(mithraPureObjectResourceType.getName(), wrapper);
                        }
                    }
                });
            }
            waitForExecutorWithCheck();
        }
        return mithraPureObjectList.size();
    }

    private void waitForExecutorWithCheck()
    {
        getExecutor().waitUntilDone();
        if (executorError != null)
        {
            throw new MithraGeneratorException("exception while generating", executorError);
        }
    }

    private int parseMithraTempObjects(final MithraType mithraType, final String importSource,
                                       final MithraGeneratorImport.FileProvider fileProvider) throws FileNotFoundException
    {
        List<MithraTempObjectResourceType> mithraTempObjectList = mithraType.getMithraTempObjectResources();
        if (mithraTempObjectList.size() > 0)
        {
            chopAndStickResource.resetSerialResource();
            for (int i=0; i<mithraTempObjectList.size();i++)
            {
                final MithraTempObjectResourceType mithraTempObjectResourceType = mithraTempObjectList.get(i);
                getExecutor().submit(new GeneratorTask(i)
                {
                    public void run()
                    {
                        String objectName = mithraTempObjectResourceType.getName();
                        MithraBaseObjectType mithraObject = parseMithraObject(objectName, mithraObjects, fileProvider, this, "temp");
                        if (mithraObject != null)
                        {
                            String objectFileName = objectName + ".xml";
                            MithraObjectTypeWrapper wrapper = new MithraObjectTypeWrapper(mithraObject, objectFileName, importSource, false, ignorePackageNamingConvention, BaseMithraGenerator.this.logger);
                            wrapper.setIgnoreNonGeneratedAbstractClasses(ignoreNonGeneratedAbstractClasses);
                            wrapper.setIgnoreTransactionalMethods(ignoreTransactionalMethods);
                            wrapper.setTemporary(true);
                            mithraObjects.put(mithraTempObjectResourceType.getName(), wrapper);
                        }
                    }
                });
            }
            waitForExecutorWithCheck();
        }
        return mithraTempObjectList.size();
    }

    private int parseMithraEmbeddedValueObjects(final MithraType mithraType, final String importSource,
                                                final MithraGeneratorImport.FileProvider fileProvider) throws FileNotFoundException
    {
        List<MithraEmbeddedValueObjectResourceType> mithraEmbeddedValueObjectList = mithraType.getMithraEmbeddedValueObjectResources();
        if (!mithraEmbeddedValueObjectList.isEmpty())
        {
            chopAndStickResource.resetSerialResource();
            for (int i=0;i<mithraEmbeddedValueObjectList.size();i++)
            {
                final MithraEmbeddedValueObjectResourceType mithraEmbeddedValueObjectResourceType = mithraEmbeddedValueObjectList.get(i);
                getExecutor().submit(new GeneratorTask(i)
                {
                    public void run()
                    {
                        String objectName = mithraEmbeddedValueObjectResourceType.getName();
                        MithraEmbeddedValueObjectType evo = (MithraEmbeddedValueObjectType) parseMithraBaseObject(objectName, mithraEmbeddedValueObjects, fileProvider, this);
                        if (evo != null)
                        {
                            String objectFileName = objectName + ".xml";
                            MithraEmbeddedValueObjectTypeWrapper wrapper = new MithraEmbeddedValueObjectTypeWrapper(evo, objectFileName, importSource);
                            wrapper.setIgnoreNonGeneratedAbstractClasses(ignoreNonGeneratedAbstractClasses);
                            mithraEmbeddedValueObjects.put(mithraEmbeddedValueObjectResourceType.getName(), wrapper);
                        }
                    }
                });
            }
            waitForExecutorWithCheck();
        }
        return mithraEmbeddedValueObjectList.size();
    }

    private int parseMithraEnumerations(final MithraType mithraType, final String importSource,
                                        final MithraGeneratorImport.FileProvider fileProvider) throws FileNotFoundException
    {
        List<MithraEnumerationResourceType> mithraEnumerationList = (List<MithraEnumerationResourceType>) mithraType.getMithraEnumerationResources();
        if (!mithraEnumerationList.isEmpty())
        {
            chopAndStickResource.resetSerialResource();
            for (int i=0;i<mithraEnumerationList.size();i++)
            {
                final MithraEnumerationResourceType mithraEnumerationResourceType = mithraEnumerationList.get(i);
                getExecutor().submit(new GeneratorTask(i)
                {
                    public void run()
                    {
                        String enumerationName = mithraEnumerationResourceType.getName();
                        MithraEnumerationType enumeration = (MithraEnumerationType) parseMithraBaseObject(enumerationName, mithraEnumerations, fileProvider, this);
                        if (enumeration != null)
                        {
                            String enumerationFileName = enumerationName + ".xml";
                            MithraEnumerationTypeWrapper wrapper = new MithraEnumerationTypeWrapper(enumeration, enumerationFileName, importSource);
                            mithraEnumerations.put(mithraEnumerationResourceType.getName(), wrapper);
                        }
                    }
                });
            }
            waitForExecutorWithCheck();
        }
        return mithraEnumerationList.size();
    }

    private MithraInterfaceType parseMithraInterfaceType(String objectName, Map objectMap,
                                                         MithraGeneratorImport.FileProvider fileProvider,
                                                         GeneratorTask task, boolean isReadOnlyInterfaces,
                                                         String importedSource)
    {
        MithraInterfaceType mithraObject = (MithraInterfaceType) parseMithraType(objectName, objectMap, fileProvider, task);
        mithraObject.setReadOnlyInterfaces(isReadOnlyInterfaces);
        mithraObject.setImportedSource(importedSource);
        checkClassName(objectName, mithraObject.getClassName());
        mithraObject.postInitialize(objectName);
        return mithraObject;
    }

    private void checkClassName(String objectName, String className)
    {
        if (objectName.contains("/"))
        {
            objectName = objectName.substring(objectName.lastIndexOf('/') + 1);
        }
        if (!objectName.equals(className))
        {
            throw new MithraGeneratorException("XML filename: '" + objectName + "' must match class name specified: '" + className + "'");
        }
    }

    private MithraBaseObjectType parseMithraObject(String objectName, Map objectMap,
                                                   MithraGeneratorImport.FileProvider fileProvider,
                                                   GeneratorTask task, String errorType)
    {
        return parseMithraBaseObject(objectName, objectMap, fileProvider, task);
    }

    private MithraBaseObjectType parseMithraBaseObject(String objectName, Map objectMap,
                                                       MithraGeneratorImport.FileProvider fileProvider,
                                                       GeneratorTask task)
    {
        MithraBaseObjectType mithraObject = (MithraBaseObjectType) parseMithraType(objectName, objectMap, fileProvider, task);
        checkClassName(objectName, mithraObject.getClassName());
        return mithraObject;

    }

    private Object parseMithraType(String objectName, Map objectMap, MithraGeneratorImport.FileProvider fileProvider, GeneratorTask task)
    {
        Object mithraObject = null;
        this.logger.info("Reading " + objectName);
        if (!fileProvider.excludeObject(objectName))
        {
            if (objectMap.containsKey(objectName))
            {
                throw new MithraGeneratorException("Attempted to add object " + objectName + " twice");
            }
            String objectFileName = objectName + ".xml";
            InputStream objectFileIs = null;
            boolean serialAquired = false;
            try
            {
                MithraGeneratorImport.FileInputStreamWithSize streamWithSize = fileProvider.getFileInputStream(objectFileName);
                FullFileBuffer ffb = getFullFileBuffer();
                chopAndStickResource.acquireIoResource();
                try
                {
                    ffb.bufferFile(streamWithSize.getInputStream(), (int) streamWithSize.getSize());
                }
                finally
                {
                    chopAndStickResource.releaseIoResource();
                }
                task.acquireSerialResource();
                serialAquired = true;
                try
                {
                    ffb.updateCrc(crc32);
                }
                finally
                {
                    task.releaseSerialResource();
                }
                chopAndStickResource.acquireCpuResource();
                try
                {
                    objectFileIs = streamWithSize.getInputStream();
                    mithraObject = new MithraGeneratorUnmarshaller().parse(ffb.getBufferedInputStream(), objectFileName);
                }
                finally
                {
                    chopAndStickResource.releaseCpuResource();
                }
            }
            catch (FileNotFoundException e)
            {
                throw new MithraGeneratorException("Unable to find " + objectFileName, e);
            }
            catch (MithraGeneratorParserException e)
            {
                throw new MithraGeneratorException("Unable to parse " + objectFileName+" "+e.getMessage(), e);
            }
            catch (IOException e)
            {
                throw new MithraGeneratorException("Unable to read " + objectFileName, e);
            }
            finally
            {
                if (!serialAquired)
                {
                    task.acquireSerialResource();
                    task.releaseSerialResource();
                }
                closeIs(objectFileIs);
            }
        }
        else
        {
            this.logger.info("Skipping " + objectName + ", excluded");
        }
        return mithraObject;
    }

    private void closeIs(InputStream is)
    {
        if (is != null)
        {
            try
            {
                is.close();
            }
            catch (IOException e)
            {
                throw new MithraGeneratorException("Exception closing InputStream",e);
            }
        }
    }

    private void createSortedList()
    {
        this.sortedMithraObjects = new ArrayList<MithraObjectTypeWrapper>(this.mithraObjects.values());
        Collections.sort(this.sortedMithraObjects, new ParentClassComparator());
    }

    private void createSortedEmbeddedValueObjectList()
    {
        this.sortedMithraEmbeddedValueObjects = new ArrayList<MithraEmbeddedValueObjectTypeWrapper>(this.mithraEmbeddedValueObjects.values());
    }

    private void createSortedEnumerationsList()
    {
        this.sortedMithraEnumerations = new ArrayList<MithraEnumerationTypeWrapper>(this.mithraEnumerations.values());
    }

    public void validateXml()
    {
        boolean mithraInterfaceSuccess = this.extractMithraInterfaceRelationshipsAndSuperInterfaces();
        boolean mithraEmbeddedValueObjectXmlSuccess = this.validateMithraEmbeddedValueObjectXml();
        boolean mithraObjectXmlSuccess = this.validateMithraObjectXml();
        if (!(mithraInterfaceSuccess && mithraEmbeddedValueObjectXmlSuccess && mithraObjectXmlSuccess))
        {
            throw new MithraGeneratorException("One or more error(s) while validating mithra xml. See error logs.\n");
        }
    }

    public boolean validateMithraEmbeddedValueObjectXml()
    {
        return this.resolveMithraObjectEmbeddedValueTypes();
    }

    public boolean validateMithraObjectXml()
    {
        boolean result = this.checkAllNames();
        result &= this.resolveAttributes();
        result &= this.resolveEmbeddedValues();
        result &= this.resolveEnumerations();
        result &= this.resolveSuperClasses();
        result &= this.resolveIndices();
        result &= this.checkRelationships();  // once we have validated these real ones validate the interface relationship.
        result &= this.resolveMithraInterfaces(); // validate both attributes and relationships
        this.processForeignKeys();
        result &= this.postValidate();
        return result;
    }

    private void processForeignKeys()
    {
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper: this.mithraObjects.values())
        {
            mithraObjectTypeWrapper.processForeignKeys();
        }
    }

    private boolean resolveMithraObjectEmbeddedValueTypes() throws JavaTypeException
    {
        boolean allGood = true;
        for (MithraEmbeddedValueObjectTypeWrapper wrapper : this.sortedMithraEmbeddedValueObjects)
        {
            List errors = wrapper.resolveNestedEmbeddedValueObjects(this.getMithraEmbeddedValueObjects());
            allGood = processErrorsIfAny(errors, allGood, wrapper.getSourceFileName());
        }
        return allGood;
    }

    private boolean checkAllNames()
    {
        boolean result = true;
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper : this.sortedMithraObjects)
        {
            List errors = mithraObjectTypeWrapper.checkAllNames(this.mithraObjects);
            result = processErrorsIfAny(errors, result, mithraObjectTypeWrapper.getSourceFileName());
        }
        return result;
    }

    private boolean resolveAttributes() throws JavaTypeException
    {
        boolean allGood = true;
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper : this.sortedMithraObjects)
        {
            List errors = mithraObjectTypeWrapper.resolveAttributes(this.mithraObjects);
            allGood = processErrorsIfAny(errors, allGood, mithraObjectTypeWrapper.getSourceFileName());
        }
        return allGood;
    }

    private boolean resolveIndices() throws JavaTypeException
    {
        boolean allGood = true;
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper : this.sortedMithraObjects)
        {
            List errors = mithraObjectTypeWrapper.resolveIndices();
            allGood = processErrorsIfAny(errors, allGood, mithraObjectTypeWrapper.getSourceFileName());
        }
        return allGood;
    }

    private boolean resolveSuperClasses() throws JavaTypeException
    {
        boolean allGood = true;
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper : this.sortedMithraObjects)
        {
            List errors = mithraObjectTypeWrapper.resolveSuperClasses(this.mithraObjects);
            allGood = processErrorsIfAny(errors, allGood, mithraObjectTypeWrapper.getSourceFileName());
        }
        if (allGood)
        {
            for (MithraObjectTypeWrapper mithraObjectTypeWrapper : this.sortedMithraObjects)
            {
                List errors = mithraObjectTypeWrapper.resolveSuperClassGeneration();
                allGood = processErrorsIfAny(errors, allGood, mithraObjectTypeWrapper.getSourceFileName());
            }
            if (allGood)
            {
                assignSuperClassPackage();
            }
        }
        return allGood;
    }

    private void assignSuperClassPackage()
    {
        Map<MithraSuperTypeWrapper, List<MithraObjectTypeWrapper>> map = new HashMap();
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper : this.sortedMithraObjects)
        {
            MithraSuperTypeWrapper superType = mithraObjectTypeWrapper.getSubstituteSuperType();
            if (superType != null)
            {
                List<MithraObjectTypeWrapper> typeWrappers = map.get(superType);
                if (typeWrappers == null)
                {
                    typeWrappers = new ArrayList();
                    map.put(superType, typeWrappers);
                }
                typeWrappers.add(mithraObjectTypeWrapper);
            }
        }
        for(Iterator<Map.Entry<MithraSuperTypeWrapper, List<MithraObjectTypeWrapper>>> it = map.entrySet().iterator();it.hasNext();)
        {
            Map.Entry<MithraSuperTypeWrapper, List<MithraObjectTypeWrapper>> entry = it.next();
            List<MithraObjectTypeWrapper> typeWrappers = entry.getValue();
            MithraSuperTypeWrapper superTypeWrapper = entry.getKey();
            if (superTypeWrapper.getPackageName() == null)
            {
                String bestPackage = typeWrappers.get(0).getPackageName();

                for(int i= 1;i< typeWrappers.size();i++)
                {
                    String pkg = typeWrappers.get(i).getPackageName();
                    if (pkg.compareTo(bestPackage) < 0)
                    {
                        bestPackage = pkg;
                    }
                }
                superTypeWrapper.setPackageName(bestPackage);
            }
            for(int i=0;i< typeWrappers.size();i++)
            {
                typeWrappers.get(i).setSubstituteSuperType(superTypeWrapper);
            }
        }
    }

    private boolean resolveEmbeddedValues() throws JavaTypeException
    {
        boolean allGood = true;
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper : this.sortedMithraObjects)
        {
            List errors = mithraObjectTypeWrapper.resolveEmbeddedValueObjects(this.getMithraEmbeddedValueObjects(), this.mithraObjects);
            allGood = processErrorsIfAny(errors, allGood, mithraObjectTypeWrapper.getSourceFileName());
        }
        return allGood;
    }

    private boolean resolveEnumerations() throws JavaTypeException
    {
        boolean allGood = true;
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper : this.sortedMithraObjects)
        {
            List errors = mithraObjectTypeWrapper.resolveEnumerations(this.getMithraEnumerations());
            allGood = processErrorsIfAny(errors, allGood, mithraObjectTypeWrapper.getSourceFileName());
        }
        return allGood;
    }

    private boolean resolveMithraInterfaces() throws JavaTypeException
    {
        boolean allGood = true;
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper : this.sortedMithraObjects)
        {
            List errors = mithraObjectTypeWrapper.resolveMithraInterfaces(this.getMithraInterfaces());
            allGood = processErrorsIfAny(errors, allGood, mithraObjectTypeWrapper.getSourceFileName());
        }
        return allGood;
    }

    private boolean checkRelationships()
    {
        boolean result = true;
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper: this.mithraObjects.values())
        {
            Map errors = mithraObjectTypeWrapper.checkRelationships(this.mithraObjects);
            if (errors.size() > 0)
            {
                result = false;
                this.logger.error("\n" + mithraObjectTypeWrapper.getSourceFileName() + ": ");
                for (Iterator iterator1 = errors.keySet().iterator(); iterator1.hasNext();)
                {
                    String key = (String) iterator1.next();
                    this.logger.error("\n-- Relationship '" + key + "' -- ");
                    printErrors( (List)errors.get(key) );
                }
            }
            List indexErrors = mithraObjectTypeWrapper.checkAttributeNamesInIndices();
            if( indexErrors.size() > 0)
            {
                this.logger.error("\n" + mithraObjectTypeWrapper.getSourceFileName() + ": ");
                result = false;
                printErrors( indexErrors );
            }
        }
        return result;
    }

    private boolean postValidate() throws JavaTypeException
    {
        boolean allGood = true;
        for (MithraObjectTypeWrapper mithraObjectTypeWrapper : this.sortedMithraObjects)
        {
            List errors = mithraObjectTypeWrapper.postValidate();
            allGood = processErrorsIfAny(errors, allGood, mithraObjectTypeWrapper.getSourceFileName());
        }
        return allGood;
    }

    private boolean processErrorsIfAny(List errors, boolean allGood, String sourceFileName)
    {
        if (errors.size() > 0)
        {
            allGood = false;
            this.logger.error("\n" + sourceFileName + ": ");
            printErrors(errors);
        }
        return allGood;
    }

    private void printErrors(List errors)
    {
        for (int i = 0; i < errors.size(); i++)
        {
            this.logger.error("\t" + errors.get( i ));
        }
    }

    public void copyIfChanged(byte[] src, File outFile, AtomicInteger count) throws IOException
    {
        boolean copyFile = false;
        if ((!outFile.exists()) || (outFile.length() != src.length))
        {
            copyFile = true;
        }
        else
        {
            byte[] outContent = readFile(outFile);
            for(int i=0;i<src.length;i++)
            {
                if (src[i] != outContent[i])
                {
                    copyFile = true;
                    break;
                }
            }
        }
        if (copyFile && outFile.exists() && !outFile.canWrite())
        {
            throw new MithraGeneratorException(outFile+" must be updated, but it is readonly.");
        }

        if (copyFile)
        {
            FileOutputStream fout = new FileOutputStream(outFile);
            fout.write(src);
            fout.close();
            count.incrementAndGet();
            this.logger.info("wrote file: " + outFile.getName());
        }
    }

    private byte[] readFile(File file) throws IOException
    {
        int length = (int)file.length();
        FileInputStream fis = new FileInputStream(file);
        byte[] result = new byte[length];
        int pos = 0;
        while(pos < length)
        {
            pos += fis.read(result, pos, length - pos);
        }
        fis.close();
        return result;
    }

    public void addConfiguredMithraImport(MithraGeneratorImport importElement)
    {
        importElement.init();
        imports.add(importElement);
    }

    public ChopAndStickResource getChopAndStickResource()
    {
        return chopAndStickResource;
    }

    public AwaitingThreadExecutor getExecutor()
    {
        if (executor == null)
        {
            executor = new AwaitingThreadExecutor(Runtime.getRuntime().availableProcessors()+IO_THREADS, "Mithra Generator");
            executor.setExceptionHandler(new AutoShutdownThreadExecutor.ExceptionHandler() {
                public void handleException(AutoShutdownThreadExecutor executor, Runnable target, Throwable exception)
                {
                    executor.shutdownNow();
                    BaseMithraGenerator.this.logger.error("Error in runnable target. Shutting down queue "+exception.getClass().getName()+" :"+exception.getMessage());
                    executorError = exception;
                }
            });
        }
        return executor;
    }

    private class ParentClassComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            MithraObjectTypeWrapper left = (MithraObjectTypeWrapper) o1;
            MithraObjectTypeWrapper right = (MithraObjectTypeWrapper) o2;

            Map<String, MithraObjectTypeWrapper> mithraObjects = BaseMithraGenerator.this.getMithraObjects();
            int result = left.getHierarchyDepth(mithraObjects) - right.getHierarchyDepth(mithraObjects);
            if (result == 0)
            {
                result = left.getClassName().compareTo(right.getClassName());
            }
            return result;
        }
    }

    public abstract class GeneratorTask implements Runnable
    {
        private int resourceNumber;

        public GeneratorTask(int resourceNumber)
        {
            this.resourceNumber = resourceNumber;
        }

        public void acquireSerialResource()
        {
            getChopAndStickResource().acquireSerialResource(resourceNumber);
        }

        public void releaseSerialResource()
        {
            getChopAndStickResource().releaseSerialResource();
        }
    }

    public static class SimpleGenerationLogger implements GenerationLogger
    {
        private GenerationLog oldGenerationLog;
        private GenerationLog newGenerationLog;

        @Override
        public GenerationLog getOldGenerationLog()
        {
            return this.oldGenerationLog;
        }

        @Override
        public GenerationLog getNewGenerationLog()
        {
            return this.newGenerationLog;
        }

        @Override
        public void setOldGenerationLog(GenerationLog log)
        {
            this.oldGenerationLog = log;
        }

        @Override
        public void setNewGenerationLog(GenerationLog log)
        {
            this.newGenerationLog = log;
        }
    }

    public void execute()
    {
        this.logger.info("nothing to do");
    }
}
