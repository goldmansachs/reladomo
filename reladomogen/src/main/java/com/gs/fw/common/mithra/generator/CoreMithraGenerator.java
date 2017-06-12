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

import com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType;
import com.gs.fw.common.mithra.generator.util.AwaitingThreadExecutor;
import com.gs.fw.common.mithra.generator.writer.GeneratedFileManager;
import com.gs.fw.common.mithra.generator.writer.StandardGeneratedFileManager;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CoreMithraGenerator extends BaseMithraGenerator
{
    //this is to force loading the JspRuntimeContext class which sets the default factory in JspFactory.
    private static final String TEMPLATE_PACKAGE_PREFIX = "com.gs.fw.common.mithra.templates";
    private static final Map<String, String> TEMPLATE_PACKAGES = new HashMap<String, String>();
    private static final Map<String, List<String>> TEMPLATE_LISTS = new HashMap<String, List<String>>();

    private static final String GENERATED_COMMON_TEMPLATE = "CommonSuper";

    private static final List<String> READ_ONLY_TEMPLATES = Arrays.asList(
            "Abstract_jsp",
            "Data_jsp",
            "ListAbstract_jsp",
            "Finder_jsp",
            "DatabaseObjectAbstract_jsp");
    private static final List<String> DATED_READ_ONLY_TEMPLATES = Arrays.asList(
            "Abstract_jsp",
            "Data_jsp",
            "ListAbstract_jsp",
            "Finder_jsp",
            "DatabaseObjectAbstract_jsp");
    private static final List<String> TRANSACTIONAL_TEMPLATES = Arrays.asList(
            "Abstract_jsp",
            "Data_jsp",
            "ListAbstract_jsp",
            "Finder_jsp",
            "DatabaseObjectAbstract_jsp");
    private static final List<String> DATED_TRANSACTIONAL_TEMPLATES = Arrays.asList(
            "Abstract_jsp",
            "Data_jsp",
            "ListAbstract_jsp",
            "Finder_jsp",
            "DatabaseObjectAbstract_jsp");
    private static final List<String> EMBEDDED_VALUE_TEMPLATES = Arrays.asList(
            "Abstract_jsp",
            "Data_jsp");
    private static final List<String> ENUMERATION_TEMPLATES = Arrays.asList(
            "Enumeration_jsp");
    private static final List<String> SUPERCLASS_TEMPLATES = Arrays.asList(
            "Abstract_jsp",
            "Finder_jsp");

    private static final List<String> MITHRA_INTERFACE_TEMPLATES = Arrays.asList(
            "Abstract_jsp",
            "Finder_jsp"
    );

    public static final String FORMAT_NONE = "none";
    public static final String FORMAT_FAST = "fast";
    public static final String FORMAT_PRETTY = "pretty";

    private boolean executed = false;
    private boolean generateConcreteClasses = true;
    private boolean warnAboutConcreteClasses = true;
    private boolean generateGscListMethod = false;
    private boolean generateLegacyCaramel = false;
    private String format = FORMAT_FAST;
    private boolean generateImported;
    private ThreadLocal<ByteArrayOutputStream> byteArrayOutputStreamThreadLocal = new ThreadLocal<ByteArrayOutputStream>();
    private ThreadLocal<SourceFormatter> sourceFormatterThreadLocal = new ThreadLocal<SourceFormatter>();

    private GeneratedFileManager generatedFileManager;

    static
    {
        TEMPLATE_PACKAGES.put(READ_ONLY, TEMPLATE_PACKAGE_PREFIX + ".readonly");
        TEMPLATE_PACKAGES.put(DATED_READ_ONLY, TEMPLATE_PACKAGE_PREFIX + ".datedreadonly");
        TEMPLATE_PACKAGES.put(TRANSACTIONAL, TEMPLATE_PACKAGE_PREFIX + ".transactional");
        TEMPLATE_PACKAGES.put(DATED_TRANSACTIONAL, TEMPLATE_PACKAGE_PREFIX + ".datedtransactional");
        TEMPLATE_PACKAGES.put(EMBEDDED_VALUE, TEMPLATE_PACKAGE_PREFIX + ".embeddedvalue");
        TEMPLATE_PACKAGES.put(ENUMERATION, TEMPLATE_PACKAGE_PREFIX + ".enumeration");
        TEMPLATE_PACKAGES.put(MITHRA_INTERFACE, TEMPLATE_PACKAGE_PREFIX + ".mithrainterface");

        TEMPLATE_LISTS.put(READ_ONLY, READ_ONLY_TEMPLATES);
        TEMPLATE_LISTS.put(DATED_READ_ONLY, DATED_READ_ONLY_TEMPLATES);
        TEMPLATE_LISTS.put(TRANSACTIONAL, TRANSACTIONAL_TEMPLATES);
        TEMPLATE_LISTS.put(DATED_TRANSACTIONAL, DATED_TRANSACTIONAL_TEMPLATES);
        TEMPLATE_LISTS.put(EMBEDDED_VALUE, EMBEDDED_VALUE_TEMPLATES);
        TEMPLATE_LISTS.put(ENUMERATION, ENUMERATION_TEMPLATES);
        TEMPLATE_LISTS.put(MITHRA_INTERFACE, MITHRA_INTERFACE_TEMPLATES);
    }

    public CoreMithraGenerator()
    {
        super();
        initGeneratedFileManager(new StandardGeneratedFileManager());
    }

    private ByteArrayOutputStream getByteArrayOutputStream()
    {
        ByteArrayOutputStream result = byteArrayOutputStreamThreadLocal.get();
        if (result == null)
        {
            result = new ByteArrayOutputStream(10000);
            byteArrayOutputStreamThreadLocal.set(result);
        }
        result.reset();
        return result;
    }

    private SourceFormatter getSourceFormatter()
    {
        SourceFormatter result = sourceFormatterThreadLocal.get();
        if (result == null)
        {
            result = new SourceFormatter();
            sourceFormatterThreadLocal.set(result);
        }
        return result;
    }

    public boolean isGenerateImported()
    {
        return this.generateImported;
    }

    public void setGenerateImported(boolean generateImported)
    {
        this.generateImported = generateImported;
    }

    public void setGenerateConcreteClasses(boolean generateConcreteClasses)
    {
        this.generateConcreteClasses = generateConcreteClasses;
    }

    public void setWarnAboutConcreteClasses(boolean warnAboutConreteClasses)
    {
        this.warnAboutConcreteClasses = warnAboutConreteClasses;
    }

    public void setGenerateGscListMethod(boolean generateGscListMethod)
    {
        this.generateGscListMethod = generateGscListMethod;
    }

    @Deprecated
    public void setGenerateLegacyCaramel(boolean generateLegacyCaramel)
    {
        this.generateLegacyCaramel = false;
    }

    public void setCodeFormat(String format)
    {
        if (format.equals(FORMAT_NONE) || format.equals(FORMAT_PRETTY) || format.equals(FORMAT_FAST))
        {
            this.format = format;
        }
        else
        {
            System.out.println("Code format option '"+format+"' not recognized. Using '"+this.format+"' instead. Valid values are 'none', 'fast', and 'pretty");
        }
    }

    public void setGeneratedFileManager(GeneratedFileManager generatedFileManager)
    {
        this.initGeneratedFileManager(generatedFileManager);
    }

    private void initGeneratedFileManager(GeneratedFileManager generatedFileManager)
    {
        // safety check : all files should be generated with the same manager
        if (this.generatedFileManager != null)
        {
            throw new MithraGeneratorException("Attempt to reset file manager. An instance of " + generatedFileManager.getClass().getCanonicalName() + " has already been set");
        }
        this.generatedFileManager = generatedFileManager;
    }

    private void setFileGenerationOptions()
    {
        GeneratedFileManager.Options fileGenerationOptions = new GeneratedFileManager.Options(
                this.getGeneratedDir(),
                this.getNonGeneratedDir(),
                this.warnAboutConcreteClasses,
                this.generateConcreteClasses,
                this.getGenerationLogger(),
                this.logger
        );
        this.generatedFileManager.setOptions(fileGenerationOptions);
    }

    private void applyMithraInterfaceTemplates(MithraInterfaceType mithraObject, AtomicInteger count)
    {
        List templates = TEMPLATE_LISTS.get(MITHRA_INTERFACE);
        String templatePackage = TEMPLATE_PACKAGES.get(MITHRA_INTERFACE);
        for (Iterator iterator = templates.iterator(); iterator.hasNext();)
        {
            String name = (String) iterator.next();
            String templatePrefix = name.substring(name.lastIndexOf('.') + 1, name.lastIndexOf('_'));
            String outputFileSuffix = "";

            if (templatePackage.endsWith("mithrainterface"))
            {
                outputFileSuffix = templatePrefix;
            }

            generateFile(mithraObject, count, templatePackage, templatePrefix, outputFileSuffix, true);
        }
        generateFile(mithraObject, count, templatePackage, "", "", false); //
    }

    private void applyTemplates(MithraBaseObjectTypeWrapper mithraObject, AtomicInteger count)
    {
        List templates = TEMPLATE_LISTS.get(mithraObject.getObjectType());
        String templatePackage = TEMPLATE_PACKAGES.get(mithraObject.getObjectType());
        if (mithraObject.isTablePerSubclassSuperClass())
        {
            templates = SUPERCLASS_TEMPLATES;
            templatePackage = templatePackage + ".superclass";
        }

        for (Iterator iterator = templates.iterator(); iterator.hasNext();)
        {
            String name = (String) iterator.next();
            String outputFileSuffix;
            String nonInterfaceSuffix;
            if (name.startsWith("Enumeration"))
            {
                MithraTemplate servlet = newTemplate(templatePackage + "." + name);
                this.generateJavaFileFromTemplate(mithraObject, "", servlet, false, count);
                outputFileSuffix = "";
                nonInterfaceSuffix = "";
            }
            else
            {
                if (name.startsWith("DatabaseObject") && mithraObject.isPure())
                {
                    name = name.replace("DatabaseObject", "ObjectFactory");
                }
                outputFileSuffix = name.substring(name.lastIndexOf('.') + 1, name.lastIndexOf('_'));
                nonInterfaceSuffix = "";
                if (mithraObject.isGenerateInterfaces() && outputFileSuffix.endsWith("Abstract") && !outputFileSuffix.contains("Database"))
                {
                    nonInterfaceSuffix = "Impl";
                }
            }
            generateAbstractClass(mithraObject, count, templatePackage, outputFileSuffix, nonInterfaceSuffix);
            //for every abstract class generate a dummy subclass if one is not out
            //there already. The template for the base class is in Main.jsp
            if (outputFileSuffix.endsWith("Abstract"))
            {
                generateConcreteSubclass(mithraObject, count, templatePackage, outputFileSuffix, nonInterfaceSuffix);
            }
        }
        if (mithraObject.isGenerateInterfaces())
        {
            generateInterfaces(mithraObject, count, templatePackage);
        }
        if (mithraObject.getSubstituteSuperType() != null)
        {
            generateSuperType(mithraObject.getSubstituteSuperType(), count);
        }
    }

    private void generateSuperType(MithraSuperTypeWrapper superType, AtomicInteger count)
    {
        if (!superType.isWritten())
        {
            superType.setWritten(true);
            generateFile(superType, count, TEMPLATE_PACKAGE_PREFIX, GENERATED_COMMON_TEMPLATE, "", true);
        }
    }

    private void generateAbstractClass(MithraBaseObjectTypeWrapper mithraObject, AtomicInteger count,
                                       String templatePackage, String templatePrefix, String nonInterfaceSuffix)
    {
        String outputFileSuffix;
        if(templatePrefix.contains("List"))
        {
            if(mithraObject.isGenerateInterfaces())
            {
                outputFileSuffix = "ListImplAbstract";
            }
            else
            {
                outputFileSuffix = templatePrefix;
            }
        }
        else
        {
            outputFileSuffix = nonInterfaceSuffix+templatePrefix;
        }
        generateFile(mithraObject, count, templatePackage, templatePrefix, outputFileSuffix, true);
    }

    private void generateConcreteSubclass(MithraBaseObjectTypeWrapper mithraObject, AtomicInteger count, String templatePackage, String templatePrefix, String nonInterfaceSuffix)
    {
        String outputFileSuffix;
        templatePrefix = templatePrefix.substring(0, templatePrefix.indexOf("Abstract"));
        if(templatePrefix.contains("List"))
        {
            outputFileSuffix = templatePrefix+nonInterfaceSuffix;
        }
        else
        {
            outputFileSuffix = nonInterfaceSuffix+templatePrefix;
        }

        generateFile(mithraObject, count, templatePackage, templatePrefix, outputFileSuffix, false);
    }

    private void generateFile(CommonWrapper mithraObject, AtomicInteger count, String templatePackage,
                              String templatePrefix, String foo, boolean replaceIfExists)
    {
        MithraTemplate servlet = newTemplate( templatePackage + "." + (templatePrefix.equals("")?"Main":templatePrefix) + "_jsp");

        generateJavaFileFromTemplate(mithraObject, foo, servlet, replaceIfExists, count);
    }

    private void generateInterfaces(MithraBaseObjectTypeWrapper mithraObject, AtomicInteger count, String templatePackage)
    {
        MithraTemplate servlet = newTemplate( templatePackage + "." + "AbstractInterface_jsp");
        generateJavaFileFromTemplate(mithraObject, "Abstract", servlet, true, count);

        servlet = newTemplate( templatePackage + "." + "MainInterface_jsp");
        generateJavaFileFromTemplate(mithraObject, "", servlet, false, count);

        servlet = newTemplate( templatePackage + "." + "ListAbstractInterface_jsp");
        generateJavaFileFromTemplate(mithraObject, "ListAbstract", servlet, true, count);

        servlet = newTemplate( templatePackage + "." + "ListInterface_jsp");
        generateJavaFileFromTemplate(mithraObject, "List", servlet, false, count);
    }

    private MithraTemplate newTemplate(String name)
    {
        try
        {
            return (MithraTemplate) BaseMithraGenerator.class.getClassLoader().loadClass(name).newInstance();
        }
        catch (Exception e)
        {
            throw new MithraGeneratorException("unable to load template " + name + ", make sure you have compiled the templates", e);
        }
    }

    private byte[] prettyFormatCode(byte[] originalBytes) throws IOException
    {
        throw new RuntimeException("extreme pretty printing is no longer supported");
    }

    private byte[] fastFormatCode(byte[] originalBytes) throws IOException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(originalBytes);
        LineNumberReader reader = new LineNumberReader(new InputStreamReader(bais));

        SourceFormatter sourceFormatter = getSourceFormatter();
        sourceFormatter.init();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(originalBytes.length);
        PrintWriter writer = new PrintWriter(byteArrayOutputStream);
        String line = null;
        while((line = reader.readLine()) != null)
        {
            if (line.trim().length() > 0)
            {
                sourceFormatter.formatLine(line, writer);
            }
        }
        reader.close();
        writer.flush();
        byteArrayOutputStream.flush();
        writer.close();
        byteArrayOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

    private byte[] formatCode(byte[] originalBytes) throws IOException
    {
        if (format.equalsIgnoreCase(FORMAT_FAST))
        {
            return this.fastFormatCode(originalBytes);
        }
        if (format.equalsIgnoreCase(FORMAT_PRETTY))
        {
            return this.prettyFormatCode(originalBytes);
        }
        return originalBytes;
    }

     private void generateJavaFileFromTemplate(CommonWrapper wrapper, String outputFileSuffix, MithraTemplate servlet, boolean replaceIfExists, AtomicInteger count)
    {
        String packageName = wrapper.getPackageName().replace('.', '/');
        boolean shouldCreateFile = generatedFileManager
                .shouldCreateFile(replaceIfExists, packageName, wrapper.getClassName(), outputFileSuffix);

        if (!shouldCreateFile)
        {
            return;
        }
        generateJavaFile(wrapper, replaceIfExists, packageName, wrapper.getClassName(), outputFileSuffix, servlet, count);
    }

    private void generateJavaFile(final CommonWrapper wrapper, final boolean relaceIfExists, final String packageName, final String className, final String outputFileSuffix, final MithraTemplate servlet,
                                  final AtomicInteger count)
    {
        this.getExecutor().submit(new BaseMithraGenerator.GeneratorTask(0) {
            public void run()
            {
                JspWriter writer = null;
                try
                {
                    getChopAndStickResource().acquireCpuResource();
                    byte[] result;
                    try
                    {
                        ByteArrayOutputStream byteArrayOutputStream = getByteArrayOutputStream();
                        writer = new JspWriter(byteArrayOutputStream);
                        HttpServletRequest request = new HttpServletRequest();
                        request.setAttribute("mithraWrapper", wrapper);
                        request.setAttribute("generateGscListMethod", Boolean.valueOf(generateGscListMethod));
                        request.setAttribute("generateLegacyCaramel", Boolean.valueOf(false));
                        HttpServletResponse response = new HttpServletResponse(writer);
                        servlet._jspService(request, response);
                        writer.close();
                        writer = null;
                        byte[] originalBytes = byteArrayOutputStream.toByteArray();
                        result = formatCode(originalBytes);
                    }
                    finally
                    {
                        getChopAndStickResource().releaseCpuResource();
                    }
                    getChopAndStickResource().acquireIoResource();
                    try
                    {
                        generatedFileManager.writeFile(relaceIfExists, packageName, className, outputFileSuffix, result, count);
                    }
                    finally
                    {
                        getChopAndStickResource().releaseIoResource();
                    }
                }
                catch (IOException e)
                {
                    throw new MithraGeneratorException("Error writing class "+wrapper.getPackageName()+"."+wrapper.getClassName()+outputFileSuffix+
                            " "+e.getClass().getName()+": "+e.getMessage(), e);
                }
                finally
                {
                    if (writer != null)
                    {
                        writer.close();
                    }
                }
            }
        });
    }

    public int processMithraInterfaces(Collection<? extends MithraInterfaceType> wrappers)
    {
        AtomicInteger count = new AtomicInteger();
        for (MithraInterfaceType wrapper : wrappers)
        {
            if (!wrapper.isImported() || isGenerateImported())
            {
                try
                {
                    applyMithraInterfaceTemplates(wrapper, count);
                }
                catch (Exception e)
                {
                    throw new MithraGeneratorException("Failed to generate classes for " + wrapper.getClassName()+" with nested exception "+e.getClass().getName()+": "+e.getMessage(), e);
                }
            }
        }
        this.getExecutor().waitUntilDone();
        return count.get();
    }

    public int processMithraObjects(Collection<? extends MithraBaseObjectTypeWrapper> wrappers)
    {
        AtomicInteger count = new AtomicInteger();
        for (MithraBaseObjectTypeWrapper wrapper : wrappers)
        {
            this.processWrapper(wrapper, count);
        }
        this.getExecutor().waitUntilDone();
        return count.get();
    }

    private void processWrapper(MithraBaseObjectTypeWrapper wrapper, AtomicInteger count)
    {
        if (!wrapper.isImported() || isGenerateImported())
        {
            try
            {
                applyTemplates(wrapper, count);
            }
            catch (Exception e)
            {
                throw new MithraGeneratorException("Failed to generate classes for " + wrapper.getClassName()+" with nested exception "+e.getClass().getName()+": "+e.getMessage(), e);
            }
        }
    }

    public void execute()
    {
        setFileGenerationOptions();
        if (!executed)
        {
            try
            {
                long start = System.currentTimeMillis();
                this.logger.info("MithraGenerator MD5: " + this.getMd5());
                File file = parseAndValidate();
                GenerationLogger generationLogger = this.getGenerationLogger();
                generationLogger.setOldGenerationLog(GenerationLog.readOldLog(this.getGeneratedDir(), file.getPath()));
                generationLogger.setNewGenerationLog(new GenerationLog(this.getMd5(), this.getCrc()));
                int normal = this.processMithraObjects(getMithraObjects().values());
                int mithraInterfaces = this.processMithraInterfaces(getMithraInterfaces().values());
                int embedded = this.processMithraObjects(getMithraEmbeddedValueObjects().values());
                int enumerations = this.processMithraObjects(getMithraEnumerations().values());
                if (!generationLogger.getNewGenerationLog().isSame(generationLogger.getOldGenerationLog()))
                {
                    generationLogger.getNewGenerationLog().writeLog(this.getGeneratedDir(), file.getPath());
                }
                this.logger.info("Wrote " + normal + " normal/pure/temp, " +
                        mithraInterfaces + " interface, " +
                        embedded + " embedded value, " +
                        enumerations + " enumeration Mithra objects (" + (System.currentTimeMillis() - start) + " ms)");
                executed = true;
                AwaitingThreadExecutor executor = this.getExecutor();
                if (executor != null)
                {
                    executor.shutdown();
                }
            }
            catch(MithraGeneratorException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                throw new MithraGeneratorException("Exception in mithra code generation "+e.getClass().getName()+": "+e.getMessage(), e);
            }
        }
        else
        {
            this.logger.info("skipped");
        }
    }

    public static void main(String[] args)
    {
        MithraXMLObjectTypeParser parser = new MithraXMLObjectTypeParser("H:/projects/Mithra/xml/mithra/test/MithraClassList.xml");
        parser.setLogger(new StdOutLogger());

        CoreMithraGenerator gen = new CoreMithraGenerator();
        gen.setMithraObjectTypeParser(parser);
        gen.setLogger(new StdOutLogger());

        gen.setGeneratedDir("H:/temp/Mithra/src");
        gen.setNonGeneratedDir("H:/temp/Mithra/src");
        gen.setGenerateGscListMethod(true);

        gen.execute();
    }
}
