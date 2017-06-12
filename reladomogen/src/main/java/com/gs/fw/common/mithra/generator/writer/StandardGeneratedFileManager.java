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

package com.gs.fw.common.mithra.generator.writer;

import com.gs.fw.common.mithra.generator.MithraGeneratorException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class StandardGeneratedFileManager implements GeneratedFileManager
{
    private Options options;

    @Override
    public void setOptions(Options options)
    {
        this.options = options;
    }

    @Override
    public boolean shouldCreateFile(boolean replaceIfExists, String packageName, String className, String outputFileSuffix)
    {
        String targetDir = replaceIfExists ? this.options.generatedDir : this.options.nonGeneratedDir;
        File outDir = new File(targetDir, packageName);
        File outFile = new File(outDir, className + outputFileSuffix + ".java");
        if (outFile.exists() && !replaceIfExists)
        {
            return false;
        }
        if (!outFile.exists() && !replaceIfExists && !this.options.generateConcreteClasses)
        {
            if (this.options.warnAboutConcreteClasses)
            {
                this.options.logger.info("concrete class file '" + outFile + "' does not exist and generateConcreteClasses flag is turned off. This might lead to compilation errors");
            }
            return false;
        }
        if (outFile.exists())
        {
            if (this.options.generationLogger.getNewGenerationLog().isSame(this.options.generationLogger.getOldGenerationLog()))
            {
                this.options.logger.info("skipping " + outFile.getName() + " because it's new and no changes to generator have been made");
                return false;
            }
        }
        return true;
    }

    @Override
    public void writeFile(boolean replaceIfExists, String packageName, String className, String outputFileSuffix, byte[] fileData, final AtomicInteger count) throws IOException
    {
        String targetDir = replaceIfExists ? this.options.generatedDir : this.options.nonGeneratedDir;
        File outDir = new File(targetDir, packageName);
        File outFile = new File(outDir, className + outputFileSuffix + ".java");

        outDir.mkdirs();
        copyIfChanged(fileData, outFile, count);
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
            this.options.logger.info("wrote file: " + outFile.getName());
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

}
