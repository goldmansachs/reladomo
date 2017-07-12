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

package com.gs.fw.common.mithra.generator.filesystem;

import com.gs.fw.common.mithra.generator.GenerationLogger;
import com.gs.fw.common.mithra.generator.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/* 
    Interface to abstract the creation of a generation Java file.
    Implementations could create the file using standard Java I/O or other mechanisms like annotation processor's filer api. 
 */
public interface GeneratedFileManager
{
    class Options
    {
        public final String generatedDir;
        public final String nonGeneratedDir;
        public final boolean warnAboutConcreteClasses;
        public final boolean generateConcreteClasses;
        public final GenerationLogger generationLogger;
        public final Logger logger;
        public final FauxFileSystem fauxFileSystem;

        public Options(String generatedDir, String nonGeneratedDir, boolean warnAboutConcreteClasses, boolean generateConcreteClasses,
                       GenerationLogger generationLogger, Logger logger, FauxFileSystem fauxFileSystem)
        {
            this.generatedDir = generatedDir;
            this.nonGeneratedDir = nonGeneratedDir;
            this.warnAboutConcreteClasses = warnAboutConcreteClasses;
            this.generateConcreteClasses = generateConcreteClasses;
            this.generationLogger = generationLogger;
            this.logger = logger;
            this.fauxFileSystem = fauxFileSystem;
        }
    }
    
    void setOptions(Options options);

    /*
        This method should answer True if the file should be created.
        If True, writeFile will be subsequently called with the same input parameters along with the content to be written to the file
     */
    boolean shouldCreateFile(boolean replaceIfExists, String packageName, String className, String fileSuffix);

    /*
        This method should create the file with the content from the input file data
     */
    void writeFile(boolean replaceIfExists, String packageName, String className, String fileSuffix, byte[] fileData, AtomicInteger count) throws IOException;

    /*
    returns null if file doesn't exist
     */
    byte[] readFileInGeneratedDir(String relativePath) throws IOException;
}
