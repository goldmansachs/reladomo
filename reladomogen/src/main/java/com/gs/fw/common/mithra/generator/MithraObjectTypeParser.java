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

import com.gs.fw.common.mithra.generator.filesystem.FauxFileSystem;
import com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType;

import java.io.File;
import java.util.Map;

/*
    A parser for Mithra object type definitions. The parser produces type definitions
    which are consumed by the generator to generate classes.

    The contract with the caller (generator) is as follows :
       1. Generator invokes a sequence of setter methods
       2. After all setter methods have been invoked, generator invokes parse
       3. After parse returns successfully, generator invokes the various getter methods

 */
public interface MithraObjectTypeParser
{
    // invoked before parse
    void setLogger(Logger logger);
    void setForceOffHeap(boolean forceOffHeap);
    void setDefaultFinalGetters(boolean defaultFinalGetters);
    void setFauxFileSystem(FauxFileSystem fauxFileSystem);

    // actually parse
    // returns the name of the class list, usually as a file path.
    String parse();


    // invoked after a successful parse
    Map<String,MithraObjectTypeWrapper> getMithraObjects();
    Map<String,MithraEmbeddedValueObjectTypeWrapper> getMithraEmbeddedValueObjects();
    Map<String,MithraInterfaceType> getMithraInterfaces();
    Map<String,MithraEnumerationTypeWrapper> getMithraEnumerations();

    String getChecksum();
}
