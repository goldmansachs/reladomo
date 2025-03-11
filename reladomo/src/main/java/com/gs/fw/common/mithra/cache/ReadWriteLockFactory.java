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

package com.gs.fw.common.mithra.cache;

/**
 * Factory for creating appropriate ReadWriteLock implementations based on the JDK version.
 * This factory creates ReadWriteLockJdk21 for JDK 9+ and ReadWriteLock for JDK 8.
 */
public class ReadWriteLockFactory
{
    private static final boolean IS_JDK9_OR_LATER = isJdk9OrLater();
    
    /**
     * Determines if the current JDK version is 9 or later.
     * 
     * @return true if running on JDK 9 or later, false otherwise
     */
    private static boolean isJdk9OrLater() {
        try {
            // java.lang.Module was introduced in Java 9
            Class.forName("java.lang.Module");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
    
    /**
     * Creates an appropriate ReadWriteLock implementation based on the JDK version.
     * 
     * @return a ReadWriteLock implementation
     */
    public static ReadWriteLock createReadWriteLock()
    {
        if (IS_JDK9_OR_LATER) {
            return new ReadWriteLockJdk21();
        } else {
            return new ReadWriteLock();
        }
    }
}
