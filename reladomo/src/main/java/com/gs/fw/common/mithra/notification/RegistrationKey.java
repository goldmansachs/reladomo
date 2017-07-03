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

class RegistrationKey
{
    private String classname;
    private String subject;

    public RegistrationKey(String subject, String className)
    {
        this.classname = className;
        this.subject = subject;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof RegistrationKey)) return false;

        final RegistrationKey registrationKey = (RegistrationKey) o;

        if (classname != null ? !classname.equals(registrationKey.classname) : registrationKey.classname != null)
            return false;
        return !(subject != null ? !subject.equals(registrationKey.subject) : registrationKey.subject != null);
    }

    public String getClassname()
    {
        return classname;
    }

    public int hashCode()
    {
        int result;
        result = (classname != null ? classname.hashCode() : 0);
        result = 29 * result + (subject != null ? subject.hashCode() : 0);
        return result;
    }

    public String toString()
    {
        return classname + "," + subject;
    }

    public String getSubject()
    {
        return subject;
    }
}
