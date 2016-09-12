
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

import com.gs.fw.common.mithra.generator.MithraBaseObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.metamodel.EnumerationMemberType;
import com.gs.fw.common.mithra.generator.metamodel.MithraEnumerationType;

import java.util.ArrayList;
import java.util.List;

public class MithraEnumerationTypeWrapper extends MithraBaseObjectTypeWrapper
{

    private final List<MemberWrapper> members = new ArrayList<MemberWrapper>();

    public MithraEnumerationTypeWrapper(MithraEnumerationType wrapped, String sourceFileName, String importedSource)
    {
        super(wrapped, sourceFileName, importedSource);
        this.extractMembers();
    }

    public MithraEnumerationType getWrapped()
    {
        return (MithraEnumerationType) super.getWrapped();
    }

    public String getDescription()
    {
        return "enumeration";
    }

    public String getObjectType()
    {
        return "enumeration";
    }

    public boolean isTablePerSubclassSuperClass()
    {
        return false;
    }

    public String getPackageName()
    {
        return this.getWrapped().getPackageName();
    }

    public String getEnumName()
    {
        return super.getClassName();
    }

    public MemberWrapper[] getMembers()
    {
        MemberWrapper[] members = new MemberWrapper[this.members.size()];
        return this.members.toArray(members);
    }

    public MemberWrapper getMember(String name)
    {
        for (MemberWrapper member : this.members)
        {
            if (name.equals(member.getName()))
            {
                return member;
            }
        }
        return null;
    }

    private void extractMembers()
    {
        for (int i = 0; i < this.getWrapped().getMembers().size(); i++)
        {
            EnumerationMemberType member = (EnumerationMemberType) this.getWrapped().getMembers().get(i);
            MemberWrapper wrapper = new MemberWrapper(member.getName());
            this.members.add(wrapper);
        }
    }

    public static class MemberWrapper
    {
        private final String name;

        public MemberWrapper(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return this.name;
        }
    }
}
