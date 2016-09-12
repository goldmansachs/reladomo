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

import java.util.HashMap;


public abstract class Cardinality
{

    private Cardinality()
    {
        // enum type
    }

    public abstract boolean isToMany();

    public abstract boolean isFromMany();

    public abstract Cardinality getReverseCardinality();

    public abstract String getNameAsClassName();

    public boolean isManyToMany()
    {
        return this.isFromMany() && this.isToMany();
    }

    private static final Cardinality ONE_TO_MANY =
            new Cardinality()
            {
                public boolean isToMany()
                {
                    return true;
                }

                public boolean isFromMany()
                {
                    return false;
                }

                public Cardinality getReverseCardinality()
                {
                    return Cardinality.MANY_TO_ONE;
                }

                public String getNameAsClassName()
                {
                    return "OneToMany";
                }
            };

    private static final Cardinality MANY_TO_MANY =
            new Cardinality()
            {
                public boolean isToMany()
                {
                    return true;
                }

                public boolean isFromMany()
                {
                    return true;
                }

                public Cardinality getReverseCardinality()
                {
                    return Cardinality.MANY_TO_MANY;
                }

                public String getNameAsClassName()
                {
                    return "ManyToMany";
                }
            };

    private static final Cardinality ONE_TO_ONE =
            new Cardinality()
            {
                public boolean isToMany()
                {
                    return false;
                }

                public boolean isFromMany()
                {
                    return false;
                }

                public Cardinality getReverseCardinality()
                {
                    return Cardinality.ONE_TO_ONE;
                }

                public String getNameAsClassName()
                {
                    return "OneToOne";
                }
            };

    private static final Cardinality MANY_TO_ONE =
            new Cardinality()
            {
                public boolean isToMany()
                {
                    return false;
                }

                public boolean isFromMany()
                {
                    return true;
                }

                public Cardinality getReverseCardinality()
                {
                    return Cardinality.ONE_TO_MANY;
                }

                public String getNameAsClassName()
                {
                    return "ManyToOne";
                }
            };

    private static HashMap enumMap = new HashMap();

    static
    {
        enumMap.put("one-to-many", ONE_TO_MANY);
        enumMap.put("many-to-many", MANY_TO_MANY);
        enumMap.put("one-to-one", ONE_TO_ONE);
        enumMap.put("many-to-one", MANY_TO_ONE);
    }

    public static Cardinality getByName(String name)
    {
        return (Cardinality) enumMap.get(name);
    }
}
