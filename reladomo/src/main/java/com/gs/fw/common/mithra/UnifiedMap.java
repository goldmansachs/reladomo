package com.gs.fw.common.mithra;

import org.eclipse.collections.api.map.MutableMap;

public class UnifiedMap<K, V> extends org.eclipse.collections.impl.map.mutable.UnifiedMap<K, V> implements MutableMap<K, V>
{
    public UnifiedMap()
    {
        super();
    }

    public UnifiedMap(int initialCapacity)
    {
        super(initialCapacity, 0.75F);
    }

    public static <K, V> UnifiedMap<K, V> newMap()
    {
        return new UnifiedMap<K, V>();
    }

    public static <K, V> UnifiedMap<K, V> newMap(int initialCapacity)
    {
        return new UnifiedMap<K, V>(initialCapacity);
    }
}