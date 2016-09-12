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

package com.gs.fw.common.mithra.ui.gwt.client;

import com.google.gwt.user.client.ui.ImageBundle;
import com.google.gwt.user.client.ui.AbstractImagePrototype;


public interface Images extends ImageBundle
{
    @Resource("image/button/refresh_on.gif")
    public AbstractImagePrototype refreshOn();

    @Resource("image/button/refresh_off.gif")
    public AbstractImagePrototype refreshOff();

    @Resource("image/button/force_gc_on.gif")
    public AbstractImagePrototype forceGcOn();

    @Resource("image/button/force_gc_off.gif")
    public AbstractImagePrototype forceGcOff();

    @Resource("image/button/clear_all_partially_cached_on.gif")
    public AbstractImagePrototype clearAllPartiallyCachedOn();

    @Resource("image/button/clear_all_partially_cached_off.gif")
    public AbstractImagePrototype clearAllPartiallyCachedOff();

    @Resource("image/button/clear_cache_on.gif")
    public AbstractImagePrototype clearCacheOn();

    @Resource("image/button/clear_cache_off.gif")
    public AbstractImagePrototype clearCacheOff();

    @Resource("image/button/reload_cache_on.gif")
    public AbstractImagePrototype reloadCacheOn();

    @Resource("image/button/reload_cache_off.gif")
    public AbstractImagePrototype reloadCacheOff();

    @Resource("image/button/filter_on.gif")
    public AbstractImagePrototype filterOn();

    @Resource("image/button/filter_off.gif")
    public AbstractImagePrototype filterOff();

    @Resource("image/button/sql_is_on_on.gif")
    public AbstractImagePrototype sqlIsOnOn();

    @Resource("image/button/sql_is_on_off.gif")
    public AbstractImagePrototype sqlIsOnOff();

    @Resource("image/button/sql_is_off_on.gif")
    public AbstractImagePrototype sqlIsOffOn();

    @Resource("image/button/sql_is_off_off.gif")
    public AbstractImagePrototype sqlIsOffOff();

    @Resource("image/button/sql_is_max_on_on.gif")
    public AbstractImagePrototype sqlIsMaxOnOn();

    @Resource("image/button/sql_is_max_on_off.gif")
    public AbstractImagePrototype sqlIsMaxOnOff();

}
