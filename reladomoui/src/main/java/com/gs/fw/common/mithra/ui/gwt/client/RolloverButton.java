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

import com.google.gwt.user.client.ui.Image;
import com.google.gwt.user.client.ui.MouseListenerAdapter;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.user.client.ui.AbstractImagePrototype;


public class RolloverButton extends Image
{

    public RolloverButton(String title, final AbstractImagePrototype onImage, final AbstractImagePrototype offImage)
    {
        super("");
        this.setTitle(title);
        offImage.applyTo(this);
        addMouseListener(new MouseListenerAdapter() {
            public void onMouseEnter(Widget sender)
            {
                onImage.applyTo(RolloverButton.this);
            }

            public void onMouseLeave(Widget sender)
            {
                offImage.applyTo(RolloverButton.this);
            }
        });
    }

    
}
