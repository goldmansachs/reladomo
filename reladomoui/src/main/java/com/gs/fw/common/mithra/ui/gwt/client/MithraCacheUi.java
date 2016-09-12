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

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.google.gwt.user.client.Window;

import java.util.List;



public class MithraCacheUi implements EntryPoint
{
    private VerticalPanel mainPanel = new VerticalPanel();
    private HorizontalPanel jvmPanel= new HorizontalPanel();
    private ControllerPanel partiallyCachedPanel;
    private ControllerPanel fullyCachedPanel;
    private MithraCacheUiRemoteServiceAsync asyncService;
    private TextBox filterTextBox = new TextBox();
    private Images images;
    private String mithraManagerLocation;

    public void onModuleLoad()
    {
        mithraManagerLocation = Window.Location.getParameter("mithraManagerLocation");

        images = (Images) GWT.create(Images.class);
        asyncService = MithraCacheUiRemoteService.App.getInstance();
        mainPanel.setWidth("100%");
        mainPanel.setSpacing(2);
        RootPanel.get().add(mainPanel);
        HorizontalPanel panel = new HorizontalPanel();
        panel.setSpacing(5);
        panel.add(filterTextBox);

        RolloverButton filterButton = new RolloverButton("Filter", images.filterOn(), images.filterOff());
        filterButton.addClickHandler(new ClickHandler()
        {
            public void onClick( ClickEvent sender )
            {
                filter();
            }
        });
        panel.add(filterButton);
        RolloverButton refreshButton = new RolloverButton("Refresh", images.refreshOn(), images.refreshOff());
        refreshButton.addClickHandler(new ClickHandler()
        {
            public void onClick( ClickEvent sender )
            {
                refresh();
            }
        });
        panel.add(refreshButton);

        String header = "Mithra Cache Control";
        if(mithraManagerLocation != null && !mithraManagerLocation.isEmpty())
        {
            header = header + " @ "+mithraManagerLocation;
        }
        mainPanel.add(new Header( header ));
        mainPanel.add(panel);
        mainPanel.setCellHorizontalAlignment(refreshButton, HasHorizontalAlignment.ALIGN_CENTER);
//        mainPanel.add(new Label("Mithra Cache Control"));
        mainPanel.add(new Header("JVM Memory"));

        jvmPanel.setSpacing(5);
        mainPanel.add(jvmPanel);

        partiallyCachedPanel = new ControllerPanel(true, this);
        mainPanel.add(partiallyCachedPanel);

        fullyCachedPanel = new ControllerPanel(false, this);
        mainPanel.add(fullyCachedPanel);
        refresh();
    }

    public String getMithraManagerLocation()
    {
        return mithraManagerLocation;
    }

    private void filter()
    {
        partiallyCachedPanel.setFilter(this.filterTextBox.getText());
        fullyCachedPanel.setFilter(this.filterTextBox.getText());
    }

    private void refresh()
    {
        refreshJvmMemory();
        refreshCacheControllers();
    }

    private void refreshCacheControllers()
    {
        partiallyCachedPanel.setInProgress();
        fullyCachedPanel.setInProgress();
        asyncService.getCachedClasses(mithraManagerLocation, new AbstractAsyncCallback<List<CachedClassData>>()
        {
            public void onSuccess(List<CachedClassData> cachedClassDataList)
            {
                partiallyCachedPanel.setCachedClassDataList(cachedClassDataList);
                fullyCachedPanel.setCachedClassDataList(cachedClassDataList);
            }
        });
    }

    public Image createProgressIcon()
    {
        return new Image("progressIcon.gif");
    }

    private void forceGc()
    {
        jvmPanel.clear();
        jvmPanel.add(createProgressIcon());
        asyncService.forceGc(mithraManagerLocation, new JvmMemoryAsyncCallback());
    }

    private void refreshJvmMemory()
    {
        jvmPanel.clear();
        jvmPanel.add(createProgressIcon());
        asyncService.getJvmMemory(mithraManagerLocation, new JvmMemoryAsyncCallback());
    }

    public String formatLong(long val)
    {
        String valAsString = ""+val;
        int i=valAsString.length();
        int begin = i - 3 < 0 ? 0: i - 3;
        String result = valAsString.substring(begin, i);
        i -= 3;
        for(;i>0;i-=3)
        {
            begin = i - 3 < 0 ? 0: i - 3;
            result = valAsString.substring(begin, i) + "," + result;
        }
        return result;
    }

    public void onFailure(Throwable caught)
    {
        Window.alert("Error communicating with the server! " + caught.getMessage());
    }

    public MithraCacheUiRemoteServiceAsync getAsyncService()
    {
        return asyncService;
    }

    public Images getImages()
    {
        return images;
    }

    private abstract class AbstractAsyncCallback<T> implements AsyncCallback<T>
    {
        public void onFailure(Throwable caught)
        {
            MithraCacheUi.this.onFailure(caught);
        }
    }

    private class JvmMemoryAsyncCallback extends AbstractAsyncCallback<JvmMemory>
    {
        public void onSuccess(JvmMemory memory)
        {
            jvmPanel.clear();
            jvmPanel.add(new BoldLabel("Total Memory: "));
            jvmPanel.add(new Label(formatLong(memory.getTotalMemory())));
            jvmPanel.add(new BoldLabel("Free Memory: "));
            jvmPanel.add(new Label(formatLong(memory.getFreeMemory())));
            jvmPanel.add(new BoldLabel("Used Memory: "));
            jvmPanel.add(new Label(formatLong(memory.getUsed())));
            RolloverButton gcButton = new RolloverButton("Force GC", images.forceGcOn(), images.forceGcOff());
            gcButton.addClickHandler(new ClickHandler()
            {
                public void onClick( ClickEvent sender )
                {
                    forceGc();
                }
            });
            jvmPanel.add(gcButton);
        }
    }

    private static class BoldLabel extends Label
    {
        private BoldLabel(String text)
        {
            super(text);
            this.setStylePrimaryName("mcu-bold-label");
        }
    }

    public static boolean contains(String original, String filter)
    {
        return original.indexOf(filter) >= 0;
    }

}
