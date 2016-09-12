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

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.*;
import com.google.gwt.user.client.rpc.AsyncCallback;

import java.util.List;
import java.util.ArrayList;


public class ControllerPanel extends VerticalPanel
{

    private boolean isPartial;
    private MithraCacheUi ui;
    private static final Header PARTIAL_HEADER = new Header("Partially Cached Classes");
    private static final Header FULL_HEADER = new Header("Fully Cached Classes");
    private Grid grid;
    private String filter;
    private Image progressIcon;
    private List<CachedClassData> cachedClassList;

    public ControllerPanel(boolean partial, MithraCacheUi ui)
    {
        super();
        this.ui = ui;
        this.setSpacing(2);
        this.setWidth("100%");
        isPartial = partial;

        if (isPartial)
        {
            this.add(PARTIAL_HEADER);
            RolloverButton clearButton = new RolloverButton("Clear all partially cached", ui.getImages().clearAllPartiallyCachedOn(),
                    ui.getImages().clearAllPartiallyCachedOff());
            clearButton.addClickHandler(new ClickHandler()
            {
                public void onClick( ClickEvent sender )
                {
                    clearAllPartiallyCached();
                }
            });
            this.add(clearButton);
        }
        else
        {
            this.add(FULL_HEADER);
        }
    }

    private void clearAllPartiallyCached()
    {
        setInProgress();
        ui.getAsyncService().clearAllQueryCaches(this.ui.getMithraManagerLocation(), new AbstractAsyncCallback<List<CachedClassData>>()
        {
            public void onSuccess(List<CachedClassData> result)
            {
                setCachedClassDataList(result);
            }
        });
    }

    public void setInProgress()
    {
        clearGrid();
        progressIcon = ui.createProgressIcon();
        this.add(progressIcon);
    }

    private void clearGrid()
    {
        if (this.grid != null)
        {
            this.remove(this.grid);
            this.grid = null;
        }
    }

    public void setCachedClassDataList(List<CachedClassData> cachedClassDataList)
    {
        ArrayList<CachedClassData> list = new ArrayList<CachedClassData>(cachedClassDataList.size());
        for(CachedClassData data : cachedClassDataList)
        {
            if(data.isPartialCache() == this.isPartial)
            {
                list.add(data);
            }
        }
        this.cachedClassList = list;
        populateGrid();
        if (progressIcon != null)
        {
            this.remove(progressIcon);
            progressIcon = null;
        }
    }

    private void populateGrid()
    {
        List<CachedClassData> filtered = this.cachedClassList;
        if (filter != null && filter.length() > 0)
        {
            filtered = new ArrayList<CachedClassData>(cachedClassList.size());
            for(final CachedClassData data : cachedClassList)
            {
                if(MithraCacheUi.contains(data.getClassName().toLowerCase(), filter))
                {
                    filtered.add(data);
                }
            }
        }
        clearGrid();
        if (filtered.size() > 0)
        {
            grid = new Grid(filtered.size(), 4);
            grid.setWidth("100%");
            for(int i=0;i<filtered.size();i++)
            {
                final CachedClassData data = filtered.get(i);
                populateRow(grid, i, data);
            }
            this.add(grid);
        }
    }

    private void populateRow(Grid grid, int row, CachedClassData data)
    {
        grid.setText(row, 0, data.getClassName());
        grid.setText(row, 1, ui.formatLong(data.getCacheSize()));
        RolloverButton clearButton = createCacheButton(data);
        grid.setWidget(row, 2, clearButton);
        RolloverButton loggerButton = createLoggerButton(data);
        grid.setWidget(row, 3, loggerButton);
        for(int i=0;i<4;i++)
        {
            setStyle(grid, row, i);
        }
    }

    private void setStyle(Grid grid, int row, int col)
    {
        grid.getCellFormatter().setStyleName(row, col, "mcu-text-"+(( (row & 1)  == 0)? "light": "dark"));
    }

    private RolloverButton createLoggerButton(final CachedClassData data)
    {
        if (data.isSqlOff())
        {
            RolloverButton button = new RolloverButton("SQL is Off", ui.getImages().sqlIsOffOn(), ui.getImages().sqlIsOffOff());
            button.addClickHandler(new ClickHandler()
            {
                public void onClick(ClickEvent sender)
                {
                    ui.getAsyncService().turnSqlOn(ui.getMithraManagerLocation(), data.getClassName(), new AbstractAsyncCallback<CachedClassData>()
                    {
                        public void onSuccess(CachedClassData result)
                        {
                            updateData(result);
                        }
                    });
                }
            });
            return button;
        }
        else if (data.isSqlOn())
        {
            RolloverButton button = new RolloverButton("SQL is On", ui.getImages().sqlIsOnOn(), ui.getImages().sqlIsOnOff());
            button.addClickHandler(new ClickHandler()
            {
                public void onClick(ClickEvent sender)
                {
                    ui.getAsyncService().turnSqlMaxOn(ui.getMithraManagerLocation(), data.getClassName(), new AbstractAsyncCallback<CachedClassData>()
                    {
                        public void onSuccess(CachedClassData result)
                        {
                            updateData(result);
                        }
                    });
                }
            });
            return button;
        }
        else
        {
            RolloverButton button = new RolloverButton("SQL is Max On", ui.getImages().sqlIsMaxOnOn(), ui.getImages().sqlIsMaxOnOff());
            button.addClickHandler(new ClickHandler()
            {
                public void onClick(ClickEvent sender)
                {
                    ui.getAsyncService().turnSqlOff(ui.getMithraManagerLocation(), data.getClassName(), new AbstractAsyncCallback<CachedClassData>()
                    {
                        public void onSuccess(CachedClassData result)
                        {
                            updateData(result);
                        }
                    });
                }
            });
            return button;
        }
    }

    private void updateData(CachedClassData cachedClassData)
    {
        Grid g = this.grid;
        if (g != null && (filter == null || MithraCacheUi.contains(cachedClassData.getClassName().toLowerCase(), filter)))
        {
            for(int i=0;i<g.getRowCount();i++)
            {
                if (g.getText(i, 0).equals(cachedClassData.getClassName()))
                {
                    populateRow(g, i, cachedClassData);
                }
            }
        }
    }

    private RolloverButton createCacheButton(final CachedClassData data)
    {
        if (this.isPartial)
        {
            RolloverButton clearButton = new RolloverButton("Clear Cache", ui.getImages().clearCacheOn(), ui.getImages().clearCacheOff());
            clearButton.addClickHandler(new ClickHandler()
            {
                public void onClick(ClickEvent sender)
                {
                    clearCache(data.getClassName());
                }
            });
            return clearButton;
        }
        else
        {
            RolloverButton clearButton = new RolloverButton("Reload Cache", ui.getImages().reloadCacheOn(), ui.getImages().reloadCacheOff());
            clearButton.addClickHandler(new ClickHandler()
            {
                public void onClick(ClickEvent sender)
                {
                    reloadCache(data.getClassName());
                }
            });
            return clearButton;
        }
    }

    public void setFilter(String filter)
    {
        this.filter = filter.toLowerCase();
        this.populateGrid();
    }

    private abstract class AbstractAsyncCallback<T> implements AsyncCallback<T>
    {
        public void onFailure(Throwable caught)
        {
            ui.onFailure(caught);
        }
    }

    private void clearCache(String className)
    {
        ui.getAsyncService().clearCache(this.ui.getMithraManagerLocation(), className, new AbstractAsyncCallback<CachedClassData>()
        {
            public void onSuccess(CachedClassData result)
            {
                updateData(result);
            }
        });
    }

    private void reloadCache(String className)
    {
        ui.getAsyncService().reloadCache(this.ui.getMithraManagerLocation(), className, new AbstractAsyncCallback<CachedClassData>()
        {
            public void onSuccess(CachedClassData result)
            {
                updateData(result);
            }
        });
    }


}
