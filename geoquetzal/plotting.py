"""
Visualization helpers for GeoQuetzal.

Import explicitly: ``from geoquetzal.plotting import plot_map, explore``
"""

from typing import Optional, Union

import geopandas as gpd
import matplotlib.pyplot as plt


def plot_map(gdf, column=None, cmap="YlOrRd", title=None,
             legend=True, figsize=(10, 10), edgecolor="white",
             linewidth=0.5, label_column=None, label_fontsize=7,
             ax=None, **kwargs):
    """Create a static map of a GeoDataFrame."""
    if ax is None:
        fig, ax = plt.subplots(1, 1, figsize=figsize)
    else:
        fig = ax.get_figure()

    plot_kwargs = dict(ax=ax, edgecolor=edgecolor, linewidth=linewidth, **kwargs)
    if column is not None:
        plot_kwargs.update(column=column, cmap=cmap, legend=legend,
                          legend_kwds={"shrink": 0.6})
    gdf.plot(**plot_kwargs)

    if label_column and label_column in gdf.columns:
        for _, row in gdf.iterrows():
            c = row.geometry.centroid
            ax.annotate(row[label_column], xy=(c.x, c.y), ha="center",
                       va="center", fontsize=label_fontsize, fontweight="light")

    if title:
        ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_axis_off()
    fig.tight_layout()
    return fig


def explore(gdf, column=None, tooltip=None, cmap="YlOrRd",
            tiles="CartoDB positron", **kwargs):
    """Create an interactive zoomable map using folium."""
    explore_kwargs = dict(tiles=tiles, **kwargs)
    if column:
        explore_kwargs.update(column=column, cmap=cmap)
    if tooltip:
        explore_kwargs["tooltip"] = tooltip
    explore_kwargs.setdefault("style_kwds", {"fillOpacity": 0.7, "weight": 1, "color": "white"})
    return gdf.explore(**explore_kwargs)
