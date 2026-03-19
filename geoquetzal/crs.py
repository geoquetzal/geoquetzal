"""
Coordinate Reference System utilities for Guatemala.

Import explicitly: ``from geoquetzal.crs import to_gtm, suggest_crs``
"""

from typing import Optional

import geopandas as gpd
import pandas as pd

CRS_WGS84 = "EPSG:4326"
CRS_UTM16N = "EPSG:32616"
CRS_UTM15N = "EPSG:32615"
CRS_GTM = (
    "+proj=tmerc +lat_0=0 +lon_0=-90.5 +k=0.9998 "
    "+x_0=500000 +y_0=0 +datum=WGS84 +units=m +no_defs"
)
"""Guatemala Transverse Mercator — the national projected CRS."""

_CRS_CATALOGUE = [
    {"code": "ESRI:103598", "name": "Guatemala Transverse Mercator (GTM)",
     "units": "metres", "use": "National mapping, IGN standard", "recommended": True,
     "notes": "Recommended for all of Guatemala."},
    {"code": "EPSG:32616", "name": "WGS 84 / UTM Zone 16N",
     "units": "metres", "use": "Eastern and central Guatemala", "recommended": True,
     "notes": "Covers 90°W to 84°W."},
    {"code": "EPSG:32615", "name": "WGS 84 / UTM Zone 15N",
     "units": "metres", "use": "Western Guatemala and Petén", "recommended": False,
     "notes": "Covers 96°W to 90°W."},
    {"code": "EPSG:4326", "name": "WGS 84",
     "units": "degrees", "use": "GPS, web maps, data exchange", "recommended": False,
     "notes": "Geographic CRS (not projected)."},
]


def suggest_crs(gdf: Optional[gpd.GeoDataFrame] = None) -> pd.DataFrame:
    """Suggest coordinate reference systems for Guatemala."""
    recs = pd.DataFrame(_CRS_CATALOGUE)
    if gdf is not None:
        bounds = gdf.total_bounds
        center_lon = (bounds[0] + bounds[2]) / 2
        if center_lon < -90:
            recs.loc[recs["code"] == "EPSG:32615", "recommended"] = True
            recs.loc[recs["code"] == "EPSG:32616", "recommended"] = False
    return recs.sort_values(["recommended", "code"], ascending=[False, True]).reset_index(drop=True)


def to_gtm(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Transform to Guatemala Transverse Mercator (GTM)."""
    return gdf.to_crs(CRS_GTM)


def to_utm16n(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Transform to UTM Zone 16N (EPSG:32616)."""
    return gdf.to_crs(CRS_UTM16N)
