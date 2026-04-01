"""
Voronoi polygon approximations for lugar poblado boundaries.

Since INE Guatemala does not publish official lugar poblado boundaries,
this module generates Voronoi tessellations from lugar poblado centroids
clipped to municipio boundaries. These polygons are approximations suitable
for choropleth visualization — they represent the "zone of influence" of
each centroid within its municipio, not the actual extent of the settlement.

This is the recommended approach for sub-municipal choropleth mapping in
Guatemala when official boundaries are unavailable.

Notes
-----
- Lugares poblados with NULL coordinates (codes ending in ``999``,
  e.g. ``102999``) are excluded — they cannot participate in Voronoi
  tessellation. These represent unnamed informal settlements ("Otros
  Lugares Poblados") without a defined centroid.
- Municipios with only one lugar poblado (with valid coordinates) will
  return that municipio's full boundary polygon as the Voronoi cell.
- The resulting polygons are approximations. Do not use for precise
  area calculations or official cartography.

Examples
--------
>>> import geoquetzal as gq
>>>
>>> # Voronoi polygons for all lugares poblados (~20K polygons)
>>> gdf = gq.voronoi_lugares_poblados()
>>>
>>> # Single departamento
>>> gdf = gq.voronoi_lugares_poblados(departamento="Sacatepéquez")
>>>
>>> # Single municipio
>>> gdf = gq.voronoi_lugares_poblados(municipio="Antigua Guatemala")
>>>
>>> # Join with census data and map
>>> lp  = gq.lugares_poblados(departamento="Sacatepéquez")
>>> vor = gq.voronoi_lugares_poblados(departamento="Sacatepéquez")
>>> gdf = vor.merge(lp, on=["departamento", "municipio", "lugar_poblado"])
>>> gdf["pct_internet"] = gdf["pch9_i_si"] / gdf["poblacion_total"]
>>> gdf.plot(column="pct_internet", legend=True, edgecolor="white")
"""

from typing import Optional, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPoint, Point
from shapely.ops import unary_union

from geoquetzal._lookup import resolve_departamento, resolve_municipio
from geoquetzal.geography import municipios as _municipios
from geoquetzal.lugares_poblados import lugares_poblados as _lugares_poblados


def _voronoi_clipped_to_polygon(points: list, clip_polygon) -> dict:
    """
    Generate Voronoi polygons from a list of shapely Points,
    clipped to clip_polygon. Returns a dict mapping point index
    to clipped Voronoi polygon.
    """
    from scipy.spatial import Voronoi
    import numpy as np
    from shapely.geometry import Polygon
    from shapely.ops import unary_union

    if len(points) == 0:
        return {}

    # Single point — entire municipio polygon
    if len(points) == 1:
        return {0: clip_polygon}

    coords = np.array([[p.x, p.y] for p in points])

    # Add far-away mirror points to ensure all regions are bounded
    # (standard trick for Voronoi on finite sets)
    bounds = clip_polygon.bounds  # minx, miny, maxx, maxy
    width  = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    margin = max(width, height) * 10

    mirror_points = np.array([
        [bounds[0] - margin, bounds[1] - margin],
        [bounds[0] - margin, bounds[3] + margin],
        [bounds[2] + margin, bounds[1] - margin],
        [bounds[2] + margin, bounds[3] + margin],
    ])
    all_coords = np.vstack([coords, mirror_points])

    vor = Voronoi(all_coords)

    result = {}
    for i, point in enumerate(points):
        region_index = vor.point_region[i]
        region = vor.regions[region_index]

        if -1 in region or len(region) == 0:
            # Unbounded region — clip a large buffer instead
            poly = Point(point).buffer(margin)
        else:
            vertices = [vor.vertices[v] for v in region]
            poly = Polygon(vertices)

        clipped = poly.intersection(clip_polygon)
        if not clipped.is_empty:
            result[i] = clipped

    return result


def voronoi_lugares_poblados(
    departamento: Optional[Union[str, int]] = None,
    municipio: Optional[Union[str, int]] = None,
) -> gpd.GeoDataFrame:
    """Generate Voronoi polygon approximations for lugar poblado boundaries.

    Lugar poblado boundaries are not published by INE Guatemala. This function
    generates Voronoi tessellations from lugar poblado centroids, clipped to
    municipio boundaries. The result is suitable for choropleth visualization
    at sub-municipal level.

    Parameters
    ----------
    departamento : str or int, optional
        Filter by departamento name or code.
    municipio : str or int, optional
        Filter by municipio name or code.

    Returns
    -------
    geopandas.GeoDataFrame
        One row per lugar poblado with columns:
        ``departamento``, ``municipio``, ``lugar_poblado``, ``nombre``,
        ``lat``, ``longitud``, ``geometry`` (Voronoi polygon, EPSG:4326).

    Notes
    -----
    Lugares poblados with NULL coordinates (codes ending in ``999``) are
    excluded. Municipios with a single lugar poblado return the full
    municipio polygon as the Voronoi cell.

    Examples
    --------
    >>> import geoquetzal as gq
    >>>
    >>> # All Voronoi polygons for Sacatepéquez
    >>> gdf = gq.voronoi_lugares_poblados(departamento="Sacatepéquez")
    >>>
    >>> # Join with census data
    >>> lp  = gq.lugares_poblados(departamento="Sacatepéquez")
    >>> vor = gq.voronoi_lugares_poblados(departamento="Sacatepéquez")
    >>> gdf = vor.merge(lp, on=["departamento", "municipio", "lugar_poblado"])
    >>>
    >>> # Choropleth: % households with internet access
    >>> gdf["pct_internet"] = gdf["pch9_i_si"] / gdf["poblacion_total"]
    >>> gdf.plot(column="pct_internet", cmap="YlGnBu", legend=True,
    ...          edgecolor="white", linewidth=0.3)
    """
    try:
        from scipy.spatial import Voronoi  # noqa: F401
    except ImportError:
        raise ImportError(
            "scipy is required for Voronoi tessellation.\n"
            "Install it with: pip install scipy"
        )

    # --- Load lugar poblado centroids ---
    lp = _lugares_poblados(departamento=departamento, municipio=municipio)

    # Exclude NULL coordinates (999 codes — unnamed settlements)
    lp_valid = lp.dropna(subset=["longitud", "lat"]).copy()
    n_excluded = len(lp) - len(lp_valid)
    if n_excluded > 0:
        print(
            f"   ℹ {n_excluded} lugares poblados excluidos por coordenadas nulas "
            f"(códigos terminados en 999 — asentamientos sin nombre oficial)."
        )

    if lp_valid.empty:
        raise ValueError("No lugares poblados con coordenadas válidas encontrados.")

    # --- Load municipio boundaries ---
    if municipio is not None:
        _, _, depto_code, _ = resolve_municipio(municipio)
        munis = _municipios(departamento=depto_code)
    elif departamento is not None:
        munis = _municipios(departamento=departamento)
    else:
        munis = _municipios()

    munis = munis.to_crs("EPSG:4326")

    # --- Generate Voronoi per municipio ---
    result_rows = []

    muni_groups = lp_valid.groupby("municipio")

    for muni_code, group in muni_groups:
        # Get municipio boundary
        muni_boundary = munis[munis["codigo_muni"] == muni_code]
        if muni_boundary.empty:
            continue
        clip_poly = unary_union(muni_boundary.geometry.values)

        # Build point list
        points = [
            Point(row["longitud"], row["lat"])
            for _, row in group.iterrows()
        ]
        indices = list(group.index)

        # Generate clipped Voronoi polygons
        voronoi_polys = _voronoi_clipped_to_polygon(points, clip_poly)

        for local_idx, global_idx in enumerate(indices):
            if local_idx not in voronoi_polys:
                continue
            row = lp_valid.loc[global_idx]
            result_rows.append({
                "departamento":  int(row["departamento"]),
                "municipio":     int(row["municipio"]),
                "lugar_poblado": int(row["lugar_poblado"]),
                "nombre":        row["nombre"],
                "lat":           row["lat"],
                "longitud":      row["longitud"],
                "geometry":      voronoi_polys[local_idx],
            })

    if not result_rows:
        raise ValueError("No se pudieron generar polígonos Voronoi.")

    gdf = gpd.GeoDataFrame(result_rows, geometry="geometry", crs="EPSG:4326")
    print(f"   ✓ {len(gdf):,} polígonos Voronoi generados")
    return gdf.reset_index(drop=True)
