"""
Cache management for geographic data downloads.

- MINFIN boundaries (departamentos + municipios) are bundled in the
  package — no download needed. Loaded instantly from local files.
  Source: Ministerio de Finanzas Públicas de Guatemala
  https://github.com/minfin-bi/Mapas-TopoJSON-Guatemala

- GADM v4.1 boundaries (country outline + Guatemala City zones)
  are downloaded from UC Davis and cached locally.
  https://geodata.ucdavis.edu/gadm/gadm4.1/json/

- Census microdata (Parquet) is downloaded from GitHub Releases
  and cached locally.
"""

import hashlib
import os
from io import BytesIO
from pathlib import Path
from typing import Optional

import geopandas as gpd
import requests

_CACHE_DIR: Optional[Path] = None
_USE_CACHE: bool = True

# -----------------------------------------------------------------------
# GADM v4.1 GeoJSON URLs (used for country outline and zonas)
# -----------------------------------------------------------------------

_GADM_BASE = "https://geodata.ucdavis.edu/gadm/gadm4.1/json"
_GADM_URLS = {
    0: f"{_GADM_BASE}/gadm41_GTM_0.json",
    1: f"{_GADM_BASE}/gadm41_GTM_1.json",
    2: f"{_GADM_BASE}/gadm41_GTM_2.json",
}


# -----------------------------------------------------------------------
# Cache management
# -----------------------------------------------------------------------

def get_cache_dir() -> Path:
    """Return the cache directory, creating it if needed."""
    global _CACHE_DIR
    if _CACHE_DIR is not None:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        return _CACHE_DIR

    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        default = base / "geoquetzal" / "cache"
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
        default = base / "geoquetzal"

    default.mkdir(parents=True, exist_ok=True)
    return default


def set_cache(path: Optional[str] = None, *, use_cache: bool = True) -> None:
    """Configure caching behaviour."""
    global _CACHE_DIR, _USE_CACHE
    _CACHE_DIR = Path(path).expanduser().resolve() if path else None
    _USE_CACHE = use_cache


def clear_cache() -> int:
    """Delete all cached files. Returns number of files removed."""
    cache = get_cache_dir()
    removed = 0
    for f in cache.glob("*"):
        if f.is_file():
            f.unlink()
            removed += 1
    return removed


def _cache_key(url: str) -> str:
    h = hashlib.md5(url.encode()).hexdigest()[:12]
    name = url.rsplit("/", 1)[-1].split("?")[0]
    return f"{name}_{h}"


def _download(url: str, label: str) -> bytes:
    """Download a URL with caching. Returns raw bytes."""
    cache_file = get_cache_dir() / _cache_key(url)

    if _USE_CACHE and cache_file.exists():
        return cache_file.read_bytes()

    print(f"⬇  Descargando {label}...")
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise ConnectionError(
            f"No se pudo descargar de {url}.\n"
            f"Verifique su conexión a internet.\nError: {exc}"
        ) from exc

    if _USE_CACHE:
        cache_file.write_bytes(resp.content)

    return resp.content


# -----------------------------------------------------------------------
# MINFIN boundaries (bundled in package)
# -----------------------------------------------------------------------

def _load_minfin(level: str) -> gpd.GeoDataFrame:
    """Load bundled MINFIN TopoJSON for Guatemala.

    Files are included in the pip package — no download needed.

    Parameters
    ----------
    level : {"departamentos", "municipios"}

    Returns
    -------
    geopandas.GeoDataFrame
    """
    filenames = {
        "departamentos": "deptos.json",
        "municipios": "munis.json",
    }
    if level not in filenames:
        raise ValueError(
            f"Nivel '{level}' no válido. Use 'departamentos' o 'municipios'."
        )

    path = Path(__file__).parent / "data" / filenames[level]
    return _read_topojson(path)


def _read_topojson(path) -> gpd.GeoDataFrame:
    """Read a TopoJSON file, handling quantized coordinates.

    Manually decodes arcs and dequantizes coordinates using only
    json + shapely (no extra dependencies).
    """
    import json

    with open(path) as f:
        topo = json.load(f)

    # --- Dequantize arcs ---
    arcs = topo.get("arcs", [])
    transform = topo.get("transform")

    if transform:
        sx, sy = transform["scale"]
        tx, ty = transform["translate"]
        decoded_arcs = []
        for arc in arcs:
            coords = []
            x, y = 0, 0
            for dx, dy in arc:
                x += dx
                y += dy
                coords.append([x * sx + tx, y * sy + ty])
            decoded_arcs.append(coords)
    else:
        decoded_arcs = arcs

    # --- Reconstruct geometries ---
    def _resolve_arc_index(idx):
        """Resolve an arc index (may be negative for reversed arcs)."""
        if idx >= 0:
            return list(decoded_arcs[idx])
        else:
            return list(reversed(decoded_arcs[~idx]))

    def _build_ring(arc_indices):
        """Build a coordinate ring from a list of arc indices."""
        coords = []
        for idx in arc_indices:
            arc_coords = _resolve_arc_index(idx)
            # Skip first point if continuing from previous arc
            if coords:
                arc_coords = arc_coords[1:]
            coords.extend(arc_coords)
        return coords

    def _topo_geom_to_geojson(geom):
        """Convert a TopoJSON geometry object to a GeoJSON geometry."""
        gtype = geom["type"]

        if gtype == "Point":
            coords = geom["coordinates"]
            if transform:
                coords = [coords[0] * sx + tx, coords[1] * sy + ty]
            return {"type": "Point", "coordinates": coords}

        elif gtype == "MultiPoint":
            coords = geom["coordinates"]
            if transform:
                coords = [[c[0] * sx + tx, c[1] * sy + ty] for c in coords]
            return {"type": "MultiPoint", "coordinates": coords}

        elif gtype == "LineString":
            return {
                "type": "LineString",
                "coordinates": _build_ring(geom["arcs"]),
            }

        elif gtype == "MultiLineString":
            return {
                "type": "MultiLineString",
                "coordinates": [_build_ring(ring) for ring in geom["arcs"]],
            }

        elif gtype == "Polygon":
            return {
                "type": "Polygon",
                "coordinates": [_build_ring(ring) for ring in geom["arcs"]],
            }

        elif gtype == "MultiPolygon":
            return {
                "type": "MultiPolygon",
                "coordinates": [
                    [_build_ring(ring) for ring in polygon]
                    for polygon in geom["arcs"]
                ],
            }

        elif gtype == "GeometryCollection":
            return {
                "type": "GeometryCollection",
                "geometries": [
                    _topo_geom_to_geojson(g) for g in geom["geometries"]
                ],
            }

        return None

    # --- Build GeoDataFrame ---
    obj_name = list(topo["objects"].keys())[0]
    obj = topo["objects"][obj_name]

    features = []
    for geom in obj.get("geometries", []):
        geojson_geom = _topo_geom_to_geojson(geom)
        properties = geom.get("properties", {})
        # Also include "id" from TopoJSON as a property
        if "id" in geom and "id" not in properties:
            properties["id"] = geom["id"]
        features.append({
            "type": "Feature",
            "geometry": geojson_geom,
            "properties": properties,
        })

    geojson = {"type": "FeatureCollection", "features": features}
    return gpd.GeoDataFrame.from_features(geojson, crs="EPSG:4326")


# -----------------------------------------------------------------------
# GADM download (GeoJSON — used for country outline and zonas)
# -----------------------------------------------------------------------

def _download_geojson(level: int) -> gpd.GeoDataFrame:
    """Download a GADM GeoJSON for Guatemala at the given admin level.

    Parameters
    ----------
    level : {0, 1, 2}
        0 = country, 1 = departamentos, 2 = municipios

    Returns
    -------
    geopandas.GeoDataFrame
    """
    if level not in _GADM_URLS:
        raise ValueError(f"Nivel {level} no válido. Use 0, 1 o 2.")

    url = _GADM_URLS[level]
    cache_file = get_cache_dir() / _cache_key(url)

    if _USE_CACHE and cache_file.exists():
        try:
            return gpd.read_file(cache_file)
        except Exception:
            cache_file.unlink(missing_ok=True)

    data = _download(url, f"datos geográficos (nivel {level}) de GADM v4.1")
    if _USE_CACHE:
        cache_file.write_bytes(data)

    return gpd.read_file(BytesIO(data))
