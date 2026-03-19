"""
Emigration microdata from INE Guatemala (XII Censo 2018).

Provides access to person-level emigration records. Each row represents
one person who emigrated from a household, with their sex, age at
departure, and year of departure.

Downloads a single Parquet file (~1.6 MB) from GitHub Releases on first
call and caches it locally.

Source: INE Guatemala — XII Censo Nacional de Población y VII de Vivienda 2018
URL: https://censo2018.ine.gob.gt/descarga
Data hosted at: https://github.com/geoquetzal/censo2018/releases/tag/emigracion-v1.0

Variables
---------
- ``DEPARTAMENTO``: Department code (1–22)
- ``MUNICIPIO``: Municipality code (e.g. 301)
- ``COD_MUNICIPIO``: Municipality code within department
- ``ZONA``: Zone (special handling for Guatemala City)
- ``AREA``: 1 = Urbano, 2 = Rural
- ``NUM_VIVIENDA``: Housing unit number
- ``NUM_HOGAR``: Household number within housing unit
- ``ID_EMIGRACION``: Emigrant ID
- ``PEI3``: Sex (1 = Hombre, 2 = Mujer)
- ``PEI4``: Age when they left
- ``PEI5``: Year they left (9999 = unknown)

Examples
--------
>>> import geoquetzal as gq
>>>
>>> # All emigration records (~1.6 MB download)
>>> df = gq.emigracion()
>>>
>>> # Filter by departamento (name or code)
>>> df = gq.emigracion(departamento="Huehuetenango")
>>> df = gq.emigracion(departamento=13)
>>>
>>> # Filter by municipio
>>> df = gq.emigracion(municipio="Antigua Guatemala")
>>> df = gq.emigracion(municipio=301)
>>>
>>> # With geometry joined at municipio level
>>> gdf = gq.emigracion(departamento="Huehuetenango", geometry="municipio")
"""

from typing import Optional, Union

import pandas as pd

from geoquetzal._lookup import (
    DEPARTAMENTO_CODES,
    resolve_departamento,
    resolve_municipio,
    _get_municipio_catalogue,
)
from geoquetzal.cache import get_cache_dir

# ---------------------------------------------------------------------------
# GitHub Release URL
# ---------------------------------------------------------------------------

_RELEASE_URL = (
    "https://github.com/geoquetzal/censo2018/releases/download/"
    "emigracion-v1.0/emigracion.parquet"
)

# ---------------------------------------------------------------------------
# Variable metadata
# ---------------------------------------------------------------------------

VARIABLES = {
    "DEPARTAMENTO": {
        "etiqueta": "Departamento",
        "tipo": "geográfica",
        "valores": DEPARTAMENTO_CODES,
    },
    "MUNICIPIO": {
        "etiqueta": "Municipio (ver catálogo)",
        "tipo": "geográfica",
        "valores": "Ver geoquetzal.describe('emigracion', 'MUNICIPIO')",
    },
    "COD_MUNICIPIO": {
        "etiqueta": "Código de municipio dentro del departamento",
        "tipo": "geográfica",
    },
    "ZONA": {
        "etiqueta": "Zona (recodificación para municipios de Guatemala)",
        "tipo": "geográfica",
    },
    "AREA": {
        "etiqueta": "Área",
        "tipo": "geográfica",
        "valores": {1: "Urbano", 2: "Rural"},
    },
    "NUM_VIVIENDA": {
        "etiqueta": "Número de vivienda",
        "tipo": "identificador",
    },
    "NUM_HOGAR": {
        "etiqueta": "Número del hogar en la vivienda",
        "tipo": "identificador",
    },
    "ID_EMIGRACION": {
        "etiqueta": "Número del emigrante",
        "tipo": "identificador",
    },
    "PEI3": {
        "etiqueta": "¿Cuál es el sexo de la persona que se fue?",
        "tipo": "demográfica",
        "valores": {1: "Hombre", 2: "Mujer"},
    },
    "PEI4": {
        "etiqueta": "¿Qué edad tenía la persona cuando se fue?",
        "tipo": "demográfica",
        "valores": "Edad en años (numérico)",
    },
    "PEI5": {
        "etiqueta": "¿En qué año se fue?",
        "tipo": "demográfica",
        "valores": "Año (9999 = no sabe / no responde)",
    },
}


# ---------------------------------------------------------------------------
# Download helper
# ---------------------------------------------------------------------------

def _download_parquet() -> "Path":
    """Download the emigration parquet file, with caching."""
    import requests
    from pathlib import Path

    cache_dir = get_cache_dir()
    filename = "emigracion.parquet"
    cached = cache_dir / filename

    if cached.exists():
        return cached

    print("⬇  Descargando datos de emigración (~1.6 MB)...")

    try:
        resp = requests.get(_RELEASE_URL, timeout=120, stream=True)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise ConnectionError(
            f"No se pudo descargar de {_RELEASE_URL}.\n"
            f"Verifique su conexión a internet.\nError: {exc}"
        ) from exc

    cached.write_bytes(resp.content)
    size_mb = cached.stat().st_size / 1024 / 1024
    print(f"   ✓ {size_mb:.1f} MB descargados")
    return cached


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def emigracion(
    departamento: Optional[Union[str, int]] = None,
    municipio: Optional[Union[str, int]] = None,
    geometry: Optional[str] = None,
) -> Union[pd.DataFrame, "gpd.GeoDataFrame"]:
    """Load emigration microdata from INE Censo 2018.

    Each row is one person who emigrated from a Guatemalan household.
    Returns raw microdata for the researcher to analyse with pandas.

    Downloads a single Parquet file (~1.6 MB) from GitHub Releases on
    first call and caches it locally.

    Parameters
    ----------
    departamento : str or int, optional
        Filter by departamento. Accepts name (accent-insensitive)
        or code (1–22).
    municipio : str or int, optional
        Filter by municipio. Accepts name (accent-insensitive)
        or code (e.g. 301, 1301).
    geometry : {"departamento", "municipio"}, optional
        If specified, joins boundary geometry and returns a
        GeoDataFrame. The user must specify the level.

    Returns
    -------
    pandas.DataFrame or geopandas.GeoDataFrame
        Microdata with one row per emigrant. If ``geometry`` is
        specified, includes a ``geometry`` column for mapping.

    Examples
    --------
    >>> import geoquetzal as gq
    >>>
    >>> # All records (~1.6 MB download on first call)
    >>> df = gq.emigracion()
    >>> len(df)
    242203
    >>>
    >>> # By departamento name or code
    >>> df = gq.emigracion(departamento="Huehuetenango")
    >>> df = gq.emigracion(departamento=13)
    >>>
    >>> # By municipio
    >>> df = gq.emigracion(municipio="Antigua Guatemala")
    >>> df = gq.emigracion(municipio=301)
    >>>
    >>> # With geometry for mapping
    >>> gdf = gq.emigracion(departamento="San Marcos", geometry="municipio")
    """
    path = _download_parquet()
    df = pd.read_parquet(path)

    # --- Filter by departamento ---
    if departamento is not None:
        code, name = resolve_departamento(departamento)
        df = df[df["DEPARTAMENTO"] == code].copy()

    # --- Filter by municipio ---
    if municipio is not None:
        muni_code, muni_name, depto_code, depto_name = resolve_municipio(municipio)
        df = df[df["MUNICIPIO"] == muni_code].copy()

    # --- Add readable labels ---
    cat = _get_municipio_catalogue()
    df = df.merge(
        cat[["codigo_muni", "municipio", "departamento"]].rename(
            columns={"codigo_muni": "MUNICIPIO"}
        ),
        on="MUNICIPIO",
        how="left",
    )

    # --- Join geometry (always on numeric codes, not names) ---
    if geometry is not None:
        import geopandas as gpd
        from geoquetzal.geography import departamentos as _deptos, municipios as _munis

        if geometry == "departamento":
            geo = _deptos(name=departamento) if departamento is not None else _deptos()
            df = geo[["codigo_depto", "geometry"]].merge(
                df, left_on="codigo_depto", right_on="DEPARTAMENTO", how="right"
            )

        elif geometry == "municipio":
            if departamento is not None:
                geo = _munis(departamento=departamento)
            elif municipio is not None:
                muni_code, muni_name, depto_code, depto_name = resolve_municipio(municipio)
                geo = _munis(departamento=depto_code)
            else:
                geo = _munis()
            df = geo[["codigo_muni", "geometry"]].merge(
                df, left_on="codigo_muni", right_on="MUNICIPIO", how="right"
            )

        else:
            raise ValueError(
                f"geometry='{geometry}' no válido. "
                f"Use 'departamento' o 'municipio'."
            )

        df = gpd.GeoDataFrame(df, geometry="geometry")

    return df.reset_index(drop=True)


def describe(variable: Optional[str] = None) -> Union[pd.DataFrame, dict]:
    """Describe the variables in the emigration dataset.

    Parameters
    ----------
    variable : str, optional
        Specific variable name (e.g. ``"PEI3"``). If ``None``,
        returns a summary table of all variables.

    Returns
    -------
    pandas.DataFrame or dict
        Summary table or detailed variable info.

    Examples
    --------
    >>> import geoquetzal as gq
    >>> gq.describe_emigracion()           # all variables
    >>> gq.describe_emigracion("PEI3")     # details for sex variable
    """
    if variable is not None:
        var_upper = variable.upper()
        if var_upper not in VARIABLES:
            raise ValueError(
                f"Variable '{variable}' no encontrada en el dataset de emigración.\n"
                f"Variables disponibles: {', '.join(VARIABLES.keys())}"
            )
        info = VARIABLES[var_upper]
        result = {"variable": var_upper, **info}

        # For MUNICIPIO, include the catalogue
        if var_upper == "MUNICIPIO":
            result["catalogo"] = _get_municipio_catalogue()

        return result

    rows = []
    for name, info in VARIABLES.items():
        rows.append({
            "variable": name,
            "etiqueta": info.get("etiqueta", ""),
            "tipo": info.get("tipo", ""),
        })
    return pd.DataFrame(rows)
