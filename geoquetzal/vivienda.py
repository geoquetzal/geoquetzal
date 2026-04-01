"""
Housing (Vivienda) microdata from INE Guatemala (XII Censo 2018).

Downloads partitioned Parquet files from GitHub Releases. Each
departamento is a separate file.

Source: INE Guatemala — XII Censo Nacional de Población y VII de Vivienda 2018
Data hosted at: https://github.com/geoquetzal/censo2018/releases/tag/vivienda-v1.0

Variables cover housing type, wall/roof/floor materials, and occupancy status.

Examples
--------
>>> import geoquetzal as gq
>>>
>>> # Single departamento
>>> df = gq.vivienda(departamento="Huehuetenango")
>>>
>>> # All of Guatemala
>>> df = gq.vivienda()
>>>
>>> # With geometry
>>> gdf = gq.vivienda(departamento="Sacatepéquez", geometry="municipio")
"""

from pathlib import Path
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
# GitHub Release URLs
# ---------------------------------------------------------------------------

_RELEASE_BASE = (
    "https://github.com/geoquetzal/censo2018/releases/download/vivienda-v1.0"
)


def _parquet_url(depto_code: int) -> str:
    return f"{_RELEASE_BASE}/vivienda_depto_{depto_code:02d}.parquet"


# ---------------------------------------------------------------------------
# Variable metadata
# ---------------------------------------------------------------------------

VARIABLES = {
    "DEPARTAMENTO": {"etiqueta": "Departamento", "tipo": "geográfica"},
    "MUNICIPIO": {"etiqueta": "Municipio", "tipo": "geográfica"},
    "COD_MUNICIPIO": {"etiqueta": "Código de municipio dentro del departamento", "tipo": "geográfica"},
    "ZONA": {"etiqueta": "Zona", "tipo": "geográfica"},
    "AREA": {
        "etiqueta": "Área",
        "tipo": "geográfica",
        "valores": {1: "Urbano", 2: "Rural"},
    },
    "NUM_VIVIENDA": {"etiqueta": "Número de vivienda", "tipo": "identificador"},
    "PCV1": {
        "etiqueta": "¿El tipo de la vivienda es:",
        "tipo": "vivienda",
        "valores": {
            1: "Casa formal", 2: "Apartamento",
            3: "Cuarto de casa de vecindad (palomar)", 4: "Rancho",
            5: "Vivienda improvisada", 6: "Otro tipo de vivienda particular",
            9: "Particular no especificada", 10: "Vivienda colectiva",
            11: "Sin vivienda",
        },
    },
    "PCV2": {
        "etiqueta": "¿Cuál es el material predominante en las paredes exteriores?",
        "tipo": "materiales",
        "valores": {
            1: "Ladrillo", 2: "Block", 3: "Concreto", 4: "Adobe",
            5: "Madera", 6: "Lámina Metálica", 7: "Bajareque",
            8: "Lepa, palo o caña", 9: "Material de desecho",
            10: "Otro", 99: "No especificado",
        },
    },
    "PCV3": {
        "etiqueta": "¿Cuál es el material predominante en el techo?",
        "tipo": "materiales",
        "valores": {
            1: "Concreto", 2: "Lámina Metálica", 3: "Asbesto cemento",
            4: "Teja", 5: "Paja, palma o similar",
            6: "Material de desecho", 7: "Otro", 9: "No especificado",
        },
    },
    "PCV4": {
        "etiqueta": "¿La condición de la vivienda es?",
        "tipo": "vivienda",
        "valores": {
            1: "Ocupada", 2: "Ocupada de uso temporal",
            3: "Desocupada", 4: "Moradores ausentes / rechazo total",
        },
    },
    "PCV5": {
        "etiqueta": "¿Cuál es el material predominante en el piso?",
        "tipo": "materiales",
        "valores": {
            1: "Ladrillo cerámico", 2: "Ladrillo de cemento",
            3: "Ladrillo de barro", 4: "Torta de cemento",
            5: "Parqué/vinil", 6: "Madera", 7: "Tierra", 8: "Otro",
        },
    },
}


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _download_parquet(depto_code: int) -> Path:
    """Download a single departamento parquet file, with caching."""
    import requests

    cache_dir = get_cache_dir()
    filename = f"vivienda_depto_{depto_code:02d}.parquet"
    cached = cache_dir / filename

    if cached.exists():
        return cached

    url = _parquet_url(depto_code)
    print(f"⬇  Descargando vivienda depto {depto_code:02d} ({DEPARTAMENTO_CODES[depto_code]})...")

    try:
        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise ConnectionError(
            f"No se pudo descargar de {url}.\n"
            f"Verifique su conexión a internet.\nError: {exc}"
        ) from exc

    cached.write_bytes(resp.content)
    size_mb = cached.stat().st_size / 1024 / 1024
    print(f"   ✓ {size_mb:.1f} MB descargados")
    return cached


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def viviendas(
    departamento: Optional[Union[str, int]] = None,
    municipio: Optional[Union[str, int]] = None,
    geometry: Optional[str] = None,
) -> Union[pd.DataFrame, "gpd.GeoDataFrame"]:
    """Load housing microdata from INE Censo 2018.

    Each row is one housing unit. Downloads partitioned Parquet files
    from GitHub.

    Parameters
    ----------
    departamento : str or int, optional
        Filter by departamento. Only downloads that departamento's file.
    municipio : str or int, optional
        Filter by municipio. Downloads the parent departamento's file.
    geometry : {"departamento", "municipio"}, optional
        Join boundary geometry at the specified level.

    Returns
    -------
    pandas.DataFrame or geopandas.GeoDataFrame

    Examples
    --------
    >>> import geoquetzal as gq
    >>>
    >>> df = gq.vivienda(departamento="Huehuetenango")
    >>> df = gq.vivienda(municipio="Antigua Guatemala")
    >>> df = gq.vivienda()
    >>> gdf = gq.vivienda(departamento="Petén", geometry="municipio")
    """
    # Determine which departamento(s) to download
    if municipio is not None:
        muni_code, muni_name, depto_code, depto_name = resolve_municipio(municipio)
        codes_to_download = [depto_code]
    elif departamento is not None:
        code, name = resolve_departamento(departamento)
        codes_to_download = [code]
    else:
        codes_to_download = sorted(DEPARTAMENTO_CODES.keys())
        if len(codes_to_download) > 1:
            print(f"⬇  Descargando vivienda para los 22 departamentos...")

    # Download and concatenate
    frames = []
    for code in codes_to_download:
        path = _download_parquet(code)
        frames.append(pd.read_parquet(path))

    df = pd.concat(frames, ignore_index=True)

    if len(codes_to_download) > 1:
        print(f"   ✓ {len(df):,} viviendas cargadas")

    # Filter by municipio if specified
    if municipio is not None:
        df = df[df["MUNICIPIO"] == muni_code].copy()

    # Add readable labels
    cat = _get_municipio_catalogue()
    df = df.merge(
        cat[["codigo_muni", "municipio", "departamento"]].rename(
            columns={"codigo_muni": "MUNICIPIO"}
        ),
        on="MUNICIPIO",
        how="left",
    )

    # Join geometry
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
                geo = _munis(departamento=depto_code)
            else:
                geo = _munis()
            df = geo[["codigo_muni", "geometry"]].merge(
                df, left_on="codigo_muni", right_on="MUNICIPIO", how="right"
            )
        else:
            raise ValueError(
                f"geometry='{geometry}' no válido. Use 'departamento' o 'municipio'."
            )
        df = gpd.GeoDataFrame(df, geometry="geometry")

    return df.reset_index(drop=True)


def describe(variable: Optional[str] = None) -> Union[pd.DataFrame, dict]:
    """Describe the variables in the vivienda dataset.

    Parameters
    ----------
    variable : str, optional
        Specific variable (e.g. ``"PCV1"``). If ``None``, lists all.

    Returns
    -------
    pandas.DataFrame or dict

    Examples
    --------
    >>> import geoquetzal as gq
    >>> gq.describe_vivienda()           # all variables
    >>> gq.describe_vivienda("PCV2")     # wall material details
    """
    if variable is not None:
        var_upper = variable.upper()
        if var_upper not in VARIABLES:
            raise ValueError(
                f"Variable '{variable}' no encontrada.\n"
                f"Variables disponibles: {', '.join(VARIABLES.keys())}"
            )
        return {"variable": var_upper, **VARIABLES[var_upper]}

    rows = []
    for name, info in VARIABLES.items():
        rows.append({
            "variable": name,
            "etiqueta": info.get("etiqueta", ""),
            "tipo": info.get("tipo", ""),
        })
    return pd.DataFrame(rows)


# Backward-compatible alias (v1.0 used vivienda, v1.1+ uses viviendas)
vivienda = viviendas

