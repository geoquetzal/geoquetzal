"""
Household (Hogar) microdata from INE Guatemala (XII Censo 2018).

Downloads partitioned Parquet files from GitHub Releases. Each
departamento is a separate file (~1–7 MB), so requesting a single
departamento is fast and lightweight.

Source: INE Guatemala — XII Censo Nacional de Población y VII de Vivienda 2018
Data hosted at: https://github.com/geoquetzal/censo2018/releases/tag/hogar-v1.0

Variables include housing tenure, water source, sanitation, electricity,
household appliances, cooking fuel, remittances, and emigration indicators.

Examples
--------
>>> import geoquetzal as gq
>>>
>>> # Single departamento (~2 MB download)
>>> df = gq.hogares(departamento="Huehuetenango")
>>>
>>> # All of Guatemala (~38 MB total)
>>> df = gq.hogares()
>>>
>>> # With geometry
>>> gdf = gq.hogares(departamento="Sacatepéquez", geometry="municipio")
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
    "https://github.com/geoquetzal/censo2018/releases/download/hogar-v1.0"
)


def _parquet_url(depto_code: int) -> str:
    """Build the download URL for a departamento's parquet file."""
    return f"{_RELEASE_BASE}/hogar_depto_{depto_code:02d}.parquet"


# ---------------------------------------------------------------------------
# Variable metadata
# ---------------------------------------------------------------------------

VARIABLES = {
    "DEPARTAMENTO": {"etiqueta": "Departamento", "tipo": "geográfica"},
    "MUNICIPIO": {"etiqueta": "Municipio", "tipo": "geográfica"},
    "COD_MUNICIPIO": {"etiqueta": "Código de municipio dentro del departamento", "tipo": "geográfica"},
    "ZONA": {"etiqueta": "Zona", "tipo": "geográfica"},
    "AREA": {"etiqueta": "Área", "tipo": "geográfica", "valores": {1: "Urbano", 2: "Rural"}},
    "NUM_VIVIENDA": {"etiqueta": "Número de vivienda", "tipo": "identificador"},
    "NUM_HOGAR": {"etiqueta": "Número del hogar en la vivienda", "tipo": "identificador"},
    "PCH1": {
        "etiqueta": "¿La vivienda que ocupa este hogar es:",
        "tipo": "vivienda",
        "valores": {1: "Propia pagada totalmente", 2: "Propia pagándola a plazos",
                    3: "Alquilada", 4: "Cedida o prestada", 5: "Propiedad comunal",
                    6: "Otra condición"},
    },
    "PCH2": {
        "etiqueta": "¿La persona propietaria de esta vivienda es:",
        "tipo": "vivienda",
        "valores": {1: "Hombre", 2: "Mujer", 3: "Ambos", 9: "No declarado"},
    },
    "PCH3": {
        "etiqueta": "¿La persona que toma las principales decisiones en el hogar es:",
        "tipo": "hogar",
        "valores": {1: "Hombre", 2: "Mujer", 3: "Ambos", 9: "No declarado"},
    },
    "PCH4": {
        "etiqueta": "¿De dónde obtiene principalmente el agua para consumo del hogar?",
        "tipo": "servicios",
        "valores": {
            1: "Tubería red dentro de la vivienda",
            2: "Tubería red fuera de la vivienda, pero en el terreno",
            3: "Chorro público", 4: "Pozo perforado público o privado",
            5: "Agua de lluvia", 6: "Río", 7: "Lago",
            8: "Manantial o nacimiento", 9: "Camión o tonel", 10: "Otro",
        },
    },
    "PCH5": {
        "etiqueta": "¿Qué tipo de servicio sanitario tiene este hogar?",
        "tipo": "servicios",
        "valores": {
            1: "Inodoro conectado a red de drenajes",
            2: "Inodoro conectado a fosa séptica",
            3: "Excusado lavable", 4: "Letrina o pozo ciego", 5: "No tiene",
        },
    },
    "PCH6": {
        "etiqueta": "¿El servicio sanitario es de:",
        "tipo": "servicios",
        "valores": {1: "Uso exclusivo del hogar", 2: "Uso compartido con otros hogares"},
    },
    "PCH7": {
        "etiqueta": "¿Cómo se deshace de las aguas grises?",
        "tipo": "servicios",
        "valores": {1: "Conectado a red de drenajes", 2: "Sin red de drenajes"},
    },
    "PCH8": {
        "etiqueta": "¿De qué tipo de alumbrado dispone principalmente el hogar?",
        "tipo": "servicios",
        "valores": {
            1: "Red de energía eléctrica", 2: "Panel solar / eólico",
            3: "Gas corriente", 4: "Candela", 5: "Otro",
        },
    },
    "PCH9_A": {"etiqueta": "¿Cuenta este hogar con radio?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_B": {"etiqueta": "¿Cuenta este hogar con estufa?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_C": {"etiqueta": "¿Cuenta este hogar con televisor?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_D": {"etiqueta": "¿Cuenta este hogar con servicio de cable?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_E": {"etiqueta": "¿Cuenta este hogar con refrigeradora?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_F": {"etiqueta": "¿Cuenta este hogar con tanque o depósito de agua?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_G": {"etiqueta": "¿Cuenta este hogar con lavadora de ropa?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_H": {"etiqueta": "¿Cuenta este hogar con computadora?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_I": {"etiqueta": "¿Cuenta este hogar con servicio de internet?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_J": {"etiqueta": "¿Cuenta este hogar con temazcal o tuj?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_K": {"etiqueta": "¿Cuenta este hogar con sistema de agua caliente?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_L": {"etiqueta": "¿Cuenta este hogar con moto?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH9_M": {"etiqueta": "¿Cuenta este hogar con carro?", "tipo": "equipamiento", "valores": {1: "Sí", 2: "No"}},
    "PCH10": {
        "etiqueta": "¿Cómo elimina la mayor parte de la basura en el hogar?",
        "tipo": "servicios",
        "valores": {
            1: "Servicio municipal", 2: "Servicio privado", 3: "La queman",
            4: "La entierran", 5: "La tiran en un río, quebrada o mar",
            6: "La tiran en cualquier lugar", 7: "Abonera / reciclaje", 8: "Otro",
        },
    },
    "PCH11": {"etiqueta": "¿De cuántos cuartos dispone este hogar?", "tipo": "vivienda"},
    "PCH12": {"etiqueta": "Del total de los cuartos, ¿cuántos utiliza como dormitorios?", "tipo": "vivienda"},
    "PCH13": {
        "etiqueta": "¿Dispone el hogar de un cuarto exclusivo para cocinar?",
        "tipo": "vivienda",
        "valores": {1: "Sí", 2: "No"},
    },
    "PCH14": {
        "etiqueta": "¿Cuál es la fuente principal que utiliza el hogar para cocinar?",
        "tipo": "servicios",
        "valores": {
            1: "Gas propano", 2: "Leña", 3: "Electricidad",
            4: "Carbón", 5: "Gas corriente", 6: "Otra fuente", 7: "No cocina",
        },
    },
    "PCH15": {
        "etiqueta": "¿Recibe remesas con regularidad por parte de personas que viven en el extranjero?",
        "tipo": "económica",
        "valores": {1: "Sí", 2: "No"},
    },
    "PEI1": {
        "etiqueta": "A partir del año 2002, ¿alguna persona que pertenecía a este hogar, se fue a vivir a otro país y aún no ha regresado?",
        "tipo": "migración",
        "valores": {1: "Sí", 2: "No"},
    },
    "PEI2": {"etiqueta": "¿Cuál es el total de personas que se fueron y aún no han regresado?", "tipo": "migración"},
    "PEI2_E": {"etiqueta": "Total de emigrantes con sexo, edad y año de partida reportado", "tipo": "migración"},
}


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _download_parquet(depto_code: int) -> Path:
    """Download a single departamento parquet file, with caching."""
    import requests

    cache_dir = get_cache_dir()
    filename = f"hogar_depto_{depto_code:02d}.parquet"
    cached = cache_dir / filename

    if cached.exists():
        return cached

    url = _parquet_url(depto_code)
    print(f"⬇  Descargando hogares depto {depto_code:02d} ({DEPARTAMENTO_CODES[depto_code]})...")

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

def hogares(
    departamento: Optional[Union[str, int]] = None,
    municipio: Optional[Union[str, int]] = None,
    geometry: Optional[str] = None,
) -> Union[pd.DataFrame, "gpd.GeoDataFrame"]:
    """Load household microdata from INE Censo 2018.

    Each row is one household. Downloads partitioned Parquet files
    from GitHub (~1–7 MB per departamento).

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
    >>> # Single departamento (~2 MB download)
    >>> df = gq.hogares(departamento="Huehuetenango")
    >>>
    >>> # By municipio
    >>> df = gq.hogares(municipio="Antigua Guatemala")
    >>>
    >>> # All of Guatemala (~38 MB, downloads 22 files)
    >>> df = gq.hogares()
    >>>
    >>> # With geometry
    >>> gdf = gq.hogares(departamento="Petén", geometry="municipio")
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
            print(f"⬇  Descargando hogares para los 22 departamentos...")

    # Download and concatenate
    frames = []
    for code in codes_to_download:
        path = _download_parquet(code)
        frames.append(pd.read_parquet(path))

    df = pd.concat(frames, ignore_index=True)

    if len(codes_to_download) > 1:
        print(f"   ✓ {len(df):,} hogares cargados")

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
    """Describe the variables in the hogares dataset.

    Parameters
    ----------
    variable : str, optional
        Specific variable (e.g. ``"PCH4"``). If ``None``, lists all.

    Returns
    -------
    pandas.DataFrame or dict

    Examples
    --------
    >>> import geoquetzal as gq
    >>> gq.describe_hogares()           # all 37 variables
    >>> gq.describe_hogares("PCH4")     # water source details
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
