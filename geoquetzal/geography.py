"""
Geographic boundary data for Guatemala.

Downloads administrative boundaries at three levels:
country (level 0), departamentos (level 1), and municipios (level 2).

Data sources
------------
- **MINFIN** (default): TopoJSON from Ministerio de Finanzas Públicas
  de Guatemala. 22 departamentos, 340 municipios with INE codes built in.
  No name-matching required. Source:
  https://github.com/minfin-bi/Mapas-TopoJSON-Guatemala

- **GADM** v4.1: GeoJSON from UC Davis. Used for country outline and
  Guatemala City zone polygons. Requires name-matching to INE codes.
  Source: https://geodata.ucdavis.edu/gadm/gadm4.1/json/
"""

import re
import warnings
from typing import Optional, Union

import pandas as pd
import geopandas as gpd

from geoquetzal.cache import _load_minfin, _download_geojson
from geoquetzal._lookup import (
    DEPARTAMENTO_CODES,
    REGIONES,
    normalize_name,
    resolve_departamento,
    resolve_municipio,
    _get_municipio_catalogue,
)


# =====================================================================
#  MINFIN source (default) — clean, simple, 340 municipios
# =====================================================================

def _clean_minfin_deptos(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Clean MINFIN departamento TopoJSON.

    MINFIN attributes: id (depto code), Departamento (name)
    """
    rename = {}
    if "id" in gdf.columns:
        rename["id"] = "codigo_depto"
    if "Departamento" in gdf.columns:
        rename["Departamento"] = "departamento"

    gdf = gdf.rename(columns=rename)

    # Ensure int codes
    gdf["codigo_depto"] = pd.to_numeric(gdf["codigo_depto"], errors="coerce")
    gdf = gdf.dropna(subset=["codigo_depto"])
    gdf["codigo_depto"] = gdf["codigo_depto"].astype(int)

    # MINFIN may use depto codes × 100 (e.g. 100=Guatemala, 700=Sololá)
    # Detect and convert to INE codes (1–22)
    if gdf["codigo_depto"].max() > 22:
        gdf["codigo_depto"] = gdf["codigo_depto"] // 100

    # Drop non-departamento features (e.g. Belice)
    gdf = gdf[gdf["codigo_depto"].isin(DEPARTAMENTO_CODES.keys())].copy()

    # Replace names with canonical INE names (in case of minor differences)
    gdf["departamento"] = (
        gdf["codigo_depto"]
        .map(DEPARTAMENTO_CODES)
        .fillna(gdf["departamento"])
    )

    # Set CRS (MINFIN TopoJSON doesn't declare one, but coordinates are WGS84)
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)

    # Add area in km²
    projected = gdf.to_crs(epsg=32616)
    gdf["area_km2"] = (projected.geometry.area / 1e6).round(2)

    return gdf


def _clean_minfin_munis(gdf: gpd.GeoDataFrame) -> tuple:
    """Clean MINFIN municipio TopoJSON.

    Returns (municipios_gdf, lagos_gdf) — lakes are separated
    instead of discarded.

    MINFIN attributes: id (muni code), Municipio (name),
    id_depto (depto code), Departamento (name)
    """
    rename = {}
    if "id" in gdf.columns:
        rename["id"] = "codigo_muni"
    if "Municipio" in gdf.columns:
        rename["Municipio"] = "municipio"
    if "id_depto" in gdf.columns:
        rename["id_depto"] = "codigo_depto"
    if "Departamento" in gdf.columns:
        rename["Departamento"] = "departamento"

    gdf = gdf.rename(columns=rename)

    # Ensure int codes
    gdf["codigo_muni"] = pd.to_numeric(gdf["codigo_muni"], errors="coerce")
    gdf["codigo_depto"] = pd.to_numeric(gdf["codigo_depto"], errors="coerce")
    gdf = gdf.dropna(subset=["codigo_muni", "codigo_depto"])
    gdf["codigo_muni"] = gdf["codigo_muni"].astype(int)
    gdf["codigo_depto"] = gdf["codigo_depto"].astype(int)

    # MINFIN uses depto codes × 100 (e.g. 100=Guatemala, 700=Sololá)
    # Convert to INE codes (1–22)
    if gdf["codigo_depto"].max() > 22:
        gdf["codigo_depto"] = gdf["codigo_depto"] // 100

    # Set CRS
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)

    # Separate municipios, lakes, and other (Belice)
    cat = _get_municipio_catalogue()
    valid_codes = set(cat["codigo_muni"].values)

    munis = gdf[gdf["codigo_muni"].isin(valid_codes)].copy()
    non_munis = gdf[~gdf["codigo_muni"].isin(valid_codes)]
    lake_mask = non_munis["municipio"].str.contains("Lago", case=False, na=False)
    lagos = non_munis[lake_mask].copy()

    # Replace municipio names with canonical INE names
    munis["departamento"] = (
        munis["codigo_depto"]
        .map(DEPARTAMENTO_CODES)
        .fillna(munis["departamento"])
    )
    muni_names = cat.set_index("codigo_muni")["municipio"]
    munis["municipio"] = (
        munis["codigo_muni"]
        .map(muni_names)
        .fillna(munis["municipio"])
    )

    # Add area in km²
    projected = munis.to_crs(epsg=32616)
    munis["area_km2"] = (projected.geometry.area / 1e6).round(2)

    if len(lagos) > 0:
        proj_lagos = lagos.to_crs(epsg=32616)
        lagos["area_km2"] = (proj_lagos.geometry.area / 1e6).round(2)
        # Clean lake names
        lagos["nombre"] = lagos["municipio"]
        lagos = lagos[["nombre", "geometry", "area_km2"]].reset_index(drop=True)

    return munis, lagos


# =====================================================================
#  GADM source — used for country() and zonas=True
# =====================================================================

# Known GADM → INE depto spelling fixes
_GADM_DEPTO_ALIASES = {
    "quezaltenango": "quetzaltenango",
    "sanmarcos": "san marcos",
    "solola": "solola",
    "elprogreso": "el progreso",
    "santarosa": "santa rosa",
    "bajaverapaz": "baja verapaz",
    "altaverapaz": "alta verapaz",
}

# Hard-coded GID_2 → INE codigo_muni overrides
_GADM_GID_OVERRIDES = {
    # Alta Verapaz
    "GTM.1.5_1": 1611,   # Lanquín → San Agustín Lanquín
    "GTM.1.15_1": 1606,  # Tucurú → San Miguel Tucurú
    # Chimaltenango
    "GTM.3.3_1": 404,    # Comalapa → San Juan Comalapa
    "GTM.3.8_1": 408,    # Pochuta → San Miguel Pochuta
    "GTM.3.15_1": 412,   # Yepocapa → San Pedro Yepocapa
    # Guatemala — Petapa
    "GTM.7.7_1": 117,    # Petapa → San Miguel Petapa
    # Guatemala City zones → all municipio 101
    "GTM.7.17_1": 101, "GTM.7.18_1": 101, "GTM.7.19_1": 101,
    "GTM.7.20_1": 101, "GTM.7.21_1": 101, "GTM.7.22_1": 101,
    "GTM.7.23_1": 101, "GTM.7.24_1": 101, "GTM.7.25_1": 101,
    "GTM.7.26_1": 101, "GTM.7.27_1": 101, "GTM.7.28_1": 101,
    "GTM.7.29_1": 101, "GTM.7.30_1": 101, "GTM.7.31_1": 101,
    "GTM.7.32_1": 101, "GTM.7.33_1": 101, "GTM.7.34_1": 101,
    "GTM.7.35_1": 101, "GTM.7.36_1": 101, "GTM.7.37_1": 101,
    "GTM.7.38_1": 101,
    # Huehuetenango
    "GTM.8.29_1": 1308,  # Soloma → San Pedro Soloma
    # Izabal — exclude
    "GTM.9.5_1": None,   # "?" — unknown feature
    "GTM.9.7_1": None,   # SanLuis — GADM error
    # Jutiapa
    "GTM.11.13_1": 2217, # Quezada → Quesada
    # Quetzaltenango
    "GTM.13.6_1": 917,   # Colomba → Colomba Costa Cuca
    "GTM.13.13_1": 903,  # Olintepeque → San Juan Olintepeque
    "GTM.13.14_1": 909,  # Ostuncalco → San Juan Ostuncalco
    # Quiché
    "GTM.14.5_1": 1406,  # Chichicastenango → Santo Tomás Chichicastenango
    "GTM.14.8_1": 1420,  # Ixcán → Playa Grande Ixcán
    "GTM.14.10_1": 1413, # Nebaj → Santa María Nebaj
    "GTM.14.20_1": 1415, # Uspantán → San Miguel Uspantán
    # Sacatepéquez
    "GTM.16.1_1": 314,   # Alotenango → San Juan Alotenango
    # San Marcos
    "GTM.17.6_1": 1214,  # ElRodeo → San José el Rodeo
    "GTM.17.25_1": 1208, # SanSibinal → Sibinal
    # Sololá — exclude
    "GTM.19.2_1": None,  # "?" — unknown feature
    # Totonicapán
    "GTM.21.3_1": 808,   # SanBartolo → San Bartolo Aguas Calientes
}

# Non-municipio feature names to exclude
_GADM_EXCLUDE_NAMES = {
    "lago de atitlan", "lagodeatitlan", "lago de amatitlan",
    "lagodeamatitlan", "lago atitlan", "lagoatitlan",
    "lago amatitlan", "lagoamatitlan", "lake atitlan",
    "lake amatitlan", "?",
}


def _gadm_normalize(name: str) -> str:
    """Normalize a GADM name, applying known depto spelling fixes."""
    norm = normalize_name(name)
    nospace = norm.replace(" ", "")
    if nospace in _GADM_DEPTO_ALIASES:
        return _GADM_DEPTO_ALIASES[nospace]
    return norm


def _match_depto_code(gadm_name: str) -> int:
    """Match a GADM departamento name to its INE numeric code."""
    fixed = _gadm_normalize(gadm_name)
    for code, ine_name in DEPARTAMENTO_CODES.items():
        if normalize_name(ine_name) == fixed:
            return code
    fixed_nospace = fixed.replace(" ", "")
    for code, ine_name in DEPARTAMENTO_CODES.items():
        if normalize_name(ine_name).replace(" ", "") == fixed_nospace:
            return code
    return None


def _match_muni_code(gadm_muni: str, gadm_depto: str, gadm_gid: str = None) -> int:
    """Match a GADM municipio name to its INE code (multi-layer)."""
    cat = _get_municipio_catalogue()
    norm_muni = normalize_name(gadm_muni)

    # Exclude lakes and unknown features
    if norm_muni in _GADM_EXCLUDE_NAMES or norm_muni.replace(" ", "") in _GADM_EXCLUDE_NAMES:
        return None

    # GID override (most reliable)
    if gadm_gid and gadm_gid in _GADM_GID_OVERRIDES:
        return _GADM_GID_OVERRIDES[gadm_gid]

    # Narrow to department
    depto_code = _match_depto_code(gadm_depto)
    cat_dept = cat[cat["codigo_depto"] == depto_code] if depto_code else cat

    # Exact normalized
    for _, row in cat_dept.iterrows():
        if normalize_name(row["municipio"]) == norm_muni:
            return int(row["codigo_muni"])

    # No-space
    norm_nospace = norm_muni.replace(" ", "")
    for _, row in cat_dept.iterrows():
        if normalize_name(row["municipio"]).replace(" ", "") == norm_nospace:
            return int(row["codigo_muni"])

    # Token-set
    gadm_tokens = sorted(norm_muni.split())
    for _, row in cat_dept.iterrows():
        if sorted(normalize_name(row["municipio"]).split()) == gadm_tokens:
            return int(row["codigo_muni"])

    return None


def _clean_gadm_columns(
    gdf: gpd.GeoDataFrame, level: int, *, zonas: bool = False,
) -> gpd.GeoDataFrame:
    """Rename GADM columns and add INE codes. Used for country() and zonas."""
    rename = {}
    if level == 0:
        rename = {"COUNTRY": "pais", "GID_0": "codigo_pais"}
    elif level == 1:
        rename = {
            "COUNTRY": "pais", "GID_0": "codigo_pais",
            "GID_1": "gid_depto", "NAME_1": "departamento",
            "VARNAME_1": "nombre_alt", "NL_NAME_1": "nombre_local",
            "TYPE_1": "tipo", "ENGTYPE_1": "tipo_en",
            "CC_1": "codigo_cc", "HASC_1": "codigo_hasc",
        }
    elif level == 2:
        rename = {
            "COUNTRY": "pais", "GID_0": "codigo_pais",
            "GID_1": "gid_depto", "NAME_1": "departamento",
            "GID_2": "gid_muni", "NAME_2": "municipio",
            "VARNAME_2": "nombre_alt", "NL_NAME_2": "nombre_local",
            "TYPE_2": "tipo", "ENGTYPE_2": "tipo_en",
            "CC_2": "codigo_cc", "HASC_2": "codigo_hasc",
        }

    existing = {k: v for k, v in rename.items() if k in gdf.columns}
    gdf = gdf.rename(columns=existing)

    if level == 1:
        gdf["codigo_depto"] = gdf["departamento"].apply(_match_depto_code)
        gdf = gdf.dropna(subset=["codigo_depto"])
        gdf["codigo_depto"] = gdf["codigo_depto"].astype(int)
        gdf["departamento"] = gdf["codigo_depto"].map(DEPARTAMENTO_CODES).fillna(gdf["departamento"])

    elif level == 2:
        gdf["codigo_depto"] = gdf["departamento"].apply(_match_depto_code)
        gid_col = "gid_muni" if "gid_muni" in gdf.columns else None
        gdf["codigo_muni"] = gdf.apply(
            lambda r: _match_muni_code(
                r["municipio"], r["departamento"],
                r.get("gid_muni") if gid_col else None,
            ),
            axis=1,
        )

        # Filter unmatched (silently — GID overrides handle known issues)
        unmatched = gdf[gdf["codigo_muni"].isna()]
        if len(unmatched) > 0:
            name_excluded = unmatched["municipio"].apply(
                lambda n: normalize_name(n).replace(" ", "") in _GADM_EXCLUDE_NAMES
                or normalize_name(n) in _GADM_EXCLUDE_NAMES
            )
            gid_excluded = (
                unmatched["gid_muni"].apply(
                    lambda g: g in _GADM_GID_OVERRIDES and _GADM_GID_OVERRIDES[g] is None
                ) if gid_col else pd.Series(False, index=unmatched.index)
            )
            real_unmatched = unmatched[~(name_excluded | gid_excluded)]
            if len(real_unmatched) > 0:
                details = [
                    f"  {r['municipio']} ({r['departamento']}) [GID: {r.get('gid_muni', '?')}]"
                    for _, r in real_unmatched.iterrows()
                ]
                warnings.warn(
                    f"{len(real_unmatched)} municipio(s) GADM sin código INE:\n"
                    + "\n".join(details),
                    stacklevel=3,
                )
            gdf = gdf.dropna(subset=["codigo_muni"])

        gdf = gdf.dropna(subset=["codigo_depto"])
        gdf["codigo_depto"] = gdf["codigo_depto"].astype(int)
        gdf["codigo_muni"] = gdf["codigo_muni"].astype(int)

        # Handle Guatemala City zones
        gt_city_mask = gdf["codigo_muni"] == 101
        if gt_city_mask.sum() > 1:
            gt_zones = gdf[gt_city_mask].copy()
            gt_rest = gdf[~gt_city_mask].copy()

            gt_zones["zona"] = gt_zones["municipio"].apply(
                lambda n: int(m.group(1)) if (m := re.search(r"(\d+)", str(n))) else None
            )

            if zonas:
                gdf = gpd.GeoDataFrame(
                    pd.concat([gt_rest, gt_zones], ignore_index=True),
                    geometry="geometry", crs=gt_rest.crs,
                )
            else:
                dissolved_geom = (
                    gt_zones.geometry.union_all()
                    if hasattr(gt_zones.geometry, "union_all")
                    else gt_zones.geometry.unary_union
                )
                gt_row = gt_zones.iloc[[0]].copy()
                gt_row.loc[gt_row.index[0], "geometry"] = dissolved_geom
                gt_row["zona"] = None
                gdf = gpd.GeoDataFrame(
                    pd.concat([gt_rest, gt_row], ignore_index=True),
                    geometry="geometry", crs=gt_rest.crs,
                )
        else:
            gdf["zona"] = None

        # Replace with INE names
        gdf["departamento"] = gdf["codigo_depto"].map(DEPARTAMENTO_CODES).fillna(gdf["departamento"])
        cat = _get_municipio_catalogue()
        muni_names = cat.set_index("codigo_muni")["municipio"]
        gdf["municipio"] = gdf["codigo_muni"].map(muni_names).fillna(gdf["municipio"])

    # Add area in km²
    projected = gdf.to_crs(epsg=32616)
    gdf["area_km2"] = (projected.geometry.area / 1e6).round(2)

    return gdf


# =====================================================================
#  Public API
# =====================================================================

def country(resolution: str = "high") -> gpd.GeoDataFrame:
    """Download the national boundary of Guatemala.

    Uses GADM v4.1 (MINFIN does not provide a country outline).

    Parameters
    ----------
    resolution : {"high", "low"}
        ``"low"`` simplifies geometry for small-scale maps.

    Returns
    -------
    geopandas.GeoDataFrame
    """
    gdf = _download_geojson(level=0)
    gdf = _clean_gadm_columns(gdf, level=0)
    if resolution == "low":
        gdf["geometry"] = gdf.geometry.simplify(tolerance=0.01)
    return gdf


def departamentos(
    name: Optional[Union[str, int, list]] = None,
    region: Optional[str] = None,
    resolution: str = "high",
) -> gpd.GeoDataFrame:
    """Load departamento boundaries for Guatemala.

    Uses boundaries from MINFIN (Ministerio de Finanzas Públicas),
    bundled in the package with INE codes built in.

    Parameters
    ----------
    name : str, int, or list, optional
        Filter by name(s) or code(s). Accent-insensitive.
    region : str, optional
        Filter by administrative region (e.g. ``"V - Central"``).
    resolution : {"high", "low"}

    Returns
    -------
    geopandas.GeoDataFrame
        One row per departamento (22 total if unfiltered).

    Examples
    --------
    >>> gq.departamentos()
    >>> gq.departamentos("Sacatepequez")
    >>> gq.departamentos(3)
    >>> gq.departamentos(region="V - Central")
    """
    gdf = _load_minfin("departamentos")
    gdf = _clean_minfin_deptos(gdf)

    if region is not None:
        norm_region = normalize_name(region)
        matched_codes = None
        for key, deptos in REGIONES.items():
            if norm_region in normalize_name(key):
                matched_codes = [resolve_departamento(d)[0] for d in deptos]
                break
        if matched_codes is None:
            raise ValueError(
                f"Región '{region}' no encontrada. Regiones válidas:\n"
                + "\n".join(f"  {k}" for k in REGIONES)
            )
        gdf = gdf[gdf["codigo_depto"].isin(matched_codes)].copy()

    if name is not None:
        if not isinstance(name, list):
            name = [name]
        codes = [resolve_departamento(n)[0] for n in name]
        gdf = gdf[gdf["codigo_depto"].isin(codes)].copy()

    if resolution == "low":
        gdf["geometry"] = gdf.geometry.simplify(tolerance=0.005)

    return gdf.reset_index(drop=True)


def municipios(
    departamento: Optional[Union[str, int, list]] = None,
    name: Optional[Union[str, int]] = None,
    resolution: str = "high",
    zonas: bool = False,
) -> gpd.GeoDataFrame:
    """Load municipio boundaries for Guatemala.

    Uses boundaries from MINFIN (340 municipios with INE codes),
    bundled in the package.
    For Guatemala City zone-level polygons, use ``zonas=True``
    (switches to GADM v4.1 automatically).

    Parameters
    ----------
    departamento : str, int, or list, optional
        Filter by parent departamento (name or code).
    name : str or int, optional
        Filter to a specific municipio by name or code.
    resolution : {"high", "low"}
    zonas : bool, default False
        If True, keep Guatemala City's zone polygons as separate
        rows with a ``zona`` column. Automatically uses GADM v4.1
        for this (MINFIN does not have zone-level polygons).

    Returns
    -------
    geopandas.GeoDataFrame

    Examples
    --------
    >>> gq.municipios("Sacatepequez")
    >>> gq.municipios(departamento=3)
    >>> gq.municipios(name="Antigua Guatemala")
    >>> gq.municipios(name=301)
    >>>
    >>> # Guatemala City with zone-level detail
    >>> gq.municipios("Guatemala", zonas=True)
    """
    if zonas:
        # Zone polygons only available from GADM
        gdf = _download_geojson(level=2)
        gdf = _clean_gadm_columns(gdf, level=2, zonas=True)
    else:
        gdf_raw = _load_minfin("municipios")
        gdf, _lagos = _clean_minfin_munis(gdf_raw)

    if departamento is not None:
        if not isinstance(departamento, list):
            departamento = [departamento]
        codes = [resolve_departamento(d)[0] for d in departamento]
        gdf = gdf[gdf["codigo_depto"].isin(codes)].copy()

    if name is not None:
        muni_code, muni_name, depto_code, depto_name = resolve_municipio(name)
        gdf = gdf[gdf["codigo_muni"] == muni_code].copy()

    if resolution == "low":
        gdf["geometry"] = gdf.geometry.simplify(tolerance=0.003)

    return gdf.reset_index(drop=True)


def lagos() -> gpd.GeoDataFrame:
    """Load lake boundaries for Guatemala.

    Returns polygons for Lago de Atitlán and Lago de Amatitlán,
    extracted from bundled MINFIN boundary data.

    Returns
    -------
    geopandas.GeoDataFrame
        One row per lake, with columns: ``nombre``, ``geometry``,
        ``area_km2``.

    Examples
    --------
    >>> import geoquetzal as gq
    >>>
    >>> # Plot municipios with lakes overlaid
    >>> ax = gq.municipios("Sololá").plot(edgecolor="white")
    >>> gq.lagos().plot(ax=ax, color="lightblue", edgecolor="steelblue")
    >>>
    >>> # National map with lakes
    >>> ax = gq.departamentos().plot(color="lightyellow", edgecolor="gray")
    >>> gq.lagos().plot(ax=ax, color="lightblue", edgecolor="steelblue")
    """
    gdf_raw = _load_minfin("municipios")
    _munis, lake_gdf = _clean_minfin_munis(gdf_raw)
    return lake_gdf.reset_index(drop=True)


# =====================================================================
#  Diagnostic tool
# =====================================================================

def diagnose_matching(source: str = "minfin"):
    """Report matching status between boundary data and INE catalogue.

    Parameters
    ----------
    source : {"minfin", "gadm"}
        Which boundary source to diagnose.

    Returns
    -------
    dict with keys: matched, unmatched, missing_ine
    """
    cat = _get_municipio_catalogue()
    all_ine = set(cat["codigo_muni"].values)

    if source == "minfin":
        print("Cargando MINFIN municipios...")
        gdf_raw = _load_minfin("municipios")
        gdf, lake_gdf = _clean_minfin_munis(gdf_raw)

        matched_codes = set(gdf["codigo_muni"].values)
        missing_ine = all_ine - matched_codes
        extra = matched_codes - all_ine

        print(f"\n{'='*60}")
        print(f"MINFIN municipios: {len(gdf)}")
        print(f"Matched to INE:    {len(matched_codes & all_ine)}")
        print(f"Missing from INE:  {len(missing_ine)}")
        print(f"Extra (not in INE): {len(extra)}")
        print(f"{'='*60}")

        if missing_ine:
            print(f"\n--- INE municipios not in MINFIN ---")
            for code in sorted(missing_ine):
                row = cat[cat["codigo_muni"] == code].iloc[0]
                print(f"  {code:4d} | {row['departamento']:20s} | {row['municipio']}")

        if extra:
            print(f"\n--- MINFIN codes not in INE catalogue ---")
            for code in sorted(extra):
                rows = gdf[gdf["codigo_muni"] == code]
                for _, r in rows.iterrows():
                    print(f"  {code:4d} | {r.get('departamento', '?'):20s} | {r.get('municipio', '?')}")

        if not missing_ine and not extra:
            print(f"\n✓ Perfect match: all {len(all_ine)} INE municipios found in MINFIN!")

        return {
            "matched": sorted(matched_codes & all_ine),
            "missing_ine": sorted(missing_ine),
            "extra": sorted(extra),
        }

    elif source == "gadm":
        print("Descargando GADM nivel 2 (municipios)...")
        gdf = _download_geojson(level=2)

        rename = {"NAME_1": "gadm_depto", "NAME_2": "gadm_muni",
                  "GID_2": "gid_2", "VARNAME_2": "varname_2"}
        existing = {k: v for k, v in rename.items() if k in gdf.columns}
        gdf = gdf.rename(columns=existing)

        matched_codes = set()
        unmatched_gadm = []
        excluded_gadm = []
        matched_gadm = []

        for _, row in gdf.iterrows():
            gadm_muni = row["gadm_muni"]
            gadm_depto = row["gadm_depto"]
            gid = row.get("gid_2", "")

            is_gid_excluded = (
                gid in _GADM_GID_OVERRIDES and _GADM_GID_OVERRIDES[gid] is None
            )
            code = _match_muni_code(gadm_muni, gadm_depto, gid)

            if code is not None:
                matched_codes.add(code)
                ine_name = cat[cat["codigo_muni"] == code]["municipio"].iloc[0]
                matched_gadm.append({
                    "gid_2": gid, "gadm_depto": gadm_depto,
                    "gadm_muni": gadm_muni, "codigo_muni": code,
                    "ine_name": ine_name,
                })
            elif is_gid_excluded:
                excluded_gadm.append({
                    "gid_2": gid, "gadm_depto": gadm_depto,
                    "gadm_muni": gadm_muni,
                })
            else:
                unmatched_gadm.append({
                    "gid_2": gid, "gadm_depto": gadm_depto,
                    "gadm_muni": gadm_muni,
                    "varname_2": row.get("varname_2", ""),
                })

        missing_ine = all_ine - matched_codes

        print(f"\n{'='*60}")
        print(f"GADM features:   {len(gdf)}")
        print(f"Matched:         {len(matched_gadm)}")
        print(f"Excluded:        {len(excluded_gadm)}")
        print(f"Unmatched:       {len(unmatched_gadm)}")
        print(f"INE without GADM: {len(missing_ine)}")
        print(f"{'='*60}")

        if unmatched_gadm:
            print(f"\n--- Unmatched GADM features ---")
            for u in unmatched_gadm:
                print(f"  {u['gid_2']:20s} | {u['gadm_depto']:20s} | {u['gadm_muni']}")

        if missing_ine:
            print(f"\n--- INE municipios without GADM polygon ---")
            for code in sorted(missing_ine):
                row = cat[cat["codigo_muni"] == code].iloc[0]
                print(f"  {code:4d} | {row['departamento']:20s} | {row['municipio']}")

        if not unmatched_gadm and not missing_ine:
            print(f"\n✓ All GADM features matched and all INE municipios covered!")
        elif not unmatched_gadm:
            print(f"\n✓ All GADM features matched. {len(missing_ine)} INE "
                  f"municipios not in GADM v4.1.")

        return {
            "matched": matched_gadm,
            "excluded": excluded_gadm,
            "unmatched_gadm": unmatched_gadm,
            "missing_ine": sorted(missing_ine),
        }

    else:
        raise ValueError(f"source='{source}' no válido. Use 'minfin' o 'gadm'.")


if __name__ == "__main__":
    import sys
    src = "minfin"
    if "--gadm" in sys.argv:
        src = "gadm"
    diagnose_matching(source=src)
