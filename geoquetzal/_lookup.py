"""
Shared lookup tables and name-resolution helpers.

Provides accent-insensitive, fuzzy matching for departamento and
municipio names and codes. Used across all GeoQuetzal modules.
"""

import unicodedata
from pathlib import Path
from typing import Optional, Union

import pandas as pd

# ---------------------------------------------------------------------------
# Departamento codes (INE standard)
# ---------------------------------------------------------------------------

DEPARTAMENTO_CODES = {
    1: "Guatemala",
    2: "El Progreso",
    3: "Sacatepéquez",
    4: "Chimaltenango",
    5: "Escuintla",
    6: "Santa Rosa",
    7: "Sololá",
    8: "Totonicapán",
    9: "Quetzaltenango",
    10: "Suchitepéquez",
    11: "Retalhuleu",
    12: "San Marcos",
    13: "Huehuetenango",
    14: "Quiché",
    15: "Baja Verapaz",
    16: "Alta Verapaz",
    17: "Petén",
    18: "Izabal",
    19: "Zacapa",
    20: "Chiquimula",
    21: "Jalapa",
    22: "Jutiapa",
}

DEPARTAMENTO_NAMES = {v: k for k, v in DEPARTAMENTO_CODES.items()}

# Administrative regions
REGIONES = {
    "I - Metropolitana": ["Guatemala"],
    "II - Norte": ["Alta Verapaz", "Baja Verapaz"],
    "III - Nororiente": ["Chiquimula", "El Progreso", "Izabal", "Zacapa"],
    "IV - Suroriente": ["Jalapa", "Jutiapa", "Santa Rosa"],
    "V - Central": ["Chimaltenango", "Escuintla", "Sacatepéquez"],
    "VI - Suroccidente": [
        "Quetzaltenango", "Retalhuleu", "San Marcos",
        "Sololá", "Suchitepéquez", "Totonicapán",
    ],
    "VII - Noroccidente": ["Huehuetenango", "Quiché"],
    "VIII - Petén": ["Petén"],
}


# ---------------------------------------------------------------------------
# Municipio catalogue (loaded lazily from bundled CSV)
# ---------------------------------------------------------------------------

_municipio_df: Optional[pd.DataFrame] = None


def _get_municipio_catalogue() -> pd.DataFrame:
    """Load the municipio catalogue (340 rows) from bundled CSV."""
    global _municipio_df
    if _municipio_df is None:
        path = Path(__file__).parent / "data" / "catalogo_municipios.csv"
        _municipio_df = pd.read_csv(path)
    return _municipio_df


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    """Normalise a name for fuzzy matching (lowercase, strip accents)."""
    name = name.strip().lower()
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return name


# ---------------------------------------------------------------------------
# Departamento resolution
# ---------------------------------------------------------------------------

def resolve_departamento(value: Union[str, int]) -> tuple:
    """Resolve a departamento input to (code: int, name: str).

    Accepts:
        - int code: 1–22
        - str code: "1", "01", "13"
        - str name: "Huehuetenango", "huehuetenango", "Sacatepequez"

    Returns
    -------
    tuple of (int, str)
        (departamento_code, canonical_name)

    Raises
    ------
    ValueError
        If the input cannot be matched.
    """
    # Try numeric code
    if isinstance(value, int):
        if value in DEPARTAMENTO_CODES:
            return value, DEPARTAMENTO_CODES[value]
        raise ValueError(f"Código de departamento {value} no válido (1–22).")

    # Try string as numeric
    try:
        code = int(value)
        if code in DEPARTAMENTO_CODES:
            return code, DEPARTAMENTO_CODES[code]
    except ValueError:
        pass

    # Try exact name match (normalized)
    norm = normalize_name(value)
    for code, canonical in DEPARTAMENTO_CODES.items():
        if normalize_name(canonical) == norm:
            return code, canonical

    # Try partial match
    for code, canonical in DEPARTAMENTO_CODES.items():
        if norm in normalize_name(canonical) or normalize_name(canonical) in norm:
            return code, canonical

    valid = "\n".join(f"  {c:2d}: {n}" for c, n in DEPARTAMENTO_CODES.items())
    raise ValueError(
        f"Departamento '{value}' no encontrado.\n"
        f"Departamentos válidos:\n{valid}"
    )


# ---------------------------------------------------------------------------
# Municipio resolution
# ---------------------------------------------------------------------------

def resolve_municipio(value: Union[str, int]) -> tuple:
    """Resolve a municipio input to (code: int, name: str, depto_code: int, depto_name: str).

    Accepts:
        - int code: e.g. 301 (Antigua Guatemala)
        - str code: "301", "1301"
        - str name: "Antigua Guatemala", "antigua guatemala"

    Returns
    -------
    tuple of (int, str, int, str)
        (municipio_code, municipio_name, depto_code, depto_name)

    Raises
    ------
    ValueError
        If the input cannot be matched.
    """
    cat = _get_municipio_catalogue()

    # Try numeric code
    if isinstance(value, int):
        row = cat[cat["codigo_muni"] == value]
        if len(row) == 1:
            r = row.iloc[0]
            return int(r["codigo_muni"]), r["municipio"], int(r["codigo_depto"]), r["departamento"]
        raise ValueError(f"Código de municipio {value} no encontrado.")

    # Try string as numeric
    try:
        code = int(value)
        row = cat[cat["codigo_muni"] == code]
        if len(row) == 1:
            r = row.iloc[0]
            return int(r["codigo_muni"]), r["municipio"], int(r["codigo_depto"]), r["departamento"]
    except ValueError:
        pass

    # Try name match (normalized)
    norm = normalize_name(value)
    cat_norm = cat["municipio"].apply(normalize_name)

    # Exact
    mask = cat_norm == norm
    if mask.sum() == 1:
        r = cat[mask].iloc[0]
        return int(r["codigo_muni"]), r["municipio"], int(r["codigo_depto"]), r["departamento"]

    # Partial
    mask = cat_norm.str.contains(norm, na=False)
    if mask.sum() == 1:
        r = cat[mask].iloc[0]
        return int(r["codigo_muni"]), r["municipio"], int(r["codigo_depto"]), r["departamento"]
    elif mask.sum() > 1:
        matches = cat[mask][["codigo_muni", "municipio", "departamento"]].to_string(index=False)
        raise ValueError(
            f"Municipio '{value}' es ambiguo. Coincidencias:\n{matches}\n"
            f"Use el código de municipio para ser más específico."
        )

    raise ValueError(
        f"Municipio '{value}' no encontrado. "
        f"Use geoquetzal.municipios() para ver todos los municipios disponibles."
    )
