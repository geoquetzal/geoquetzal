"""
Pre-aggregated lugar poblado indicators from INE Guatemala (XII Censo 2018).

Downloads a single Parquet file from GitHub Releases (~TBD MB).
Each row is one lugar poblado (sub-municipal locality), with pre-computed
counts and averages for demographics, ethnicity, language, education,
disability, employment, housing, and services.

There are 20,254 lugares poblados across the 22 departamentos of Guatemala.
This is the first sub-municipal census dataset available for Guatemala.

Source: INE Guatemala — XII Censo Nacional de Población y VII de Vivienda 2018
Data hosted at: https://github.com/geoquetzal/censo2018/releases/tag/lugares-poblados-v1.0

Notes
-----
- Columns ending in ``_nulo`` contain non-response counts for that variable.
  These are preserved intentionally to allow response rate estimation.
- Lugares poblados with codes ending in ``999`` (e.g. ``102999``) represent
  unnamed settlements ("Otros Lugares Poblados") with NULL coordinates.
  These are included in the data but cannot be mapped.
- Columns represent raw counts, not percentages. The appropriate denominator
  depends on the research question — use the ``_nulo`` columns to compute
  response rates and choose your own denominator.

Examples
--------
>>> import geoquetzal as gq
>>>
>>> # All lugares poblados (~20,254 rows)
>>> df = gq.lugares_poblados()
>>>
>>> # Single departamento
>>> df = gq.lugares_poblados(departamento="Sacatepéquez")
>>>
>>> # Single municipio
>>> df = gq.lugares_poblados(municipio="Antigua Guatemala")
>>>
>>> # As GeoDataFrame (point geometry from centroids)
>>> gdf = gq.lugares_poblados(geometry=True)
>>>
>>> # Map internet access
>>> gdf = gq.lugares_poblados(geometry=True)
>>> gdf["pct_internet"] = gdf["pch9_i_si"] / gdf["poblacion_total"]
>>> gdf.plot(column="pct_internet", legend=True)
"""

from pathlib import Path
from typing import Optional, Union

import pandas as pd

from geoquetzal._lookup import (
    DEPARTAMENTO_CODES,
    resolve_departamento,
    resolve_municipio,
)
from geoquetzal.cache import get_cache_dir

# ---------------------------------------------------------------------------
# GitHub Release URL
# ---------------------------------------------------------------------------

_RELEASE_BASE = (
    "https://github.com/geoquetzal/censo2018/releases/download/lugares-poblados-v1.0"
)
_FILENAME_ALL = "lugares_poblados.parquet"
_FILENAME_DEPTO = "lugares_poblados_depto_{code:02d}.parquet"


def _parquet_url(depto_code: Optional[int] = None) -> str:
    if depto_code is None:
        return f"{_RELEASE_BASE}/{_FILENAME_ALL}"
    return f"{_RELEASE_BASE}/{_FILENAME_DEPTO.format(code=depto_code)}"


# ---------------------------------------------------------------------------
# Variable metadata
# ---------------------------------------------------------------------------

VARIABLES = {
    # --- Geographic identifiers ---
    "departamento":  {"etiqueta": "Código de departamento", "tipo": "geográfica", "fuente": "geografica"},
    "municipio":     {"etiqueta": "Código de municipio", "tipo": "geográfica", "fuente": "geografica"},
    "lugar_poblado": {"etiqueta": "Código de lugar poblado (6 dígitos)", "tipo": "geográfica", "fuente": "geografica"},
    "nombre":        {"etiqueta": "Nombre del lugar poblado", "tipo": "geográfica", "fuente": "geografica"},
    "area":          {"etiqueta": "Área (del registro de vivienda)", "tipo": "geográfica",
                      "valores": {1: "Urbano", 2: "Rural"}},
    "longitud":      {"etiqueta": "Longitud del centroide", "tipo": "geográfica", "fuente": "geografica"},
    "lat":           {"etiqueta": "Latitud del centroide", "tipo": "geográfica", "fuente": "geografica"},

    # --- Scalar aggregations (persona) ---
    "poblacion_total":          {"etiqueta": "Población total", "tipo": "escalar", "fuente": "persona"},
    "pea_total":                {"etiqueta": "Población económicamente activa (conteo)", "tipo": "escalar", "fuente": "persona"},
    "pei_total":                {"etiqueta": "Población económicamente inactiva (conteo)", "tipo": "escalar", "fuente": "persona"},
    "aneduca_promedio":         {"etiqueta": "Promedio de años de estudio", "tipo": "escalar", "fuente": "persona"},
    "pcp37_promedio_mujeres":   {"etiqueta": "Edad promedio al primer nacido vivo (mujeres)", "tipo": "escalar", "fuente": "persona"},

    # --- Scalar aggregations (hogar) ---
    "pch11_promedio_cuartos":       {"etiqueta": "Promedio de cuartos por hogar", "tipo": "escalar", "fuente": "hogar"},
    "pch12_promedio_dormitorios":   {"etiqueta": "Promedio de dormitorios por hogar", "tipo": "escalar", "fuente": "hogar"},

    # --- Ethnicity (pcp12) ---
    "pcp12_maya":             {"etiqueta": "Conteo: se identifica como Maya", "tipo": "étnica", "fuente": "persona"},
    "pcp12_garifuna":         {"etiqueta": "Conteo: se identifica como Garífuna", "tipo": "étnica", "fuente": "persona"},
    "pcp12_xinka":            {"etiqueta": "Conteo: se identifica como Xinka", "tipo": "étnica", "fuente": "persona"},
    "pcp12_afrodescendiente": {"etiqueta": "Conteo: se identifica como Afrodescendiente/Creole/Afromestizo", "tipo": "étnica", "fuente": "persona"},
    "pcp12_ladino":           {"etiqueta": "Conteo: se identifica como Ladino(a)", "tipo": "étnica", "fuente": "persona"},
    "pcp12_extranjero":       {"etiqueta": "Conteo: se identifica como Extranjero(a)", "tipo": "étnica", "fuente": "persona"},
    "pcp12_nulo":             {"etiqueta": "Conteo: no respondió PCP12", "tipo": "étnica", "fuente": "persona"},

    # --- Mayan linguistic community (pcp13) — only asked to Maya ---
    "pcp13_achi":         {"etiqueta": "Conteo: comunidad Achi", "tipo": "étnica", "fuente": "persona"},
    "pcp13_akateka":      {"etiqueta": "Conteo: comunidad Akateka", "tipo": "étnica", "fuente": "persona"},
    "pcp13_awakateka":    {"etiqueta": "Conteo: comunidad Awakateka", "tipo": "étnica", "fuente": "persona"},
    "pcp13_chorti":       {"etiqueta": "Conteo: comunidad Ch'orti'", "tipo": "étnica", "fuente": "persona"},
    "pcp13_chalchiteka":  {"etiqueta": "Conteo: comunidad Chalchiteka", "tipo": "étnica", "fuente": "persona"},
    "pcp13_chuj":         {"etiqueta": "Conteo: comunidad Chuj", "tipo": "étnica", "fuente": "persona"},
    "pcp13_itza":         {"etiqueta": "Conteo: comunidad Itza'", "tipo": "étnica", "fuente": "persona"},
    "pcp13_ixil":         {"etiqueta": "Conteo: comunidad Ixil", "tipo": "étnica", "fuente": "persona"},
    "pcp13_jakalteko":    {"etiqueta": "Conteo: comunidad Jakalteko/Popti'", "tipo": "étnica", "fuente": "persona"},
    "pcp13_kiche":        {"etiqueta": "Conteo: comunidad K'iche'", "tipo": "étnica", "fuente": "persona"},
    "pcp13_kaqchiquel":   {"etiqueta": "Conteo: comunidad Kaqchiquel", "tipo": "étnica", "fuente": "persona"},
    "pcp13_mam":          {"etiqueta": "Conteo: comunidad Mam", "tipo": "étnica", "fuente": "persona"},
    "pcp13_mopan":        {"etiqueta": "Conteo: comunidad Mopan", "tipo": "étnica", "fuente": "persona"},
    "pcp13_poqomam":      {"etiqueta": "Conteo: comunidad Poqomam", "tipo": "étnica", "fuente": "persona"},
    "pcp13_poqomchi":     {"etiqueta": "Conteo: comunidad Poqomchi'", "tipo": "étnica", "fuente": "persona"},
    "pcp13_qanjobql":     {"etiqueta": "Conteo: comunidad Q'anjob'al", "tipo": "étnica", "fuente": "persona"},
    "pcp13_qeqchi":       {"etiqueta": "Conteo: comunidad Q'eqchi'", "tipo": "étnica", "fuente": "persona"},
    "pcp13_sakapulteka":  {"etiqueta": "Conteo: comunidad Sakapulteka", "tipo": "étnica", "fuente": "persona"},
    "pcp13_sipakapense":  {"etiqueta": "Conteo: comunidad Sipakapense", "tipo": "étnica", "fuente": "persona"},
    "pcp13_tektiteka":    {"etiqueta": "Conteo: comunidad Tektiteka", "tipo": "étnica", "fuente": "persona"},
    "pcp13_tzutujil":     {"etiqueta": "Conteo: comunidad Tz'utujil", "tipo": "étnica", "fuente": "persona"},
    "pcp13_uspanteka":    {"etiqueta": "Conteo: comunidad Uspanteka", "tipo": "étnica", "fuente": "persona"},
    "pcp13_nulo":         {"etiqueta": "Conteo: no respondió PCP13 (incluye no Maya)", "tipo": "étnica", "fuente": "persona"},

    # --- Traditional clothing (pcp14) ---
    "pcp14_si":   {"etiqueta": "Conteo: usa traje maya/garífuna/xinka regularmente", "tipo": "étnica", "fuente": "persona"},
    "pcp14_no":   {"etiqueta": "Conteo: no usa traje", "tipo": "étnica", "fuente": "persona"},
    "pcp14_nulo": {"etiqueta": "Conteo: no respondió PCP14", "tipo": "étnica", "fuente": "persona"},

    # --- Mother tongue (pcp15) ---
    "pcp15_espanol":    {"etiqueta": "Conteo: idioma materno Español", "tipo": "idioma", "fuente": "persona"},
    "pcp15_kiche":      {"etiqueta": "Conteo: idioma materno K'iche'", "tipo": "idioma", "fuente": "persona"},
    "pcp15_kaqchiquel": {"etiqueta": "Conteo: idioma materno Kaqchiquel", "tipo": "idioma", "fuente": "persona"},
    "pcp15_mam":        {"etiqueta": "Conteo: idioma materno Mam", "tipo": "idioma", "fuente": "persona"},
    "pcp15_qeqchi":     {"etiqueta": "Conteo: idioma materno Q'eqchi'", "tipo": "idioma", "fuente": "persona"},
    "pcp15_achi":       {"etiqueta": "Conteo: idioma materno Achí", "tipo": "idioma", "fuente": "persona"},
    "pcp15_akateko":    {"etiqueta": "Conteo: idioma materno Akateko", "tipo": "idioma", "fuente": "persona"},
    "pcp15_awakateko":  {"etiqueta": "Conteo: idioma materno Awakateko", "tipo": "idioma", "fuente": "persona"},
    "pcp15_chorti":     {"etiqueta": "Conteo: idioma materno Ch'orti'", "tipo": "idioma", "fuente": "persona"},
    "pcp15_chalchiteko":{"etiqueta": "Conteo: idioma materno Chalchiteko", "tipo": "idioma", "fuente": "persona"},
    "pcp15_chuj":       {"etiqueta": "Conteo: idioma materno Chuj", "tipo": "idioma", "fuente": "persona"},
    "pcp15_itza":       {"etiqueta": "Conteo: idioma materno Itza'", "tipo": "idioma", "fuente": "persona"},
    "pcp15_ixil":       {"etiqueta": "Conteo: idioma materno Ixil", "tipo": "idioma", "fuente": "persona"},
    "pcp15_jakalteko":  {"etiqueta": "Conteo: idioma materno Jakalteko/Popti'", "tipo": "idioma", "fuente": "persona"},
    "pcp15_mopan":      {"etiqueta": "Conteo: idioma materno Mopan", "tipo": "idioma", "fuente": "persona"},
    "pcp15_poqomam":    {"etiqueta": "Conteo: idioma materno Poqomam", "tipo": "idioma", "fuente": "persona"},
    "pcp15_poqomchi":   {"etiqueta": "Conteo: idioma materno Poqomchi'", "tipo": "idioma", "fuente": "persona"},
    "pcp15_qanjobql":   {"etiqueta": "Conteo: idioma materno Q'anjob'al", "tipo": "idioma", "fuente": "persona"},
    "pcp15_sakapulteko":{"etiqueta": "Conteo: idioma materno Sakapulteko", "tipo": "idioma", "fuente": "persona"},
    "pcp15_sipakapense":{"etiqueta": "Conteo: idioma materno Sipakapense", "tipo": "idioma", "fuente": "persona"},
    "pcp15_tektiteko":  {"etiqueta": "Conteo: idioma materno Tektiteko", "tipo": "idioma", "fuente": "persona"},
    "pcp15_tzutujil":   {"etiqueta": "Conteo: idioma materno Tz'utujil", "tipo": "idioma", "fuente": "persona"},
    "pcp15_uspanteko":  {"etiqueta": "Conteo: idioma materno Uspanteko", "tipo": "idioma", "fuente": "persona"},
    "pcp15_xinka":      {"etiqueta": "Conteo: idioma materno Xinka", "tipo": "idioma", "fuente": "persona"},
    "pcp15_garifuna":   {"etiqueta": "Conteo: idioma materno Garífuna", "tipo": "idioma", "fuente": "persona"},
    "pcp15_ingles":     {"etiqueta": "Conteo: idioma materno Inglés", "tipo": "idioma", "fuente": "persona"},
    "pcp15_senas":      {"etiqueta": "Conteo: idioma materno Señas", "tipo": "idioma", "fuente": "persona"},
    "pcp15_otro_idioma":{"etiqueta": "Conteo: idioma materno Otro", "tipo": "idioma", "fuente": "persona"},
    "pcp15_no_habla":   {"etiqueta": "Conteo: no habla", "tipo": "idioma", "fuente": "persona"},
    "pcp15_nulo":       {"etiqueta": "Conteo: no respondió PCP15", "tipo": "idioma", "fuente": "persona"},

    # --- Disability (pcp16_a through pcp16_f) ---
    "pcp16_a_sin_dificultad":  {"etiqueta": "Ver: sin dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_a_algo_dificultad": {"etiqueta": "Ver: con algo de dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_a_mucha_dificultad":{"etiqueta": "Ver: con mucha dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_a_no_puede":        {"etiqueta": "Ver: no puede", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_a_nulo":            {"etiqueta": "Ver: no respondió", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_b_sin_dificultad":  {"etiqueta": "Oír: sin dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_b_algo_dificultad": {"etiqueta": "Oír: con algo de dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_b_mucha_dificultad":{"etiqueta": "Oír: con mucha dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_b_no_puede":        {"etiqueta": "Oír: no puede", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_b_nulo":            {"etiqueta": "Oír: no respondió", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_c_sin_dificultad":  {"etiqueta": "Caminar: sin dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_c_algo_dificultad": {"etiqueta": "Caminar: con algo de dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_c_mucha_dificultad":{"etiqueta": "Caminar: con mucha dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_c_no_puede":        {"etiqueta": "Caminar: no puede", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_c_nulo":            {"etiqueta": "Caminar: no respondió", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_d_sin_dificultad":  {"etiqueta": "Recordar/concentrarse: sin dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_d_algo_dificultad": {"etiqueta": "Recordar/concentrarse: con algo de dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_d_mucha_dificultad":{"etiqueta": "Recordar/concentrarse: con mucha dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_d_no_puede":        {"etiqueta": "Recordar/concentrarse: no puede", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_d_nulo":            {"etiqueta": "Recordar/concentrarse: no respondió", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_e_sin_dificultad":  {"etiqueta": "Cuidado personal: sin dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_e_algo_dificultad": {"etiqueta": "Cuidado personal: con algo de dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_e_mucha_dificultad":{"etiqueta": "Cuidado personal: con mucha dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_e_no_puede":        {"etiqueta": "Cuidado personal: no puede", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_e_nulo":            {"etiqueta": "Cuidado personal: no respondió", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_f_sin_dificultad":  {"etiqueta": "Comunicarse: sin dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_f_algo_dificultad": {"etiqueta": "Comunicarse: con algo de dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_f_mucha_dificultad":{"etiqueta": "Comunicarse: con mucha dificultad", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_f_no_puede":        {"etiqueta": "Comunicarse: no puede", "tipo": "discapacidad", "fuente": "persona"},
    "pcp16_f_nulo":            {"etiqueta": "Comunicarse: no respondió", "tipo": "discapacidad", "fuente": "persona"},

    # --- Education level (pcp17_a) ---
    "pcp17_a_ninguno":      {"etiqueta": "Conteo: ningún nivel educativo", "tipo": "educación", "fuente": "persona"},
    "pcp17_a_preprimaria":  {"etiqueta": "Conteo: preprimaria", "tipo": "educación", "fuente": "persona"},
    "pcp17_a_primaria":     {"etiqueta": "Conteo: primaria", "tipo": "educación", "fuente": "persona"},
    "pcp17_a_nivel_medio":  {"etiqueta": "Conteo: nivel medio (básico y diversificado)", "tipo": "educación", "fuente": "persona"},
    "pcp17_a_licenciatura": {"etiqueta": "Conteo: licenciatura", "tipo": "educación", "fuente": "persona"},
    "pcp17_a_maestria":     {"etiqueta": "Conteo: maestría", "tipo": "educación", "fuente": "persona"},
    "pcp17_a_doctorado":    {"etiqueta": "Conteo: doctorado", "tipo": "educación", "fuente": "persona"},
    "pcp17_a_nulo":         {"etiqueta": "Conteo: no respondió PCP17_A", "tipo": "educación", "fuente": "persona"},

    # --- Literacy (pcp22) ---
    "pcp22_alfabeto":    {"etiqueta": "Conteo: sabe leer y escribir", "tipo": "educación", "fuente": "persona"},
    "pcp22_no_alfabeto": {"etiqueta": "Conteo: no sabe leer y escribir", "tipo": "educación", "fuente": "persona"},
    "pcp22_nulo":        {"etiqueta": "Conteo: no respondió PCP22", "tipo": "educación", "fuente": "persona"},

    # --- Speaks another language (pcp24) ---
    "pcp24_habla_otro_idioma":    {"etiqueta": "Conteo: sabe hablar otro idioma", "tipo": "idioma", "fuente": "persona"},
    "pcp24_no_habla_otro_idioma": {"etiqueta": "Conteo: no sabe hablar otro idioma", "tipo": "idioma", "fuente": "persona"},
    "pcp24_nulo":                 {"etiqueta": "Conteo: no respondió PCP24", "tipo": "idioma", "fuente": "persona"},

    # --- Occupation 1-digit (pcp30_1d) ---
    "pcp30_1d_militar":              {"etiqueta": "Conteo: ocupaciones militares", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_directores_gerentes":  {"etiqueta": "Conteo: directores y gerentes", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_profesionales":        {"etiqueta": "Conteo: profesionales científicos e intelectuales", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_tecnicos":             {"etiqueta": "Conteo: técnicos de nivel medio", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_apoyo_administrativo": {"etiqueta": "Conteo: personal de apoyo administrativo", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_servicios_ventas":     {"etiqueta": "Conteo: trabajadores de servicios y vendedores", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_agropecuarios":        {"etiqueta": "Conteo: agricultores y trabajadores agropecuarios", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_oficiales_operarios":  {"etiqueta": "Conteo: oficiales, operarios y artesanos", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_operadores_maquinas":  {"etiqueta": "Conteo: operadores de instalaciones y máquinas", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_elementales":          {"etiqueta": "Conteo: ocupaciones elementales", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_no_especificado":      {"etiqueta": "Conteo: ocupación no especificada", "tipo": "empleo", "fuente": "persona"},
    "pcp30_1d_nulo":                 {"etiqueta": "Conteo: no respondió PCP30_1D", "tipo": "empleo", "fuente": "persona"},

    # --- Occupational category (pcp31_d) ---
    "pcp31_d_patrono":                  {"etiqueta": "Conteo: patrono(a) o empleador(a)", "tipo": "empleo", "fuente": "persona"},
    "pcp31_d_cuenta_propia_con_local":  {"etiqueta": "Conteo: cuenta propia con local", "tipo": "empleo", "fuente": "persona"},
    "pcp31_d_cuenta_propia_sin_local":  {"etiqueta": "Conteo: cuenta propia sin local", "tipo": "empleo", "fuente": "persona"},
    "pcp31_d_empleado_publico":         {"etiqueta": "Conteo: empleado(a) público(a)", "tipo": "empleo", "fuente": "persona"},
    "pcp31_d_empleado_privado":         {"etiqueta": "Conteo: empleado(a) privado(a)", "tipo": "empleo", "fuente": "persona"},
    "pcp31_d_empleado_domestico":       {"etiqueta": "Conteo: empleado(a) doméstico(a)", "tipo": "empleo", "fuente": "persona"},
    "pcp31_d_familiar_no_remunerado":   {"etiqueta": "Conteo: familiar no remunerado", "tipo": "empleo", "fuente": "persona"},
    "pcp31_d_no_declarado":             {"etiqueta": "Conteo: no declarado PCP31_D", "tipo": "empleo", "fuente": "persona"},
    "pcp31_d_nulo":                     {"etiqueta": "Conteo: no respondió PCP31_D", "tipo": "empleo", "fuente": "persona"},

    # --- Housing type (pcv1) ---
    "pcv1_casa_formal":             {"etiqueta": "Conteo: casa formal", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv1_apartamento":             {"etiqueta": "Conteo: apartamento", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv1_cuarto_vecindad":         {"etiqueta": "Conteo: cuarto de casa de vecindad", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv1_rancho":                  {"etiqueta": "Conteo: rancho", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv1_vivienda_improvisada":    {"etiqueta": "Conteo: vivienda improvisada", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv1_otro_particular":         {"etiqueta": "Conteo: otro tipo particular", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv1_vivienda_colectiva":      {"etiqueta": "Conteo: vivienda colectiva", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv1_nulo":                    {"etiqueta": "Conteo: no especificado PCV1", "tipo": "vivienda", "fuente": "vivienda"},

    # --- Wall material (pcv2) ---
    "pcv2_ladrillo":         {"etiqueta": "Conteo: paredes de ladrillo", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv2_block":            {"etiqueta": "Conteo: paredes de block", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv2_concreto":         {"etiqueta": "Conteo: paredes de concreto", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv2_adobe":            {"etiqueta": "Conteo: paredes de adobe", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv2_madera":           {"etiqueta": "Conteo: paredes de madera", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv2_lamina_metalica":  {"etiqueta": "Conteo: paredes de lámina metálica", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv2_bajareque":        {"etiqueta": "Conteo: paredes de bajareque", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv2_lepa_palo_cana":   {"etiqueta": "Conteo: paredes de lepa, palo o caña", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv2_material_desecho": {"etiqueta": "Conteo: paredes de material de desecho", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv2_otro":             {"etiqueta": "Conteo: paredes de otro material", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv2_nulo":             {"etiqueta": "Conteo: no especificado PCV2", "tipo": "vivienda", "fuente": "vivienda"},

    # --- Roof material (pcv3) ---
    "pcv3_concreto":         {"etiqueta": "Conteo: techo de concreto", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv3_lamina_metalica":  {"etiqueta": "Conteo: techo de lámina metálica", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv3_asbesto_cemento":  {"etiqueta": "Conteo: techo de asbesto cemento", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv3_teja":             {"etiqueta": "Conteo: techo de teja", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv3_paja_palma":       {"etiqueta": "Conteo: techo de paja, palma o similar", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv3_material_desecho": {"etiqueta": "Conteo: techo de material de desecho", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv3_otro":             {"etiqueta": "Conteo: techo de otro material", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv3_nulo":             {"etiqueta": "Conteo: no especificado PCV3", "tipo": "vivienda", "fuente": "vivienda"},

    # --- Floor material (pcv5) ---
    "pcv5_ladrillo_ceramico": {"etiqueta": "Conteo: piso de ladrillo cerámico", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv5_ladrillo_cemento":  {"etiqueta": "Conteo: piso de ladrillo de cemento", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv5_ladrillo_barro":    {"etiqueta": "Conteo: piso de ladrillo de barro", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv5_torta_cemento":     {"etiqueta": "Conteo: piso de torta de cemento", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv5_parque_vinil":      {"etiqueta": "Conteo: piso de parqué/vinil", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv5_madera":            {"etiqueta": "Conteo: piso de madera", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv5_tierra":            {"etiqueta": "Conteo: piso de tierra", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv5_otro":              {"etiqueta": "Conteo: piso de otro material", "tipo": "vivienda", "fuente": "vivienda"},
    "pcv5_nulo":              {"etiqueta": "Conteo: no especificado PCV5", "tipo": "vivienda", "fuente": "vivienda"},

    # --- Water source (pch4) ---
    "pch4_tuberia_dentro":        {"etiqueta": "Conteo: tubería dentro de la vivienda", "tipo": "servicios", "fuente": "hogar"},
    "pch4_tuberia_fuera_terreno": {"etiqueta": "Conteo: tubería fuera de la vivienda, en el terreno", "tipo": "servicios", "fuente": "hogar"},
    "pch4_chorro_publico":        {"etiqueta": "Conteo: chorro público", "tipo": "servicios", "fuente": "hogar"},
    "pch4_pozo_perforado":        {"etiqueta": "Conteo: pozo perforado", "tipo": "servicios", "fuente": "hogar"},
    "pch4_agua_lluvia":           {"etiqueta": "Conteo: agua de lluvia", "tipo": "servicios", "fuente": "hogar"},
    "pch4_rio":                   {"etiqueta": "Conteo: río", "tipo": "servicios", "fuente": "hogar"},
    "pch4_lago":                  {"etiqueta": "Conteo: lago", "tipo": "servicios", "fuente": "hogar"},
    "pch4_manantial":             {"etiqueta": "Conteo: manantial o nacimiento", "tipo": "servicios", "fuente": "hogar"},
    "pch4_camion_tonel":          {"etiqueta": "Conteo: camión o tonel", "tipo": "servicios", "fuente": "hogar"},
    "pch4_otro":                  {"etiqueta": "Conteo: otra fuente de agua", "tipo": "servicios", "fuente": "hogar"},
    "pch4_nulo":                  {"etiqueta": "Conteo: no respondió PCH4", "tipo": "servicios", "fuente": "hogar"},

    # --- Sanitation (pch5) ---
    "pch5_inodoro_red_drenajes":  {"etiqueta": "Conteo: inodoro conectado a red de drenajes", "tipo": "servicios", "fuente": "hogar"},
    "pch5_inodoro_fosa_septica":  {"etiqueta": "Conteo: inodoro conectado a fosa séptica", "tipo": "servicios", "fuente": "hogar"},
    "pch5_excusado_lavable":      {"etiqueta": "Conteo: excusado lavable", "tipo": "servicios", "fuente": "hogar"},
    "pch5_letrina_pozo":          {"etiqueta": "Conteo: letrina o pozo ciego", "tipo": "servicios", "fuente": "hogar"},
    "pch5_no_tiene":              {"etiqueta": "Conteo: no tiene servicio sanitario", "tipo": "servicios", "fuente": "hogar"},
    "pch5_nulo":                  {"etiqueta": "Conteo: no respondió PCH5", "tipo": "servicios", "fuente": "hogar"},

    # --- Electricity (pch8) ---
    "pch8_red_electrica":       {"etiqueta": "Conteo: red de energía eléctrica", "tipo": "servicios", "fuente": "hogar"},
    "pch8_panel_solar_eolico":  {"etiqueta": "Conteo: panel solar o eólico", "tipo": "servicios", "fuente": "hogar"},
    "pch8_gas_corriente":       {"etiqueta": "Conteo: gas corriente", "tipo": "servicios", "fuente": "hogar"},
    "pch8_candela":             {"etiqueta": "Conteo: candela", "tipo": "servicios", "fuente": "hogar"},
    "pch8_otro":                {"etiqueta": "Conteo: otro tipo de alumbrado", "tipo": "servicios", "fuente": "hogar"},
    "pch8_nulo":                {"etiqueta": "Conteo: no respondió PCH8", "tipo": "servicios", "fuente": "hogar"},

    # --- Household appliances (pch9_a through pch9_m) ---
    "pch9_a_si": {"etiqueta": "Conteo: tiene radio", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_a_no": {"etiqueta": "Conteo: no tiene radio", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_b_si": {"etiqueta": "Conteo: tiene estufa", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_b_no": {"etiqueta": "Conteo: no tiene estufa", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_c_si": {"etiqueta": "Conteo: tiene televisor", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_c_no": {"etiqueta": "Conteo: no tiene televisor", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_d_si": {"etiqueta": "Conteo: tiene cable", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_d_no": {"etiqueta": "Conteo: no tiene cable", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_e_si": {"etiqueta": "Conteo: tiene refrigeradora", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_e_no": {"etiqueta": "Conteo: no tiene refrigeradora", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_f_si": {"etiqueta": "Conteo: tiene tanque de agua", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_f_no": {"etiqueta": "Conteo: no tiene tanque de agua", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_g_si": {"etiqueta": "Conteo: tiene lavadora", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_g_no": {"etiqueta": "Conteo: no tiene lavadora", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_h_si": {"etiqueta": "Conteo: tiene computadora", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_h_no": {"etiqueta": "Conteo: no tiene computadora", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_i_si": {"etiqueta": "Conteo: tiene internet", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_i_no": {"etiqueta": "Conteo: no tiene internet", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_j_si": {"etiqueta": "Conteo: tiene temazcal o tuj", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_j_no": {"etiqueta": "Conteo: no tiene temazcal o tuj", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_k_si": {"etiqueta": "Conteo: tiene agua caliente", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_k_no": {"etiqueta": "Conteo: no tiene agua caliente", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_l_si": {"etiqueta": "Conteo: tiene moto", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_l_no": {"etiqueta": "Conteo: no tiene moto", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_m_si": {"etiqueta": "Conteo: tiene carro", "tipo": "equipamiento", "fuente": "hogar"},
    "pch9_m_no": {"etiqueta": "Conteo: no tiene carro", "tipo": "equipamiento", "fuente": "hogar"},

    # --- Cooking fuel (pch14) ---
    "pch14_gas_propano":   {"etiqueta": "Conteo: cocina con gas propano", "tipo": "servicios", "fuente": "hogar"},
    "pch14_lena":          {"etiqueta": "Conteo: cocina con leña", "tipo": "servicios", "fuente": "hogar"},
    "pch14_electricidad":  {"etiqueta": "Conteo: cocina con electricidad", "tipo": "servicios", "fuente": "hogar"},
    "pch14_carbon":        {"etiqueta": "Conteo: cocina con carbón", "tipo": "servicios", "fuente": "hogar"},
    "pch14_gas_corriente": {"etiqueta": "Conteo: cocina con gas corriente", "tipo": "servicios", "fuente": "hogar"},
    "pch14_otra_fuente":   {"etiqueta": "Conteo: cocina con otra fuente", "tipo": "servicios", "fuente": "hogar"},
    "pch14_no_cocina":     {"etiqueta": "Conteo: no cocina", "tipo": "servicios", "fuente": "hogar"},
    "pch14_nulo":          {"etiqueta": "Conteo: no respondió PCH14", "tipo": "servicios", "fuente": "hogar"},

    # --- Remittances (pch15) ---
    "pch15_si":   {"etiqueta": "Conteo: recibe remesas", "tipo": "económica", "fuente": "hogar"},
    "pch15_no":   {"etiqueta": "Conteo: no recibe remesas", "tipo": "económica", "fuente": "hogar"},
    "pch15_nulo": {"etiqueta": "Conteo: no respondió PCH15", "tipo": "económica", "fuente": "hogar"},

    # --- Emigration since 2002 (pei1) ---
    "pei1_si":   {"etiqueta": "Conteo: hogar con emigrante desde 2002", "tipo": "migración", "fuente": "hogar"},
    "pei1_no":   {"etiqueta": "Conteo: hogar sin emigrante desde 2002", "tipo": "migración", "fuente": "hogar"},
    "pei1_nulo": {"etiqueta": "Conteo: no respondió PEI1", "tipo": "migración", "fuente": "hogar"},
}


# ---------------------------------------------------------------------------
# Download helper
# ---------------------------------------------------------------------------

def _download_parquet(depto_code: Optional[int] = None) -> Path:
    """Download a lugares_poblados parquet file, with caching.

    Parameters
    ----------
    depto_code : int or None
        If None, downloads the full file (all 22 departamentos).
        If an int, downloads only that departamento's partitioned file.
    """
    import requests

    cache_dir = get_cache_dir()

    if depto_code is None:
        filename = _FILENAME_ALL
        label = "lugares poblados (todos los departamentos)"
    else:
        filename = _FILENAME_DEPTO.format(code=depto_code)
        label = f"lugares poblados depto {depto_code:02d} ({DEPARTAMENTO_CODES[depto_code]})"

    cached = cache_dir / filename
    if cached.exists():
        return cached

    url = _parquet_url(depto_code)
    print(f"⬇  Descargando {label}...")

    try:
        resp = requests.get(url, timeout=300, stream=True)
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

def lugares_poblados(
    departamento: Optional[Union[str, int]] = None,
    municipio: Optional[Union[str, int]] = None,
    lugar_poblado: Optional[int] = None,
    geometry: bool = False,
) -> Union[pd.DataFrame, "gpd.GeoDataFrame"]:
    """Load pre-aggregated lugar poblado indicators from INE Censo 2018.

    Each row is one lugar poblado (sub-municipal locality). Downloads a
    single Parquet file from GitHub Releases on first use, then caches it.

    There are 20,254 lugares poblados across Guatemala. Columns include
    pre-computed counts for demographics, ethnicity, language, education,
    disability, employment, housing, and household services.

    Parameters
    ----------
    departamento : str or int, optional
        Filter by departamento name or code.
    municipio : str or int, optional
        Filter by municipio name or code.
    geometry : bool, optional
        If True, returns a GeoDataFrame with point geometry from centroids.
        Lugares poblados with NULL coordinates are excluded when geometry=True.

    Returns
    -------
    pandas.DataFrame or geopandas.GeoDataFrame

    Notes
    -----
    Columns represent raw counts, not percentages. Compute percentages
    using the denominator appropriate for your research question.

    Columns ending in ``_nulo`` represent non-responses and can be used
    for response rate estimation.

    Examples
    --------
    >>> import geoquetzal as gq
    >>>
    >>> # All lugares poblados
    >>> df = gq.lugares_poblados()
    >>>
    >>> # Single departamento
    >>> df = gq.lugares_poblados(departamento="Sacatepéquez")
    >>>
    >>> # Single municipio
    >>> df = gq.lugares_poblados(municipio="Antigua Guatemala")
    >>>
    >>> # As GeoDataFrame (point geometry)
    >>> gdf = gq.lugares_poblados(geometry=True)
    >>>
    >>> # Map internet access rate
    >>> gdf = gq.lugares_poblados(departamento="Sacatepéquez", geometry=True)
    >>> gdf["pct_internet"] = gdf["pch9_i_si"] / gdf["poblacion_total"]
    >>> gdf.plot(column="pct_internet", legend=True, markersize=5)
    """
    # Resolve codes before downloading so we can fetch only what's needed
    depto_code = None
    muni_code = None

    if municipio is not None:
        muni_code, muni_name, depto_code, depto_name = resolve_municipio(municipio)
    elif departamento is not None:
        depto_code, depto_name = resolve_departamento(departamento)

    # Download: single-departamento file when possible, full file otherwise
    path = _download_parquet(depto_code)
    df = pd.read_parquet(path)

    # Filter by municipio if specified (departamento already narrowed by file)
    if muni_code is not None:
        df = df[df["municipio"] == muni_code].copy()

    if lugar_poblado is not None:
        df = df[df["lugar_poblado"] == int(lugar_poblado)].copy()

    # Add geometry (point from centroids)
    if geometry:
        import geopandas as gpd
        from shapely.geometry import Point

        # Exclude lugares with NULL coordinates
        df_geo = df.dropna(subset=["longitud", "lat"]).copy()
        n_excluded = len(df) - len(df_geo)
        if n_excluded > 0:
            print(
                f"   ℹ {n_excluded} lugares poblados excluidos por coordenadas nulas "
                f"(códigos terminados en 999 — asentamientos sin nombre oficial)."
            )
        df_geo["geometry"] = df_geo.apply(
            lambda r: Point(r["longitud"], r["lat"]), axis=1
        )
        return gpd.GeoDataFrame(df_geo, geometry="geometry", crs="EPSG:4326").reset_index(drop=True)

    return df.reset_index(drop=True)


def describe(variable: Optional[str] = None) -> Union[pd.DataFrame, dict]:
    """Describe the columns in the lugares_poblados dataset.

    Parameters
    ----------
    variable : str, optional
        Specific column name (e.g. ``\"pcp12_maya\"``). If ``None``, lists all.

    Returns
    -------
    pandas.DataFrame or dict

    Examples
    --------
    >>> import geoquetzal as gq
    >>> gq.describe_lugares_poblados()             # all columns
    >>> gq.describe_lugares_poblados("pcp12_maya") # ethnicity Maya count
    """
    if variable is not None:
        var_lower = variable.lower()
        if var_lower not in VARIABLES:
            raise ValueError(
                f"Variable '{variable}' no encontrada.\n"
                f"Variables disponibles: {', '.join(VARIABLES.keys())}"
            )
        return {"variable": var_lower, **VARIABLES[var_lower]}

    rows = []
    for name, info in VARIABLES.items():
        rows.append({
            "variable": name,
            "etiqueta": info.get("etiqueta", ""),
            "tipo": info.get("tipo", ""),
        })
    return pd.DataFrame(rows)
