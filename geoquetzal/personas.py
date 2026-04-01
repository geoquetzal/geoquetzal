"""
Person-level microdata from INE Guatemala (XII Censo 2018).

Downloads partitioned Parquet files from GitHub Releases. Each
departamento is a separate file (~3–70 MB). Total: 14,901,286 persons.

Source: INE Guatemala — XII Censo Nacional de Población y VII de Vivienda 2018
Data hosted at: https://github.com/geoquetzal/censo2018/releases/tag/persona-v1.0

Variables cover demographics, ethnicity, language, disability, education,
literacy, technology use, employment, occupation, fertility, and migration.

Examples
--------
>>> import geoquetzal as gq
>>>
>>> # Single departamento
>>> df = gq.personas(departamento="Huehuetenango")
>>>
>>> # All of Guatemala (~333 MB, 14.9M rows)
>>> df = gq.personas()
>>>
>>> # With geometry
>>> gdf = gq.personas(departamento="Sacatepéquez", geometry="municipio")
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
    "https://github.com/geoquetzal/censo2018/releases/download/persona-v1.0"
)


def _parquet_url(depto_code: int) -> str:
    return f"{_RELEASE_BASE}/persona_depto_{depto_code:02d}.parquet"


# ---------------------------------------------------------------------------
# Variable metadata
# ---------------------------------------------------------------------------

VARIABLES = {
    # --- Geographic ---
    "DEPARTAMENTO": {"etiqueta": "Departamento", "tipo": "geográfica"},
    "MUNICIPIO": {"etiqueta": "Municipio", "tipo": "geográfica"},
    "COD_MUNICIPIO": {"etiqueta": "Código de municipio dentro del departamento", "tipo": "geográfica"},
    "AREA": {"etiqueta": "Área", "tipo": "geográfica", "valores": {1: "Urbano", 2: "Rural"}},
    "ZONA": {"etiqueta": "Zona", "tipo": "geográfica"},
    "NUM_VIVIENDA": {"etiqueta": "Número de vivienda", "tipo": "identificador"},
    "NUM_HOGAR": {"etiqueta": "Número del hogar en la vivienda", "tipo": "identificador"},
    "PCP1": {"etiqueta": "Número de persona", "tipo": "identificador"},
    # --- Demographics ---
    "PCP5": {
        "etiqueta": "¿Qué parentesco o relación tiene con la jefa o el jefe del hogar?",
        "tipo": "demográfica",
        "valores": {
            1: "Jefe(a) del hogar", 2: "Esposo(a) o pareja", 3: "Hijo(a) o hijastro(a)",
            4: "Nieto(a)", 5: "Yerno o nuera", 6: "Padre, madre o suegro(a)",
            7: "Hermano(a) o cuñado(a)", 8: "Otro pariente", 9: "Empleado(a) doméstico(a)",
            10: "Otro no pariente",
        },
    },
    "PCP6": {
        "etiqueta": "¿Sexo de la persona?",
        "tipo": "demográfica",
        "valores": {1: "Hombre", 2: "Mujer"},
    },
    "PCP7": {"etiqueta": "¿Cuántos años cumplidos tiene?", "tipo": "demográfica"},
    "PCP9": {
        "etiqueta": "¿Tiene Fe de edad o está inscrito en el RENAP?",
        "tipo": "demográfica",
        "valores": {1: "Sí", 2: "No"},
    },
    # --- Place of birth ---
    "PCP10": {"etiqueta": "¿En qué municipio y departamento o país nació?", "tipo": "migración"},
    "PCP10_B": {"etiqueta": "Departamento de nacimiento", "tipo": "migración"},
    "LUGNACGEO": {"etiqueta": "Municipio de nacimiento", "tipo": "migración"},
    "PCP10_C": {"etiqueta": "País de nacimiento", "tipo": "migración"},
    "PCP10_D": {"etiqueta": "Año de llegada al país", "tipo": "migración"},
    # --- Residence in 2013 ---
    "PCP11": {"etiqueta": "¿En qué municipio y departamento o país residía habitualmente en abril 2013?", "tipo": "migración"},
    "PCP11_B": {"etiqueta": "Departamento de residencia en abril de 2013", "tipo": "migración"},
    "RESCINGEO": {"etiqueta": "Municipio de residencia en abril de 2013", "tipo": "migración"},
    "PCP11_C": {"etiqueta": "País de residencia en abril de 2013", "tipo": "migración"},
    # --- Ethnicity & language ---
    "PCP12": {
        "etiqueta": "Según su origen o historia, ¿cómo se considera o auto identifica?",
        "tipo": "étnica",
        "valores": {
            1: "Maya", 2: "Garífuna", 3: "Xinka", 4: "Afrodescendiente / Creole / Afromestizo",
            5: "Ladino(a)", 6: "Extranjero(a)",
        },
    },
    "PCP13": {
        "etiqueta": "¿A qué comunidad lingüística pertenece?",
        "tipo": "étnica",
        "valores": {
            1: "Achi", 2: "Akateka", 3: "Awakateka", 4: "Ch'orti'",
            5: "Chalchiteka", 6: "Chuj", 7: "Itza'", 8: "Ixil",
            9: "Jakalteko/Popti'", 10: "K'iche'", 11: "Kaqchiquel", 12: "Mam",
            13: "Mopan", 14: "Poqomam", 15: "Poqomchi'", 16: "Q'anjob'al",
            17: "Q'eqchi'", 18: "Sakapulteka", 19: "Sipakapense",
            20: "Tektiteka", 21: "Tz'utujil", 22: "Uspanteka",
        },
    },
    "PCP14": {
        "etiqueta": "¿Utiliza regularmente ropa o traje maya, garífuna, afrodescendiente o xinka?",
        "tipo": "étnica",
        "valores": {1: "Sí", 2: "No"},
    },
    "PCP15": {
        "etiqueta": "¿Cuál es el idioma en el que aprendió a hablar?",
        "tipo": "étnica",
        "valores": {
            1: "Achí", 2: "Akateko", 3: "Awakateko", 4: "Ch'orti'",
            5: "Chalchiteko", 6: "Chuj", 7: "Itza'", 8: "Ixil",
            9: "Jakalteko/Popti'", 10: "K'iche'", 11: "Kaqchiquel", 12: "Mam",
            13: "Mopan", 14: "Poqomam", 15: "Poqomchi'", 16: "Q'anjob'al",
            17: "Q'eqchi'", 18: "Sakapulteko", 19: "Sipakapense",
            20: "Tektiteko", 21: "Tz'utujil", 22: "Uspanteko",
            23: "Xinka", 24: "Garífuna", 25: "Español", 26: "Inglés",
            27: "Señas", 28: "Otro idioma", 98: "No habla",
        },
    },
    # --- Disability ---
    "PCP16_A": {"etiqueta": "¿Tiene alguna dificultad para ver?", "tipo": "discapacidad",
                "valores": {1: "No tiene dificultad", 2: "Sí, alguna dificultad", 3: "Sí, mucha dificultad", 4: "No puede hacerlo"}},
    "PCP16_B": {"etiqueta": "¿Tiene alguna dificultad para oír?", "tipo": "discapacidad",
                "valores": {1: "No tiene dificultad", 2: "Sí, alguna dificultad", 3: "Sí, mucha dificultad", 4: "No puede hacerlo"}},
    "PCP16_C": {"etiqueta": "¿Tiene alguna dificultad para caminar o subir escaleras?", "tipo": "discapacidad",
                "valores": {1: "No tiene dificultad", 2: "Sí, alguna dificultad", 3: "Sí, mucha dificultad", 4: "No puede hacerlo"}},
    "PCP16_D": {"etiqueta": "¿Tiene alguna dificultad para recordar o concentrarse?", "tipo": "discapacidad",
                "valores": {1: "No tiene dificultad", 2: "Sí, alguna dificultad", 3: "Sí, mucha dificultad", 4: "No puede hacerlo"}},
    "PCP16_E": {"etiqueta": "¿Tiene alguna dificultad para el cuidado personal o para vestirse?", "tipo": "discapacidad",
                "valores": {1: "No tiene dificultad", 2: "Sí, alguna dificultad", 3: "Sí, mucha dificultad", 4: "No puede hacerlo"}},
    "PCP16_F": {"etiqueta": "¿Tiene alguna dificultad para comunicarse?", "tipo": "discapacidad",
                "valores": {1: "No tiene dificultad", 2: "Sí, alguna dificultad", 3: "Sí, mucha dificultad", 4: "No puede hacerlo"}},
    # --- Education ---
    "PCP17_A": {
        "etiqueta": "¿Cuál fue el nivel de estudios más alto que aprobó?",
        "tipo": "educación",
        "valores": {
            0: "Ninguno", 1: "Preprimaria", 2: "Primaria", 3: "Básico",
            4: "Diversificado", 5: "Licenciatura", 6: "Maestría o doctorado",
        },
    },
    "PCP17_B": {"etiqueta": "¿Cuál fue el grado de estudios más alto que aprobó?", "tipo": "educación"},
    "PCP18": {
        "etiqueta": "Durante el ciclo escolar 2018, ¿asiste a un establecimiento educativo a estudiar?",
        "tipo": "educación",
        "valores": {1: "Sí", 2: "No"},
    },
    "PCP19": {
        "etiqueta": "¿El establecimiento educativo al que asiste en este año es:",
        "tipo": "educación",
        "valores": {1: "Público", 2: "Privado", 3: "Municipal o por cooperativa"},
    },
    "PCP20": {"etiqueta": "¿En qué municipio y departamento o país estudia?", "tipo": "educación"},
    "PCP20_B": {"etiqueta": "Departamento donde estudia", "tipo": "educación"},
    "ESTUDIAGEO": {"etiqueta": "Municipio donde estudia", "tipo": "educación"},
    "PCP20_C": {"etiqueta": "País donde estudia", "tipo": "educación"},
    "PCP21": {
        "etiqueta": "¿Cuál es la causa principal por la que no asiste a un establecimiento educativo?",
        "tipo": "educación",
        "valores": {
            1: "Falta de dinero", 2: "Tiene que trabajar",
            3: "No hay escuela, instituto o universidad",
            4: "Los padres / pareja no quieren", 5: "Quehaceres del hogar",
            6: "No le gusta / no quiere ir", 7: "Ya terminó sus estudios",
            8: "Enfermedad o discapacidad", 9: "Falta de maestro",
            10: "Embarazo", 11: "Se casó o se unió",
            12: "Algún tipo de violencia", 13: "Cambio de residencia",
            14: "Enseñan en otro idioma", 15: "Cuidado de personas",
            16: "Los padres consideran que aún no tiene la edad",
            17: "Otra causa", 99: "No declarado",
        },
    },
    # --- Literacy ---
    "PCP22": {
        "etiqueta": "¿Sabe leer y escribir?",
        "tipo": "educación",
        "valores": {1: "Sí", 2: "No"},
    },
    "PCP23_A": {
        "etiqueta": "¿En qué idioma sabe leer y escribir? Idioma 1",
        "tipo": "educación",
        "valores": {
            1: "Achí", 2: "Akateko", 3: "Awakateko", 4: "Ch'orti'",
            5: "Chalchiteko", 6: "Chuj", 7: "Itza'", 8: "Ixil",
            9: "Jakalteko/Popti'", 10: "K'iche'", 11: "Kaqchiquel", 12: "Mam",
            13: "Mopan", 14: "Poqomam", 15: "Poqomchi'", 16: "Q'anjob'al",
            17: "Q'eqchi'", 18: "Sakapulteko", 19: "Sipakapense",
            20: "Tektiteko", 21: "Tz'utujil", 22: "Uspanteko",
            23: "Xinka", 24: "Garífuna", 25: "Español", 26: "Inglés",
            27: "Señas", 28: "Otro idioma",
        },
    },
    "PCP23_B": {
        "etiqueta": "¿En qué idioma sabe leer y escribir? Idioma 2",
        "tipo": "educación",
        "valores": {
            1: "Achí", 2: "Akateko", 3: "Awakateko", 4: "Ch'orti'",
            5: "Chalchiteko", 6: "Chuj", 7: "Itza'", 8: "Ixil",
            9: "Jakalteko/Popti'", 10: "K'iche'", 11: "Kaqchiquel", 12: "Mam",
            13: "Mopan", 14: "Poqomam", 15: "Poqomchi'", 16: "Q'anjob'al",
            17: "Q'eqchi'", 18: "Sakapulteko", 19: "Sipakapense",
            20: "Tektiteko", 21: "Tz'utujil", 22: "Uspanteko",
            23: "Xinka", 24: "Garífuna", 25: "Español", 26: "Inglés",
            27: "Señas", 28: "Otro idioma",
        },
    },
    "PCP23_C": {
        "etiqueta": "¿En qué idioma sabe leer y escribir? Idioma 3",
        "tipo": "educación",
        "valores": {
            1: "Achí", 2: "Akateko", 3: "Awakateko", 4: "Ch'orti'",
            5: "Chalchiteko", 6: "Chuj", 7: "Itza'", 8: "Ixil",
            9: "Jakalteko/Popti'", 10: "K'iche'", 11: "Kaqchiquel", 12: "Mam",
            13: "Mopan", 14: "Poqomam", 15: "Poqomchi'", 16: "Q'anjob'al",
            17: "Q'eqchi'", 18: "Sakapulteko", 19: "Sipakapense",
            20: "Tektiteko", 21: "Tz'utujil", 22: "Uspanteko",
            23: "Xinka", 24: "Garífuna", 25: "Español", 26: "Inglés",
            27: "Señas", 28: "Otro idioma",
        },
    },
    "PCP24": {
        "etiqueta": "Además del idioma en el que aprendió a hablar, ¿sabe hablar otro idioma?",
        "tipo": "educación",
        "valores": {1: "Sí", 2: "No"},
    },
    "PCP25_A": {
        "etiqueta": "¿En qué otro idioma sabe hablar? Idioma 1",
        "tipo": "educación",
        "valores": {
            1: "Achí", 2: "Akateko", 3: "Awakateko", 4: "Ch'orti'",
            5: "Chalchiteko", 6: "Chuj", 7: "Itza'", 8: "Ixil",
            9: "Jakalteko/Popti'", 10: "K'iche'", 11: "Kaqchiquel", 12: "Mam",
            13: "Mopan", 14: "Poqomam", 15: "Poqomchi'", 16: "Q'anjob'al",
            17: "Q'eqchi'", 18: "Sakapulteko", 19: "Sipakapense",
            20: "Tektiteko", 21: "Tz'utujil", 22: "Uspanteko",
            23: "Xinka", 24: "Garífuna", 25: "Español", 26: "Inglés",
            27: "Señas", 28: "Otro idioma",
        },
    },
    "PCP25_B": {
        "etiqueta": "¿En qué otro idioma sabe hablar? Idioma 2",
        "tipo": "educación",
        "valores": {
            1: "Achí", 2: "Akateko", 3: "Awakateko", 4: "Ch'orti'",
            5: "Chalchiteko", 6: "Chuj", 7: "Itza'", 8: "Ixil",
            9: "Jakalteko/Popti'", 10: "K'iche'", 11: "Kaqchiquel", 12: "Mam",
            13: "Mopan", 14: "Poqomam", 15: "Poqomchi'", 16: "Q'anjob'al",
            17: "Q'eqchi'", 18: "Sakapulteko", 19: "Sipakapense",
            20: "Tektiteko", 21: "Tz'utujil", 22: "Uspanteko",
            23: "Xinka", 24: "Garífuna", 25: "Español", 26: "Inglés",
            27: "Señas", 28: "Otro idioma",
        },
    },
    "PCP25_C": {
        "etiqueta": "¿En qué otro idioma sabe hablar? Idioma 3",
        "tipo": "educación",
        "valores": {
            1: "Achí", 2: "Akateko", 3: "Awakateko", 4: "Ch'orti'",
            5: "Chalchiteko", 6: "Chuj", 7: "Itza'", 8: "Ixil",
            9: "Jakalteko/Popti'", 10: "K'iche'", 11: "Kaqchiquel", 12: "Mam",
            13: "Mopan", 14: "Poqomam", 15: "Poqomchi'", 16: "Q'anjob'al",
            17: "Q'eqchi'", 18: "Sakapulteko", 19: "Sipakapense",
            20: "Tektiteko", 21: "Tz'utujil", 22: "Uspanteko",
            23: "Xinka", 24: "Garífuna", 25: "Español", 26: "Inglés",
            27: "Señas", 28: "Otro idioma",
        },
    },
    # --- Technology ---
    "PCP26_A": {"etiqueta": "En los últimos tres meses, ¿ha usado celular?", "tipo": "tecnología", "valores": {1: "Sí", 2: "No"}},
    "PCP26_B": {"etiqueta": "En los últimos tres meses, ¿ha usado computadora?", "tipo": "tecnología", "valores": {1: "Sí", 2: "No"}},
    "PCP26_C": {"etiqueta": "En los últimos tres meses, ¿ha usado internet?", "tipo": "tecnología", "valores": {1: "Sí", 2: "No"}},
    # --- Employment ---
    "PCP27": {"etiqueta": "¿Trabajó durante la semana pasada?", "tipo": "empleo", "valores": {1: "Sí", 2: "No"}},
    "PCP28": {
        "etiqueta": "¿Qué hizo durante la semana pasada:",
        "tipo": "empleo",
        "valores": {
            1: "No trabajó, pero tiene trabajo (vacaciones, licencia, etc.)",
            2: "Participó o ayudó en actividades agropecuarias",
            3: "Elaboró o ayudó a elaborar productos alimenticios para la venta",
            4: "Elaboró o ayudó a elaborar artículos (sombreros, artesanías, muebles) para la venta",
            5: "Elaboró o ayudó a hilar, tejer o coser artículos para la venta",
            6: "Participó o ayudó en actividades comerciales o de servicios",
            7: "No trabajó",
        },
    },
    "PCP29": {
        "etiqueta": "Si no trabajó, ¿qué fue lo que hizo durante la semana pasada:",
        "tipo": "empleo",
        "valores": {
            1: "Buscó trabajo y trabajó antes", 2: "Buscó trabajo por primera vez",
            3: "Únicamente estudió", 4: "Únicamente vivió de su renta o jubilación",
            5: "Quehaceres del hogar", 6: "Cuidado de personas",
            7: "Cargo comunitario", 8: "Otra actividad no remunerada",
            9: "No declarado",
        },
    },
    "PCP30_2D": {
        "etiqueta": "Ocupación (2 dígitos)",
        "tipo": "empleo",
        "valores": {
            1: "Oficiales de las fuerzas armadas", 2: "Suboficiales de las fuerzas armadas",
            3: "Otros miembros de las fuerzas armadas",
            11: "Directores ejecutivos y personal directivo de administración pública",
            12: "Directores administradores y comerciales",
            13: "Directores y gerentes de producción y operaciones",
            14: "Gerentes de hoteles, restaurantes, comercios y otros servicios",
            21: "Profesionales de las ciencias y de la ingeniería",
            22: "Profesionales de la salud", 23: "Profesionales de la enseñanza",
            24: "Especialistas en administración pública y empresas",
            25: "Profesionales de TIC", 26: "Profesionales en derecho y ciencias sociales",
            31: "Técnicos de ciencias e ingeniería de nivel medio",
            32: "Profesionales de nivel medio de la salud",
            33: "Profesionales de nivel medio en finanzas y administración",
            34: "Técnicos de servicios jurídicos, sociales y culturales",
            35: "Técnicos de TIC", 41: "Oficinistas y secretarios",
            42: "Empleados en trato directo con el público",
            43: "Empleados contables y de registro", 44: "Otro personal administrativo",
            51: "Trabajadores de servicios personales", 52: "Vendedores",
            53: "Trabajadores de cuidados personales", 54: "Personal de protección",
            61: "Agricultores y trabajadores agropecuarios calificados",
            62: "Trabajadores forestales, pescadores y cazadores calificados",
            63: "Trabajadores agropecuarios de subsistencia",
            71: "Oficiales y operarios de construcción (excl. electricistas)",
            72: "Oficiales y operarios de metalurgia y construcción mecánica",
            73: "Artesanos y operarios de artes gráficas",
            74: "Trabajadores especializados en electricidad",
            75: "Operarios de procesamiento de alimentos, confección y artesanos",
            81: "Operadores de instalaciones fijas y máquinas", 82: "Ensambladores",
            83: "Conductores y operadores de equipos pesados",
            91: "Limpiadores y asistentes", 92: "Peones agropecuarios y forestales",
            93: "Peones de minería, construcción e industria",
            94: "Ayudantes de preparación de alimentos",
            95: "Vendedores ambulantes", 96: "Recolectores de desechos",
            999: "Ocupación no especificada",
        },
    },
    "PCP30_1D": {
        "etiqueta": "Ocupación (1 dígito)",
        "tipo": "empleo",
        "valores": {
            0: "Ocupaciones militares", 1: "Directores y gerentes",
            2: "Profesionales científicos e intelectuales",
            3: "Técnicos y profesionales de nivel medio",
            4: "Personal de apoyo administrativo",
            5: "Trabajadores de servicios y vendedores",
            6: "Agricultores y trabajadores agropecuarios calificados",
            7: "Oficiales, operarios y artesanos",
            8: "Operadores de instalaciones y máquinas",
            9: "Ocupaciones elementales", 99: "Ocupación no especificada",
        },
    },
    "PCP31_D": {
        "etiqueta": "Categoría ocupacional",
        "tipo": "empleo",
        "valores": {
            1: "Patrono(a) o empleador(a)", 2: "Cuenta propia con local",
            3: "Cuenta propia sin local", 4: "Empleado(a) público(a)",
            5: "Empleado(a) privado(a)", 6: "Empleado(a) doméstico(a)",
            7: "Familiar no remunerado", 9: "No declarado",
        },
    },
    "PCP32_2D": {
        "etiqueta": "Actividad económica (2 dígitos)",
        "tipo": "empleo",
        "valores": {
            1: "Agricultura, ganadería, caza y servicios conexos",
            2: "Silvicultura y extracción de madera", 3: "Pesca y acuicultura",
            5: "Extracción de carbón de piedra y lignito",
            6: "Extracción de petróleo crudo y gas natural",
            7: "Extracción de minerales metálicos",
            8: "Explotación de otras minas y canteras",
            9: "Servicios de apoyo para minería y canteras",
            10: "Elaboración de productos alimenticios", 11: "Elaboración de bebidas",
            12: "Elaboración de productos de tabaco", 13: "Fabricación de textiles",
            14: "Fabricación de prendas de vestir",
            15: "Fabricación de productos de cuero",
            16: "Producción de madera y fabricación de productos de madera",
            17: "Fabricación de papel", 18: "Impresión y reproducción de grabaciones",
            19: "Fabricación de coque y productos de refinación del petróleo",
            20: "Fabricación de sustancias y productos químicos",
            21: "Fabricación de productos farmacéuticos",
            22: "Fabricación de productos de caucho y plástico",
            23: "Fabricación de otros productos minerales no metálicos",
            24: "Fabricación de metales comunes",
            25: "Fabricación de productos elaborados de metal",
            26: "Fabricación de productos de informática y electrónica",
            27: "Fabricación de equipo eléctrico",
            28: "Fabricación de maquinaria y equipo",
            29: "Fabricación de vehículos automotores",
            30: "Fabricación de otro equipo de transporte",
            31: "Fabricación de muebles", 32: "Otras industrias manufactureras",
            33: "Reparación e instalación de maquinaria",
            35: "Suministro de electricidad, gas y vapor",
            36: "Captación, tratamiento y distribución de agua",
            37: "Evacuación de aguas residuales",
            38: "Gestión de desechos y recuperación de materiales",
            39: "Descontaminación y gestión de desechos",
            41: "Construcción de edificios", 42: "Obras de ingeniería civil",
            43: "Actividades especializadas de construcción",
            45: "Comercio y reparación de vehículos automotores",
            46: "Comercio al por mayor", 47: "Comercio al por menor",
            49: "Transporte terrestre", 50: "Transporte acuático",
            51: "Transporte aéreo", 52: "Almacenamiento y apoyo al transporte",
            53: "Actividades postales y de mensajería",
            55: "Actividades de alojamiento",
            56: "Servicios de comidas y bebidas",
            58: "Actividades de edición",
            59: "Producción de películas y grabación de sonido",
            60: "Programación y transmisión",
            61: "Telecomunicaciones", 62: "Programación informática",
            63: "Servicios de información",
            64: "Servicios financieros", 65: "Seguros y fondos de pensiones",
            66: "Actividades auxiliares de servicios financieros",
            68: "Actividades inmobiliarias",
            69: "Actividades jurídicas y de contabilidad",
            70: "Actividades de oficinas principales y consultoría de gestión",
            71: "Arquitectura, ingeniería y ensayos técnicos",
            72: "Investigación científica y desarrollo",
            73: "Publicidad y estudios de mercado",
            74: "Otras actividades profesionales, científicas y técnicas",
            75: "Actividades veterinarias", 77: "Alquiler y arrendamiento",
            78: "Actividades de empleo",
            79: "Agencias de viajes y operadores turísticos",
            80: "Actividades de seguridad e investigación",
            81: "Servicios a edificios y paisajismo",
            82: "Actividades administrativas y de apoyo",
            84: "Administración pública y defensa",
            85: "Enseñanza", 86: "Atención de la salud humana",
            87: "Atención en instituciones", 88: "Asistencia social sin alojamiento",
            90: "Actividades creativas y de entretenimiento",
            91: "Bibliotecas, archivos, museos y actividades culturales",
            92: "Juegos de azar y apuestas",
            93: "Actividades deportivas y recreativas",
            94: "Actividades de asociaciones",
            95: "Reparación de ordenadores y efectos personales",
            96: "Otras actividades de servicios personales",
            97: "Hogares como empleadores de personal doméstico",
            98: "Hogares como productores de bienes y servicios para uso propio",
            99: "Organizaciones y órganos extraterritoriales",
            999: "Rama de actividad económica no especificada",
        },
    },
    "PCP32_1D": {
        "etiqueta": "Actividad económica (1 dígito)",
        "tipo": "empleo",
        "valores": {
            1: "Agricultura, ganadería, silvicultura y pesca",
            2: "Explotación de minas y canteras",
            3: "Industrias manufactureras",
            4: "Suministro de electricidad, gas, vapor y aire acondicionado",
            5: "Suministro de agua; evacuación de aguas residuales y descontaminación",
            6: "Construcción",
            7: "Comercio al por mayor y menor; reparación de vehículos",
            8: "Transporte y almacenamiento",
            9: "Actividades de alojamiento y servicio de comidas",
            10: "Información y comunicaciones",
            11: "Actividades financieras y de seguros",
            12: "Actividades inmobiliarias",
            13: "Actividades profesionales, científicas y técnicas",
            14: "Actividades de servicios administrativos y de apoyo",
            15: "Administración pública y defensa",
            16: "Enseñanza",
            17: "Actividades de atención de la salud y asistencia social",
            18: "Actividades artísticas, de entretenimiento y recreativas",
            19: "Otras actividades de servicios",
            20: "Actividades de hogares como empleadores",
            21: "Actividades de organizaciones extraterritoriales",
            99: "Rama de actividad económica no especificada",
        },
    },
    "PCP33": {"etiqueta": "¿En qué municipio y departamento trabaja o trabajó?", "tipo": "empleo"},
    "PCP33_B": {"etiqueta": "Departamento donde trabaja o trabajó", "tipo": "empleo"},
    "TRABAJAGEO": {"etiqueta": "Municipio donde trabaja o trabajó", "tipo": "empleo"},
    "PCP33_C": {"etiqueta": "País donde trabaja o trabajó", "tipo": "empleo"},
    # --- Marital status ---
    "PCP34": {
        "etiqueta": "¿Cuál es su estado conyugal actual?",
        "tipo": "demográfica",
        "valores": {
            1: "Unido(a)", 2: "Casado(a)", 3: "Separado(a)",
            4: "Divorciado(a)", 5: "Viudo(a)", 6: "Soltero(a)",
        },
    },
    # --- Fertility ---
    "PCP35_A": {"etiqueta": "¿Cuántas hijas e hijos nacidos vivos ha tenido? Total", "tipo": "fecundidad"},
    "PCP35_B": {"etiqueta": "¿Cuántas hijas e hijos nacidos vivos ha tenido? Mujeres", "tipo": "fecundidad"},
    "PCP35_C": {"etiqueta": "¿Cuántas hijas e hijos nacidos vivos ha tenido? Hombres", "tipo": "fecundidad"},
    "PCP36_A": {"etiqueta": "¿Cuántos de sus hijas e hijos están vivos actualmente? Total", "tipo": "fecundidad"},
    "PCP36_B": {"etiqueta": "¿Cuántos de sus hijas e hijos están vivos actualmente? Mujeres", "tipo": "fecundidad"},
    "PCP36_C": {"etiqueta": "¿Cuántos de sus hijas e hijos están vivos actualmente? Hombres", "tipo": "fecundidad"},
    "PCP37": {"etiqueta": "¿A qué edad tuvo su primera hija o hijo nacido vivo?", "tipo": "fecundidad"},
    "PCP38_A": {"etiqueta": "Día de nacimiento de su última hija(o) nacida(o) viva(o)", "tipo": "fecundidad"},
    "PCP38_B": {"etiqueta": "Mes de nacimiento de su última hija(o) nacida(o) viva(o)", "tipo": "fecundidad"},
    "PCP38_C": {"etiqueta": "Año de nacimiento de su última hija(o) nacida(o) viva(o)", "tipo": "fecundidad"},
    "PCP39": {"etiqueta": "¿Está viva(o) su última(o) hija(o) nacida(o) viva(o)?", "tipo": "fecundidad", "valores": {1: "Sí", 2: "No"}},
    # --- Derived variables ---
    "VIVEHABGEO": {"etiqueta": "Departamento y municipio de residencia habitual", "tipo": "derivada"},
    "NIVGRADO": {
        "etiqueta": "Nivel y grado de estudio",
        "tipo": "derivada",
        "valores": {
            10: "Ninguna",
            20: "Preprimaria",
            31: "1ero Primaria", 32: "2do Primaria", 33: "3ero Primaria",
            34: "4to Primaria", 35: "5to Primaria", 36: "6to Primaria",
            41: "1ero Básico", 42: "2do Básico", 43: "3ero Básico",
            44: "4to Diversificado", 45: "5to Diversificado",
            46: "6to/7mo Diversificado",
            51: "1er Licenciatura", 52: "2do Licenciatura",
            53: "3ero Licenciatura", 54: "4to Licenciatura",
            55: "5to/6to Licenciatura",
            61: "1er Maestría", 62: "2do Maestría",
            71: "1er Doctorado", 72: "2do Doctorado",
        },
    },
    "ANEDUCA": {"etiqueta": "Años de estudio", "tipo": "derivada"},
    "PEA": {"etiqueta": "Población económicamente activa", "tipo": "derivada", "valores": {1: "PEA", 2: "No PEA"}},
    "POCUPA": {
        "etiqueta": "Personas ocupadas",
        "tipo": "derivada",
        "valores": {1: "Población ocupada"},
    },
    "PDESOC": {
        "etiqueta": "Personas desocupadas",
        "tipo": "derivada",
        "valores": {1: "Aspirante", 2: "Cesante"},
    },
    "MIGRA_VIDA": {"etiqueta": "Migrante y no migrante (migración de toda la vida)", "tipo": "derivada"},
    "MIGRA_REC": {"etiqueta": "Migrante y no migrante (migración reciente)", "tipo": "derivada"},
    "PEI": {"etiqueta": "Población económicamente inactiva", "tipo": "derivada"},
}


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _download_parquet(depto_code: int) -> Path:
    """Download a single departamento parquet file, with caching."""
    import requests

    cache_dir = get_cache_dir()
    filename = f"persona_depto_{depto_code:02d}.parquet"
    cached = cache_dir / filename

    if cached.exists():
        return cached

    url = _parquet_url(depto_code)
    print(f"⬇  Descargando personas depto {depto_code:02d} ({DEPARTAMENTO_CODES[depto_code]})...")

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

def personas(
    departamento: Optional[Union[str, int]] = None,
    municipio: Optional[Union[str, int]] = None,
    geometry: Optional[str] = None,
) -> Union[pd.DataFrame, "gpd.GeoDataFrame"]:
    """Load person-level microdata from INE Censo 2018.

    Each row is one person. Downloads partitioned Parquet files
    from GitHub (~3–70 MB per departamento, 333 MB total).

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
    >>> # Single departamento
    >>> df = gq.personas(departamento="Huehuetenango")
    >>>
    >>> # By municipio
    >>> df = gq.personas(municipio="Antigua Guatemala")
    >>>
    >>> # All of Guatemala (~333 MB, 14.9M rows)
    >>> df = gq.personas()
    >>>
    >>> # With geometry
    >>> gdf = gq.personas(departamento="Sacatepéquez", geometry="municipio")
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
            print(f"⬇  Descargando personas para los 22 departamentos (~333 MB)...")

    # Download and concatenate
    frames = []
    for code in codes_to_download:
        path = _download_parquet(code)
        frames.append(pd.read_parquet(path))

    df = pd.concat(frames, ignore_index=True)

    if len(codes_to_download) > 1:
        print(f"   ✓ {len(df):,} personas cargadas")

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
    """Describe the variables in the personas dataset.

    Parameters
    ----------
    variable : str, optional
        Specific variable (e.g. ``"PCP12"``). If ``None``, lists all.

    Returns
    -------
    pandas.DataFrame or dict

    Examples
    --------
    >>> import geoquetzal as gq
    >>> gq.describe_personas()           # all 84 variables
    >>> gq.describe_personas("PCP12")    # ethnic self-identification
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
