"""
GeoQuetzal — Geographic and Census Data for Guatemala
=====================================================

A Python library for accessing Guatemala's administrative boundaries
and census microdata from the XII Censo Nacional de Población y VII de
Vivienda 2018 (INE). The first census data library for Guatemala and
Central America.

Quick start
-----------
>>> import geoquetzal as gq
>>>
>>> # Administrative boundaries
>>> deptos = gq.departamentos()
>>> munis  = gq.municipios("Sacatepéquez")
>>>
>>> # Lakes for prettier maps
>>> ax = deptos.plot(color="lightyellow", edgecolor="gray")
>>> gq.lagos().plot(ax=ax, color="lightblue", edgecolor="steelblue")
>>>
>>> # Census microdata (person, household, housing)
>>> df = gq.personas(departamento="Huehuetenango")
>>> df = gq.hogares(municipio="Antigua Guatemala")
>>> df = gq.viviendas(departamento=3)
>>> df = gq.emigracion()
>>>
>>> # Sub-municipal data (lugar poblado level)
>>> df  = gq.lugares_poblados(departamento="Sacatepéquez")
>>> gdf = gq.lugares_poblados(geometry=True)
>>>
>>> # Voronoi polygon approximations for choropleth mapping
>>> vor = gq.voronoi_lugares_poblados(departamento="Sacatepéquez")
>>>
>>> # Explore variables
>>> gq.describe_personas("PCP12")
>>> gq.describe_hogares("PCH4")
>>> gq.describe_lugares_poblados("pcp12_maya")

Submodules
----------
- ``geoquetzal.geography``         — administrative boundaries (MINFIN + GADM)
- ``geoquetzal.emigracion``        — emigration microdata (INE Censo 2018)
- ``geoquetzal.hogares``           — household microdata (INE Censo 2018)
- ``geoquetzal.vivienda``          — housing unit microdata (INE Censo 2018)
- ``geoquetzal.personas``          — person-level microdata (INE Censo 2018)
- ``geoquetzal.lugares_poblados``  — pre-aggregated sub-municipal indicators
- ``geoquetzal.voronoi``           — Voronoi polygon approximations for lugares poblados
- ``geoquetzal.crs``               — coordinate reference systems for Guatemala
- ``geoquetzal.plotting``          — static and interactive map helpers

Data sources
------------
- Boundaries: MINFIN Guatemala + GADM v4.1
- Census data: INE Guatemala, XII Censo 2018
  https://github.com/geoquetzal/censo2018/releases
"""

__version__ = "1.1.1"
__author__ = "Jorge Yass"

# Geography — top level
from geoquetzal.geography import (
    country,
    departamentos,
    municipios,
    lagos,
)

# Census microdata — top level
from geoquetzal.emigracion import emigracion
from geoquetzal.hogares import hogares
from geoquetzal.vivienda import viviendas
from geoquetzal.vivienda import vivienda  # backward-compatible alias
from geoquetzal.personas import personas
from geoquetzal.lugares_poblados import lugares_poblados
from geoquetzal.voronoi import voronoi_lugares_poblados

# Describe functions — aliased to avoid name collision
from geoquetzal.emigracion import describe as describe_emigracion
from geoquetzal.hogares import describe as describe_hogares
from geoquetzal.vivienda import describe as describe_viviendas
from geoquetzal.vivienda import describe as describe_vivienda  # backward-compatible alias
from geoquetzal.personas import describe as describe_personas
from geoquetzal.lugares_poblados import describe as describe_lugares_poblados

__all__ = [
    # Geography
    "country",
    "departamentos",
    "municipios",
    "lagos",
    # Census microdata
    "emigracion",
    "hogares",
    "viviendas",
    "vivienda",       # backward-compatible alias
    "personas",
    "lugares_poblados",
    "voronoi_lugares_poblados",
    # Describe helpers
    "describe_emigracion",
    "describe_hogares",
    "describe_viviendas",
    "describe_vivienda",  # backward-compatible alias
    "describe_personas",
    "describe_lugares_poblados",
]
