"""
GeoQuetzal — Geographic and Census Data for Guatemala
=====================================================

A Python library for accessing Guatemala's administrative boundaries
and census microdata. The ``tigris`` equivalent for Guatemalan researchers.

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
>>> # Census microdata
>>> df = gq.emigracion(departamento="Huehuetenango")
>>> df = gq.hogares(municipio="Antigua Guatemala")
>>> df = gq.personas(departamento=3)
>>> df = gq.vivienda()
>>>
>>> # Explore variables
>>> gq.describe_hogares("PCH4")
>>> gq.describe_emigracion()

Submodules
----------
- ``geoquetzal.geography`` — administrative boundaries (MINFIN + GADM)
- ``geoquetzal.emigracion`` — emigration microdata (INE Censo 2018)
- ``geoquetzal.hogares`` — household microdata (INE Censo 2018)
- ``geoquetzal.vivienda`` — housing microdata (INE Censo 2018)
- ``geoquetzal.personas`` — person-level microdata (INE Censo 2018)
- ``geoquetzal.crs`` — coordinate reference systems for Guatemala
- ``geoquetzal.plotting`` — static and interactive map helpers

Data sources
------------
- Boundaries: MINFIN Guatemala + GADM v4.1
- Census data: INE Guatemala, XII Censo 2018
"""

__version__ = "1.0.1"
__author__ = "Jorge Yass & Anasilvia Salazar"

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
from geoquetzal.vivienda import vivienda
from geoquetzal.personas import personas

# Describe functions — aliased to avoid name collision
from geoquetzal.emigracion import describe as describe_emigracion
from geoquetzal.hogares import describe as describe_hogares
from geoquetzal.vivienda import describe as describe_vivienda
from geoquetzal.personas import describe as describe_personas

__all__ = [
    # Geography
    "country",
    "departamentos",
    "municipios",
    "lagos",
    # Census microdata
    "emigracion",
    "hogares",
    "vivienda",
    "personas",
    # Describe helpers
    "describe_emigracion",
    "describe_hogares",
    "describe_vivienda",
    "describe_personas",
]
