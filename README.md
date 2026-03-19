# GeoQuetzal

🌐 **English** | [Leer en español](README.es.md)

**Geographic and census data for Guatemala — the first library of its kind for Central America.**

GeoQuetzal gives Guatemalan students, researchers, entrepreneurs, and professionals programmatic access to administrative boundaries and census microdata, following the same philosophy as [`tigris`](https://github.com/walkerke/tigris)/[`tidycensus`](https://walker-data.com/tidycensus/) for the US and [`geobr`](https://github.com/ipeaGIT/geobr) for Brazil.

```python
import geoquetzal as gq

deptos = gq.departamentos()
deptos.plot(edgecolor="white", figsize=(8, 8))
```

## Why GeoQuetzal?

Working with Guatemalan geographic and census data typically means downloading shapefiles from GADM (Global Administrative Areas), cleaning up inconsistent name spellings, downloading census CSVs from INE (Instituto Nacional de Estadística), figuring out how to join them, and dealing with the fact that GADM spells "Quetzaltenango" as "Quezaltenango" and concatenates "San Marcos" into "SanMarcos".

GeoQuetzal handles all of that. One function call gives you clean data, ready to be analyzed with consistent INE names and numeric codes that join reliably.

## Installation

```bash
pip install geoquetzal

# With plotting support (matplotlib + folium)
pip install geoquetzal[plotting]

# Everything (adds contextily for basemaps)
pip install geoquetzal[all]
```

**Requirements:** Python 3.9+, geopandas, pandas, requests, pyarrow.

## Datasets

| Dataset | Records | Variables | Storage | Source |
|---|---|---|---|---|
| Boundaries | 22 deptos / 340 municipios | geometry + codes | Bundled (~1 MB) | MINFIN |
| Lagos | 2 lakes | geometry | Bundled | MINFIN |
| Emigración | 242,203 | 11 | GitHub (~1.6 MB) | INE Censo 2018 |
| Hogares | 3,275,931 | 37 | GitHub (~38 MB) | INE Censo 2018 |
| Vivienda | ~3,300,000 | 11 | GitHub (~30 MB) | INE Censo 2018 |
| Personas | 14,901,286 | 84 | GitHub (~333 MB) | INE Censo 2018 |

**Boundaries and lakes are bundled** in the package, they load instantly with no Internet connection. Census microdata is hosted as Parquet files on GitHub Releases and downloaded on-demand per departamento. After the first download, data loads from a local cache.

## Quick Start

Everything is available under `import geoquetzal as gq`:

### Administrative Boundaries

```python
import geoquetzal as gq

# Country outline
gq.country()

# All 22 departamentos (loads instantly — bundled in package)
deptos = gq.departamentos()

# By name or code (accent-insensitive)
gq.departamentos("Sacatepequez")     # accent-insensitive ✓
gq.departamentos("Sacatepéquez")     # exact spelling ✓
gq.departamentos(3)                  # INE code ✓

# By region
gq.departamentos(region="V - Central")

# Municipios — all 340 with INE codes
gq.municipios("Sacatepequez")                # all municipios in a departamento
gq.municipios(name="Antigua Guatemala")      # single municipio by name
gq.municipios(name=301)                      # single municipio by code

# Guatemala City zone-level polygons (uses GADM)
gq.municipios("Guatemala", zonas=True)       # 22 rows, one per zona

# Guatemalan map with lakes
ax = deptos.plot(color="lightyellow", edgecolor="gray")
gq.lagos().plot(ax=ax, color="lightblue", edgecolor="steelblue")
```

### Census Microdata

```python
import geoquetzal as gq

# Load all records
df = gq.emigracion()                    # 242K emigrant records
df = gq.hogares()                       # 3.2M households
df = gq.vivienda()                      # 3.3M housing units

#This instruction might take a while
df = gq.personas()                      # 14.9M people 

# Filter by departamento (only downloads that departamento's file)
df = gq.hogares(departamento="Huehuetenango")
df = gq.hogares(departamento=13)

# Filter by municipio
df = gq.hogares(municipio="Antigua Guatemala")
df = gq.hogares(municipio=301)
```

### Explore Variables (the descriptions are in Spanish)

```python
import geoquetzal as gq

gq.describe_hogares()            # summary table of all 37 variables
gq.describe_hogares("PCH4")      # water source — values and labels
gq.describe_hogares("PCH15")     # receives remittances

gq.describe_emigracion("PEI3")   # sex of emigrant
gq.describe_personas("PCP12")    # ethnic self-identification
gq.describe_vivienda("PCV2")     # wall material
```

### Variable Highlights

**Emigración**: sex (`PEI3`), age at departure (`PEI4`), year left (`PEI5`)

**Hogares**: water source (`PCH4`), sanitation (`PCH5`), electricity (`PCH8`), appliances — radio, TV, fridge, internet, car (`PCH9_A`–`PCH9_M`), cooking fuel (`PCH14`), remittances (`PCH15`)

**Vivienda**: housing type (`PCV1`), wall material (`PCV2`), roof (`PCV3`), floor (`PCV5`)

**Personas**: sex (`PCP6`), age (`PCP7`), ethnicity (`PCP12` — Maya/Garífuna/Xinka/Ladino), Mayan linguistic community (`PCP13`), mother tongue (`PCP15`), disability (`PCP16_A`–`PCP16_F`), education (`PCP17_A`), literacy (`PCP22`), tech access — cellphone/computer/internet (`PCP26_A`–`PCP26_C`), employment (`PCP27`), marital status (`PCP34`), fertility (`PCP35`–`PCP39`)

## Mapping Patterns

### Static Choropleth (matplotlib)

```python
import geoquetzal as gq

df = gq.hogares(departamento="Sacatepequez")
pct_internet = (
    df.groupby("MUNICIPIO")["PCH9_I"]
    .apply(lambda x: (x == 1).mean() * 100)
    .round(1)
    .reset_index(name="pct")
)

munis = gq.municipios("Sacatepequez")
result = munis.merge(pct_internet, left_on="codigo_muni", right_on="MUNICIPIO")
result.plot(column="pct", cmap="YlGnBu", legend=True, edgecolor="white")
```

### Interactive Map (folium)

```python
result.explore(
    column="pct",
    tooltip=["municipio", "pct"],
    tiles="CartoDB positron",
)
```

### Animated Choropleth (Plotly)

```python
### Animated Choropleth (Plotly)
import geoquetzal as gq
from geoquetzal.emigracion import emigracion
import plotly.express as px
import json

# Aggregate: emigrants per departamento per year
df = gq.emigracion()
df = df[df["PEI5"] != 9999]
agg = df.groupby(["DEPARTAMENTO", "PEI5"]).size().reset_index(name="emigrantes")

# Prepare GeoJSON
deptos = gq.departamentos()
geojson = json.loads(deptos.to_json())
for f in geojson["features"]:
    f["id"] = f["properties"]["codigo_depto"]

# Animated map
fig = px.choropleth(
    agg,
    geojson=geojson,
    title="Emigrantes por departamento por año",
    locations="DEPARTAMENTO",
    color="emigrantes",
    animation_frame="PEI5",
    color_continuous_scale="YlOrRd",
)
fig.update_geos(fitbounds="locations", visible=False)
fig.show()
```

> **IMPORTANT:** Always aggregate first with pandas, then merge geometry onto the 22 or 340 summary rows. Never use `geometry=` on large microdata, as it attaches a polygon to every row and is very slow.

## Using with Your Own Data

Any dataset with INE municipality or department codes works with GeoQuetzal boundaries:

```python
import geoquetzal as gq
import pandas as pd

my_data = pd.read_csv("my_research_data.csv")
munis = gq.municipios()
result = munis.merge(my_data, left_on="codigo_muni", right_on="your_code_column")
result.plot(column="your_variable", cmap="YlGnBu", legend=True)
```

## Coordinate Reference Systems

```python
from geoquetzal.crs import to_gtm, to_utm16n, suggest_crs

deptos = gq.departamentos()
suggest_crs(deptos)              # prints recommendations

deptos_gtm = to_gtm(deptos)     # Guatemala Transverse Mercator (national standard)
deptos_utm = to_utm16n(deptos)   # UTM Zone 16N (good for area/distance)
```

| CRS | EPSG | Use case |
|---|---|---|
| WGS 84 | 4326 | Default, web maps |
| Guatemala TM (GTM) | ESRI:103598 | National standard, official maps |
| UTM Zone 16N | 32616 | Area and distance calculations |

## How Data Works

**Boundaries** (departamentos, municipios, lakes) are bundled in the package from MINFIN (Ministerio de Finanzas Públicas de Guatemala) geospatial data. They load instantly with no network calls. All 340 municipios have correct INE codes built in.

**Census microdata** is partitioned by departamento into Parquet files and hosted on [GitHub Releases](https://github.com/geoquetzal/censo2018/releases). When you request a single departamento, only that file is downloaded (~1–15 MB). Requesting all of Guatemala downloads all 22 files. Everything is cached after the first download.

To clean the cache:
```python
from geoquetzal.cache import clear_cache
clear_cache()
```

**Joins** between census data and boundaries always use INE numeric codes (`codigo_depto`, `codigo_muni`), never names.

**Guatemala City zones** are available via `gq.municipios("Guatemala", zonas=True)`, which uses GADM v4.1 for the 22 zone polygons. The census microdata has a `ZONA` column you can join on.

## Data Sources & Attribution

- **Administrative boundaries**: [MINFIN Guatemala](https://github.com/minfin-bi/Mapas-TopoJSON-Guatemala) — Ministerio de Finanzas Públicas, 340 municipios
- **Country outline & zones**: [GADM v4.1](https://gadm.org) — freely available for academic and non-commercial use
- **Census microdata**: [INE Guatemala](https://censo2018.ine.gob.gt/descarga) — XII Censo Nacional de Población y VII de Vivienda 2018 (public data)
- **Hosted Parquet files**: [github.com/geoquetzal/censo2018](https://github.com/geoquetzal/censo2018/releases)

## Contributing

GeoQuetzal is open source under the MIT license. Contributions are welcome, especially around new datasets, documentation, and example notebooks.

```bash
git clone https://github.com/geoquetzal/geoquetzal.git
cd geoquetzal
pip install -e ".[dev,plotting]"
```

## Author

Created by **[Jorge Yass](https://www.linkedin.com/in/jyass/?locale=en_US)** and **[Anasilvia Salazar](https://www.linkedin.com/in/anasilviasalazar/)** online lecturers at Universidad del Valle de Guatemala (UVG) and PhD students in Human-Computer Interaction at Iowa State University.

Inspired by mentoring a Data Science for Public Good team on 2025 and the realization that Guatemala (and Central America) had no equivalent to `tigris`, `tidycensus`, or `geobr`.

## License

MIT. Census data is public information from INE Guatemala. GADM boundary data is subject to [GADM's license](https://gadm.org/license.html) (free for academic/non-commercial use).
