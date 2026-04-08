# GeoQuetzal

🌐 [Read in English](README.md) | **Español** | [Sitio Web](https://geoquetzal.org/)

**Datos geográficos y censales de Guatemala.**

GeoQuetzal le da a investigadores guatemaltecos acceso programático a límites administrativos y microdatos censales, siguiendo la misma filosofía de [`tigris`](https://github.com/walkerke/tigris)/[`tidycensus`](https://walker-data.com/tidycensus/) para Estados Unidos y [`geobr`](https://github.com/ipeaGIT/geobr) para Brasil.

```python
import geoquetzal as gq

deptos = gq.departamentos()
deptos.plot(edgecolor="white", figsize=(8, 8))
```

## ¿Por qué GeoQuetzal?

Trabajar con datos geográficos y censales de Guatemala normalmente requiere descargar shapefiles de GADM, limpiar nombres con ortografía inconsistente, descargar CSVs del INE, descifrar cómo hacer joins y lidiar con el hecho de que GADM escribe "Quetzaltenango" como "Quezaltenango" y concatena "San Marcos" en "SanMarcos".

GeoQuetzal se encarga de todo eso. Una sola función le devuelve datos limpios, listos para analizar, con nombres consistentes del INE y códigos numéricos que hacen join de forma confiable.

## Instalación

```bash
pip install geoquetzal

# Con soporte de visualización (matplotlib + folium)
pip install geoquetzal[plotting]

# Todo incluido (agrega contextily para mapas base)
pip install geoquetzal[all]
```

**Requisitos:** Python 3.9+, geopandas, pandas, requests, pyarrow.

## Datasets

| Dataset | Registros | Variables | Almacenamiento | Fuente |
|---|---|---|---|---|
| Límites | 22 deptos / 340 municipios | geometría + códigos | Incluido (~1 MB) | MINFIN |
| Lagos | 2 lagos | geometría | Incluido | MINFIN |
| Emigración | 242,203 | 11 | GitHub (~1.6 MB) | INE Censo 2018 |
| Hogares | 3,275,931 | 37 | GitHub (~38 MB) | INE Censo 2018 |
| Viviendas | ~3,300,000 | 11 | GitHub (~30 MB) | INE Censo 2018 |
| Personas | 14,901,286 | 84 | GitHub (~333 MB) | INE Censo 2018 |
| Lugares Poblados | 20,254 | 200+ | GitHub | INE Censo 2018 |
| Voronoi Lugares Poblados | 20,254 | geometría | Calculado en línea | Derivado de centroides INE |

**Los límites y lagos están incluidos** en el paquete por lo que se cargan instantáneamente sin conexión a internet. Los microdatos censales están alojados como archivos Parquet en GitHub Releases y se descargan bajo demanda por departamento. Después de la primera descarga, los datos se cargan desde un caché local.

## Inicio Rápido

Todo está disponible bajo `import geoquetzal as gq`:

### Límites Administrativos

```python
import geoquetzal as gq

# Contorno del país
gq.country()

# Los 22 departamentos (carga instantánea — incluido en el paquete)
deptos = gq.departamentos()

# Por nombre o código (sin importar acentos)
gq.departamentos("Sacatepequez")     # sin acentos ✓
gq.departamentos("Sacatepéquez")     # ortografía exacta ✓
gq.departamentos(3)                  # código INE ✓

# Por región
gq.departamentos(region="V - Central")

# Municipios — los 340 con códigos INE
gq.municipios("Sacatepequez")                # todos los municipios de un departamento
gq.municipios(name="Antigua Guatemala")      # un municipio por nombre
gq.municipios(name=301)                      # un municipio por código

# Polígonos por zona de la Ciudad de Guatemala (usa GADM)
gq.municipios("Guatemala", zonas=True)       # 22 filas, una por zona

# Lagos para mapas más bonitos
ax = deptos.plot(color="lightyellow", edgecolor="gray")
gq.lagos().plot(ax=ax, color="lightblue", edgecolor="steelblue")
```

### Microdatos Censales

```python
import geoquetzal as gq

# Cargar todos los registros
df = gq.emigracion()                    # 242K registros de emigrantes
df = gq.hogares()                       # 3.2M hogares
df = gq.viviendas()                     # 3.3M unidades de vivienda
df = gq.personas()                      # 14.9M personas

# Filtrar por departamento (solo descarga el archivo de ese departamento)
df = gq.hogares(departamento="Huehuetenango")
df = gq.hogares(departamento=13)

# Filtrar por municipio
df = gq.hogares(municipio="Antigua Guatemala")
df = gq.hogares(municipio=301)
```

### Datos Sub-Municipales (Lugar Poblado)

```python
import geoquetzal as gq

# Indicadores pre-agregados para los 20,254 lugares poblados
df = gq.lugares_poblados()

# Filtrar por departamento o municipio
df = gq.lugares_poblados(departamento="Sacatepéquez")
df = gq.lugares_poblados(municipio="Antigua Guatemala")

# Como GeoDataFrame con geometría de puntos (centroides)
gdf = gq.lugares_poblados(geometry=True)

# Mapear acceso a internet a nivel sub-municipal
gdf["pct_internet"] = gdf["pch9_i_si"] / gdf["poblacion_total"]
gdf.plot(column="pct_internet", legend=True, markersize=5)
```

### Coropleta Sub-Municipal (Polígonos Voronoi)

El INE no publica los límites oficiales de los lugares poblados. GeoQuetzal
genera aproximaciones mediante teselación Voronoi a partir de los centroides,
recortados a los límites municipales. Son adecuados para visualización
coroplética, pero son aproximaciones, NO son límites oficiales.

```python
import geoquetzal as gq

# Generar polígonos Voronoi
vor = gq.voronoi_lugares_poblados(departamento="Sacatepéquez")

# Unir con datos censales y mapear
lp  = gq.lugares_poblados(departamento="Sacatepéquez")
gdf = vor.merge(lp, on=["departamento", "municipio", "lugar_poblado"])
gdf["pct_internet"] = gdf["pch9_i_si"] / gdf["poblacion_total"]
gdf.plot(column="pct_internet", cmap="YlGnBu", legend=True,
         edgecolor="white", linewidth=0.3)
```

### Explorar Variables

```python
import geoquetzal as gq

gq.describe_hogares()            # tabla resumen de las 37 variables
gq.describe_hogares("PCH4")      # fuente de agua — valores y etiquetas
gq.describe_hogares("PCH15")     # recibe remesas

gq.describe_emigracion("PEI3")   # sexo del emigrante
gq.describe_personas("PCP12")    # autoidentificación étnica
gq.describe_viviendas("PCV2")    # material de paredes

gq.describe_lugares_poblados()                  # todas las columnas
gq.describe_lugares_poblados("pcp12_maya")      # conteo Maya por lugar poblado
```

### Variables Destacadas

**Emigración**: sexo (`PEI3`), edad al emigrar (`PEI4`), año en que se fue (`PEI5`)

**Hogares**: fuente de agua (`PCH4`), saneamiento (`PCH5`), electricidad (`PCH8`), equipamiento — radio, TV, refrigeradora, internet, carro (`PCH9_A`–`PCH9_M`), combustible para cocinar (`PCH14`), remesas (`PCH15`)

**Viviendas**: tipo de vivienda (`PCV1`), material de paredes (`PCV2`), techo (`PCV3`), piso (`PCV5`)

**Personas**: sexo (`PCP6`), edad (`PCP7`), autoidentificación étnica (`PCP12` — Maya/Garífuna/Xinka/Ladino), comunidad lingüística maya (`PCP13`), lengua materna (`PCP15`), discapacidad (`PCP16_A`–`PCP16_F`), escolaridad (`PCP17_A`), alfabetismo (`PCP22`), acceso a tecnología — celular/computadora/internet (`PCP26_A`–`PCP26_C`), empleo (`PCP27`), estado civil (`PCP34`), fecundidad (`PCP35`–`PCP39`)

**Lugares Poblados**: conteos pre-agregados de todas las variables anteriores a nivel sub-municipal, más materiales de vivienda y servicios del hogar. 20,254 localidades con geometría de puntos (centroides).

## Patrones para Mapas

### Coropleta Estática (matplotlib)

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

### Mapa Interactivo (folium)

```python
result.explore(
    column="pct",
    tooltip=["municipio", "pct"],
    tiles="CartoDB positron",
)
```

### Coropleta Animada (Plotly)

```python
import geoquetzal as gq
import plotly.express as px
import json

deptos = gq.departamentos()
geojson = json.loads(deptos.to_json())
for f in geojson["features"]:
    f["id"] = f["properties"]["codigo_depto"]

fig = px.choropleth(
    agg_df,                         # sus datos agregados
    geojson=geojson,
    locations="codigo_depto",
    color="value",
    animation_frame="year",
)
fig.update_geos(fitbounds="locations", visible=False)
fig.show()
```

> **Regla clave:** Siempre agregue primero con pandas, luego haga merge de la geometría sobre las 22 o 340 filas resumen. Nunca use `geometry=` en microdatos grandes ya que agrega un polígono a cada fila y es muy lento.

## Uso con Sus Propios Datos

Cualquier dataset con códigos de municipio o departamento del INE funciona con los límites de GeoQuetzal:

```python
import geoquetzal as gq
import pandas as pd

mis_datos = pd.read_csv("mis_datos.csv")
munis = gq.municipios()
result = munis.merge(mis_datos, left_on="codigo_muni", right_on="su_columna_codigo")
result.plot(column="su_variable", cmap="YlGnBu", legend=True)
```

## Sistemas de Referencia de Coordenadas

```python
from geoquetzal.crs import to_gtm, to_utm16n, suggest_crs

deptos = gq.departamentos()
suggest_crs(deptos)              # muestra recomendaciones

deptos_gtm = to_gtm(deptos)     # Guatemala Transversa de Mercator (estándar nacional)
deptos_utm = to_utm16n(deptos)   # UTM Zona 16N (para cálculos de área/distancia)
```

| CRS | EPSG | Uso |
|---|---|---|
| WGS 84 | 4326 | Default, mapas web |
| Guatemala TM (GTM) | ESRI:103598 | Estándar nacional, mapas oficiales |
| UTM Zona 16N | 32616 | Cálculos de área y distancia |

## Cómo Funcionan los Datos

Los **límites administrativos** (departamentos, municipios, lagos) están incluidos en el paquete, provenientes de MINFIN (Ministerio de Finanzas Públicas de Guatemala). Se cargan instantáneamente sin necesidad de conexión a internet. Los 340 municipios tienen los códigos INE correctos incluidos.

Los **microdatos censales** están particionados por departamento en archivos Parquet y alojados en [GitHub Releases](https://github.com/geoquetzal/censo2018/releases). Cuando solicita un solo departamento, solo se descarga ese archivo (~1–15 MB). Solicitar todo Guatemala descarga los 22 archivos. Todo se guarda en caché después de la primera descarga.

Los **datos de lugares poblados** son un único archivo Parquet pre-agregado (20,254 localidades sub-municipales) que se descarga una vez y se almacena en caché. Contiene conteos y promedios derivados de las tres tablas censales — personas, hogares y viviendas.

Los **polígonos Voronoi** se calculan en línea a partir de los centroides de los lugares poblados, recortados a los límites municipales. Son aproximaciones para visualización — el INE no publica límites oficiales de lugares poblados. Los lugares poblados con coordenadas nulas (códigos terminados en `999`) son excluidos de la teselación.

Los **joins** entre datos censales y límites siempre usan códigos numéricos del INE (`codigo_depto`, `codigo_muni`), nunca nombres.

Las **zonas de la Ciudad de Guatemala** están disponibles con `gq.municipios("Guatemala", zonas=True)`, que usa GADM v4.1 para los 22 polígonos por zona. Los microdatos censales tienen una columna `ZONA` para hacer join.

## Fuentes de Datos y Atribución

- **Límites administrativos**: [MINFIN Guatemala](https://github.com/minfin-bi/Mapas-TopoJSON-Guatemala) — Ministerio de Finanzas Públicas, 340 municipios
- **Contorno nacional y zonas**: [GADM v4.1](https://gadm.org) — disponible gratuitamente para uso académico y no comercial
- **Microdatos censales**: [INE Guatemala](https://censo2018.ine.gob.gt/descarga) — XII Censo Nacional de Población y VII de Vivienda 2018 (datos públicos)
- **Archivos Parquet alojados**: [github.com/geoquetzal/censo2018](https://github.com/geoquetzal/censo2018/releases)

## Contribuir

GeoQuetzal es código abierto bajo la licencia MIT. Se aceptan contribuciones, especialmente nuevos datasets, documentación y notebooks de ejemplo.

```bash
git clone https://github.com/geoquetzal/geoquetzal.git
cd geoquetzal
pip install -e ".[dev,plotting]"
```

## Autor

Creado por **Jorge Yass** y **Anasilvia Salazar** docentes en la Universidad del Valle de Guatemala (UVG) y estudiantes de doctorado en Iowa State University.

Inspirado al acompañar a un equipo de Data Science for Public Good y darse cuenta de que Guatemala (y Centroamérica) no tenía un equivalente a `tigris`, `tidycensus` o `geobr`.

## Licencia

MIT. Los datos censales son información pública del INE Guatemala. Los datos de límites de GADM están sujetos a la [licencia de GADM](https://gadm.org/license.html) (gratuito para uso académico/no comercial).
