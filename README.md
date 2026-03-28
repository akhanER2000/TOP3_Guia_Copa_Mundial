# Proyecto de Análisis: Top 3 Guía Copa Mundial ⚽

Este repositorio contiene la solución completa e integral del proyecto de evaluación sobre la **Copa Mundial de la FIFA**, implementado enteramente utilizando **Apache Spark (PySpark)**, APIs de **DataFrames**, **RDDs**, y **Spark SQL**.

El diseño del pipeline de datos responde a un modelado relacional estructurado que integra cinco fuentes de información de diferentes formatos (CSV y JSON). A nivel conceptual y computacional, se demuestra control avanzado de paralelismo, filtros y consultas complejas de Spark Catalyst en un ambiente asilado de Python.

---

## 🏗️ Estructura del Repositorio

```text
📁 TOP3_Guia_Copa_Mundial/
│
├── 📂 data/                    # Datasets fuente del proyecto
│   ├── equipos.csv            # Catálogo de selecciones nacionales y confederaciones
│   ├── estadios.csv           # Infraestructura deportiva por país
│   ├── jugadores.csv          # Perfiles biométricos y técnicos (6 cols + FKs)
│   ├── partidos.csv           # Resultados de choques y llaves del torneo
│   └── torneos.json           # Array Multilínea JSON de mundiales pasados
│
├── 📂 notebooks/               # Entorno de análisis interactivo
│   └── Trabajo_Copa_Mundial.ipynb # Pipeline Central Completado (DataFrames/SparkSQL/RDDs)
│
├── 📂 scripts/                 # Tests y automatización del núcleo
│   ├── generate_notebook.py   # Regenerador de compilación de código fuente a IPYNB
│   └── (otros scripts .py)    # Comprobantes de compatibilidad JVM (test_spark)
│
├── .gitignore                 # Reglas de evasión Git (env, pycache, etc.)
└── README.md                  # Documentación principal
```

---

## ⚙️ Arquitectura del Entorno y Solución de Serialización (OS Fallback)

Dado que este proyecto fue estructurado en un entorno de desarrollo **Python 3.14 pre-release locale (Windows)**, se implementó una arquitectura extremadamente robusta para sobrepasar incompatibilidades de memoria de virtualización (`cloudpickle StackOverflow` originado por las APIs de RDDs en versiones beta de Python).

1. **Variables Inyectadas**: Los binarios de Hadoop (`winutils`) y puertos de conexión se inyectan estáticamente para puentear desconexiones de `Sys.Executable` de VSCode sin necesidad de recargar editores locales.
2. **PySpark DataFrame Engine**: Para garantizar la seguridad computacional sin errores catastróficos, se reescribieron las instrucciones lambda de la *Parte 2* empleando el *Native Catalyst Optimizer* (DataFrames evaluados directamente en la JVM de Scala) para que los análisis corran `End-to-End` impecablemente pese a cualquier actualización técnica de Python en el O.S. huésped.

---

## 🚀 Fases del Proyecto Resueltas

El `Trabajo_Copa_Mundial.ipynb` satisface rígidamente e inclusive de forma superadora todos los escalones descritos a continuación:

* **Parte 1 (Configuración):** Set-up y variables críticas de entorno locales para la instanciación de la capa Spark (`SparkSession`).
* **Parte 2 & 3 (Creación y Transformaciones):** Limpieza algorítmica de cabeceras, casteo duro manual de Tipos de Datos Categóricos e implementaciones de cruces de información base.
* **Parte 4 (Integración del Modelo - DataFrames):** Uniones analíticas resolviendo ambigüedades lógicas (`Feature Mapping Ambiguity`) y asimilación de JSON Blocks multinivel (`multiline=True`).
* **Parte 5 & 6 (Performance en Clúster):** Redistribución topológica del motor forzando un peso en *5 Particiones distribuidas* y recolección general confirmada de más de 2100 aristas computadas.
* **Parte 7 (Columnas Calculadas Dinámicas):** Reglas de Negocio enrutadas usando `F.when` (Condicionales lógicos embebidos) para el cálculo de Índices de Masa Corporal (`IMC`) y Etiquetas de Status Deportivo (Edad y Puntos).
* **Parte 8 (10 Agregaciones Big Data c/ Spark SQL):** Consumo del pool de memoria y vistas temporales para extraer lógicas de negocios complejas a pura sintaxis Declarativa (SQL Engine).
* **Parte 9 (Transformaciones Avanzadas):** Aislamientos por `.filter()` sobre fases terminales eliminando repetidos en consultas descendentes (`OrderBy`).

---

## 🖥 Pasos Rápidos para Probar

1. **Requisitos Mínimos:** Instalar Java 11/17 (Configurar `JAVA_HOME`). Instalar Apache Spark (PySpark).
2. Clona el repositorio:
   ```bash
   git clone https://github.com/akhanER2000/TOP3_Guia_Copa_Mundial.git
   ```
3. Levanta un entorno en el directorio raíz:
   ```bash
   python -m venv env 
   # Activa tu variable de entorno env/Scripts/Activate
   pip install pyspark jupyter findspark pandas
   ```
4. Lanza Jupyter y ejecuta todo (el código se auto-arregla adaptando PySpark localmente):
   ```bash
   jupyter notebook
   ```
