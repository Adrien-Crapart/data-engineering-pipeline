# Guide Complet — OpenMetadata 1.12.3 pour Weather Data Pipeline

> Ce guide détaille toutes les étapes pour configurer et exploiter **OpenMetadata 1.12.3** avec
> les données OpenWeather du projet. Il couvre la découverte, le lineage, le profiling,
> la gouvernance, les tests, les glossaires, les alertes, et bien plus.
>
> **Version des composants** (vérifiées le 2026-04-02) :
>
> | Composant | Version | Image Docker |
> |---|---|---|
> | OpenMetadata Server | 1.12.3 | `docker.getcollate.io/openmetadata/server:1.12.3` |
> | OpenMetadata Ingestion | 1.12.3 | `docker.getcollate.io/openmetadata/ingestion:1.12.3` |
> | Elasticsearch | 9.0.2 | `docker.elastic.co/elasticsearch/elasticsearch:9.0.2` |
> | PostgreSQL | 16 | `postgres:16-alpine` |
> | Airflow | 3.x | Custom build |
> | MinIO | RELEASE.2025-09-07 | `minio/minio:RELEASE.2025-09-07T16-13-09Z` |
> | Kafka | 4.0.0 | `apache/kafka:4.0.0` |
> | Grafana | 12.4.0 | `grafana/grafana:12.4.0` |
> | Prometheus | 3.2.1 | `prom/prometheus:v3.2.1` |
>
> **Documentation officielle** : https://docs.open-metadata.org/v1.12.x

---

## Table des matières

1. [Vue d'ensemble de l'architecture](#1-vue-densemble-de-larchitecture)
2. [Démarrage du stack OpenMetadata](#2-démarrage-du-stack-openmetadata)
3. [Configuration des services (connecteurs)](#3-configuration-des-services-connecteurs)
   - 3.1 PostgreSQL — Datawarehouse
   - 3.2 MinIO S3 — Data Lake
   - 3.3 Airflow — Orchestrateur
4. [Agents d'ingestion (Metadata, Profiler, dbt)](#4-agents-dingestion-metadata-profiler-dbt)
   - 4.1 Metadata Agent (tables, colonnes, schémas)
   - 4.2 Profiler Agent (statistiques, distributions)
   - 4.3 Data Quality / Tests
   - 4.4 dbt Agent (lineage, descriptions, tags)
5. [Lineage — Traçabilité de bout en bout](#5-lineage--traçabilité-de-bout-en-bout)
6. [Domains — Organisation par domaine métier](#6-domains--organisation-par-domaine-métier)
7. [Glossaire métier](#7-glossaire-métier)
8. [Tags et classification](#8-tags-et-classification)
9. [Data Quality — Tests natifs OpenMetadata](#9-data-quality--tests-natifs-openmetadata)
10. [Gouvernance — Ownership et Tiers](#10-gouvernance--ownership-et-tiers)
11. [Alertes et notifications](#11-alertes-et-notifications)
12. [Custom Properties (propriétés personnalisées)](#12-custom-properties)
13. [Policies et rôles](#13-policies-et-rôles)
14. [KPIs de données](#14-kpis-de-données)
15. [Activités et collaboration](#15-activités-et-collaboration)
16. [API REST — Automatisation avancée](#16-api-rest--automatisation-avancée)
17. [Checklist récapitulative](#17-checklist-récapitulative)

---

## 1. Vue d'ensemble de l'architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    OpenMetadata Stack                        │
│                                                              │
│  ┌─────────────┐  ┌──────────────────┐  ┌────────────────┐  │
│  │ Elasticsearch│  │ OpenMetadata     │  │ OM Ingestion   │  │
│  │  (Search)    │  │ Server (API+UI)  │  │ (Airflow)      │  │
│  │  :9200       │  │  :8585           │  │  :8086         │  │
│  └─────────────┘  └────────┬─────────┘  └───────┬────────┘  │
│                            │                     │           │
└────────────────────────────┼─────────────────────┼───────────┘
                             │                     │
           ┌─────────────────┼─────────────────────┼──────────┐
           │                 ▼                     ▼          │
           │  ┌────────────────────────────────────────────┐  │
           │  │           Pipeline Network                  │  │
           │  │                                            │  │
           │  │  PostgreSQL ◄── dbt ◄── DLT ◄── API       │  │
           │  │  (datawarehouse)    (S3 Parquet)           │  │
           │  │     │                    │                  │  │
           │  │     ├─ core.*            ├─ raw/openweather │  │
           │  │     ├─ mart.*            MinIO :9000        │  │
           │  │     ├─ analytic.*                           │  │
           │  │     └─ staging.* (DuckDB in-memory)        │  │
           │  │                                            │  │
           │  │  Airflow :8080  ─── DAGs Pipeline          │  │
           │  └────────────────────────────────────────────┘  │
           └──────────────────────────────────────────────────┘
```

**Schémas PostgreSQL disponibles** :

| Schéma | Couche | Tables | Description |
|--------|--------|--------|-------------|
| `core` | Gold | `dim_city`, `fct_weather_observation` | Dimension ville + Fait observations |
| `mart` | Gold | `weather_daily_summary`, `city_weather_metrics` | Agrégats métier |
| `analytic` | Gold | `weather_trend_analysis`, `city_comparison_ranking` | Analyses avancées |
| `staging` | Silver | *(DuckDB in-memory, non visible dans PG)* | Staging DuckDB |

---

## 2. Démarrage du stack OpenMetadata

### Prérequis

Le stack principal (PostgreSQL, MinIO, Airflow) doit être up :

```bash
just start
```

### Lancer OpenMetadata

```bash
just start-full
```

Cela lance : `openmetadata-elasticsearch`, `execute-migrate-all` (migration BDD),
`openmetadata-server`, `openmetadata-ingestion`.

### Vérifier la santé

```bash
docker compose -f infrastructure/docker/compose.data-warehouse.yaml -f infrastructure/docker/compose.metadata.yaml ps
```

### Accès

| Service | URL | Credentials |
|---------|-----|-------------|
| **OpenMetadata UI** | http://localhost:8585 | `admin@open-metadata.org` / `admin` |
| **OM Ingestion Airflow** | http://localhost:8086 | `admin` / `admin` |

### Premier login

1. Ouvrir http://localhost:8585
2. Se connecter avec `admin@open-metadata.org` / `admin`
3. Compléter le wizard de bienvenue (nom d'organisation, etc.)

---

## 3. Configuration des services (connecteurs)

### 3.1 PostgreSQL — Datawarehouse

**Via l'UI** : Settings → Services → Databases → Add New Service

| Champ | Valeur |
|-------|--------|
| Service Name | `datawarehouse` |
| Service Type | PostgreSQL |
| Host and Port | `postgres:5432` |
| Username | `datawarehouse_user` |
| Password | `datawarehouse_password` |
| Database | `datawarehouse` |

**Test de connexion** : Cliquer "Test Connection" — doit valider les 5 checks (service, schemas, tables, views, queries).

**Via l'API** (déjà dans le DAG `openmetadata_ingestion`) :

```json
{
  "name": "datawarehouse",
  "serviceType": "Postgres",
  "connection": {
    "config": {
      "type": "Postgres",
      "scheme": "postgresql+psycopg2",
      "hostPort": "postgres:5432",
      "username": "datawarehouse_user",
      "authType": {"password": "datawarehouse_password"},
      "database": "datawarehouse"
    }
  }
}
```

### 3.2 MinIO S3 — Data Lake

**Via l'UI** : Settings → Services → Storage → Add New Service

| Champ | Valeur |
|-------|--------|
| Service Name | `minio-data-lake` |
| Service Type | S3 |
| AWS Access Key ID | `minioadmin` |
| AWS Secret Access Key | `minioadmin` |
| AWS Region | `us-east-1` |
| Endpoint URL | `http://minio:9000` |

### 3.3 Airflow — Orchestrateur

> **Enregistré automatiquement** par `just om-setup` (via `om_setup_full.py`).
> Peut aussi être configuré manuellement via l'UI.

**Via l'UI** : Settings → Services → Pipelines → Add New Service

| Champ | Valeur |
|-------|--------|
| Service Name | `airflow` |
| Service Type | Airflow |
| Host and Port | `http://airflow-webserver:8080` |
| Connection | Basic Auth (`airflow` / `airflow`) |

> **Note** : L'intégration Airflow pipeline permet de voir les DAGs comme des pipelines
> dans OpenMetadata et de tracer quels DAGs alimentent quelles tables.

---

## 4. Agents d'ingestion (Metadata, Profiler, dbt)

> **Terminologie OM 1.12.x** : Dans OpenMetadata 1.12, les "Ingestion Pipelines" ont été
> renommés **"Agents"**. L'onglet dans la page Service s'appelle **"Agents"** (et non plus
> "Ingestion"). Le bouton pour ajouter un pipeline s'appelle **"Add Agent"**.
> Ref: https://docs.open-metadata.org/v1.12.x/how-to-guides/admin-guide/how-to-ingest-metadata

Chaque type d'agent se configure depuis la page Service, onglet **Agents**.

### 4.1 Metadata Agent (tables, colonnes, schémas)

**Chemin UI** :
1. Aller dans **Settings** (engrenage en bas à gauche) → **Services** → **Databases**
2. Cliquer sur le service **`datawarehouse`**
3. Onglet **Agents** → **Add Metadata Agent**

**Configuration** :

| Paramètre | Valeur |
|-----------|--------|
| Name | `datawarehouse_metadata` (auto-généré) |
| Database Filter Pattern | *(laisser vide — une seule base)* |
| Schema Filter Pattern (includes) | `core`, `mart`, `analytic`, `staging`, `staging_quarantine` |
| Include Tables | ✅ |
| Include Views | ✅ |
| Mark Deleted Tables | ✅ |
| Enable Debug Log | ✅ (recommandé) |
| Schedule | Quotidien — `0 4 * * *` (ou "Run Now" pour un premier lancement) |

> **Astuce** : Cliquez d'abord **"Run Now"** pour un premier lancement immédiat,
> puis configurez le schedule pour les exécutions suivantes.

**Ce que ça fait** :
- Découvre toutes les tables dans les schémas `core`, `mart`, `analytic`, `staging`, `staging_quarantine`
- Importe les noms de colonnes, types de données, clés primaires
- Détecte les nouvelles tables et marque les supprimées
- Crée l'arbre : Service → Database → Schema → Table → Column

**Tables découvertes** :

```
datawarehouse
  └── datawarehouse (database)
        ├── staging
        │     ├── stg_weather_current
        │     └── stg_weather_forecast
        ├── staging_quarantine
        │     ├── quarantine_weather_current
        │     └── quarantine_weather_forecast
        ├── core
        │     ├── dim_city (city_name, country_code, latitude, longitude)
        │     └── fct_weather_observation (row_id, city_name, temperature_celsius, ...)
        ├── mart
        │     ├── weather_daily_summary (city_name, date_day, avg_temperature_celsius, ...)
        │     └── city_weather_metrics (city_name, total_observations, ...)
        └── analytic
              ├── weather_trend_analysis (city_name, date_day, temp_7d_moving_avg, ...)
              └── city_comparison_ranking (city_name, temp_rank_warmest, ...)
```

**Vérification après exécution** :
- Retourner sur l'onglet **Agents** : le pipeline doit afficher un statut **Success** (vert)
- Aller dans l'onglet **Databases** : la base `datawarehouse` doit apparaître avec ses schémas
- Aller dans **Explore** (menu latéral) : les tables doivent être listées et cherchables

### 4.2 Profiler Agent (statistiques, distributions)

**Chemin UI** :
1. Toujours sur la page du service **`datawarehouse`**
2. Onglet **Agents** → **Add Profiler Agent**

> **Prérequis** : Le Metadata Agent doit avoir été exécuté avec succès au moins une fois.
> Les tables doivent être visibles dans l'UI avant de lancer le profiler.

**Configuration** :

| Paramètre | Valeur |
|-----------|--------|
| Name | `datawarehouse_profiler` (auto-généré) |
| Schema Filter Pattern (includes) | `core`, `mart`, `analytic` |
| Profile Sample | `Percentage` → `100` (petites tables, OK à 100%) |
| Generate Sample Data | ✅ (10 lignes preview) |
| Compute Metrics | ✅ |
| Include Views | ON |
| Enable Debug Log | ✅ |
| Schedule | Quotidien — `0 5 * * *` |

**Ce que ça fait** :
- Pour chaque colonne, calcule : min, max, mean, median, stddev, null count, unique count, distribution
- Génère des échantillons de données visibles dans l'UI (onglet **Sample Data** de chaque table)
- Détecte les colonnes PII automatiquement (email, nom, etc.)
- Historique des profils pour voir l'évolution dans le temps

> **Terminologie OM 1.12.3** : Les onglets de table sont :
> `Columns` | `Activity Feeds & Tasks` | `Sample Data` | `Queries` |
> **`Data Observability`** | `Lineage` | `Contract` | `Custom Properties`
>
> L'onglet **Data Observability** contient les sous-onglets :
> **Table Profile** | **Column Profile** | **Data Quality** | **Incidents**

**Métriques attendues par table** :

| Table | Exemples de métriques attendues |
|-------|-------------------------------|
| `fct_weather_observation` | `temperature_celsius`: mean ≈ 15°C, min ≈ -10, max ≈ 45 |
| `dim_city` | `city_name`: 100% unique, 0% null |
| `weather_daily_summary` | `observation_count`: min = 1, mean ≈ 8 |
| `city_weather_metrics` | `total_observations`: varies per city |
| `weather_trend_analysis` | `temp_7d_moving_avg`: smooth distribution |
| `city_comparison_ranking` | `temp_rank_warmest`: 1 to N cities |

**Vérification** :
- Aller sur une table (ex: `fct_weather_observation`) → onglet **Data Observability**
- Sous-onglet **Table Profile** : Row Count, Column Count, Size, Profile Sample doivent être visibles
- Sous-onglet **Column Profile** : statistiques par colonne (histogrammes, min/max/mean, null%, unique%)
- Onglet **Sample Data** : 10 lignes d'exemple doivent apparaître

> **Si Sample Data est vide** : Relancez le Profiler Agent en vous assurant que
> l'option **"Generate Sample Data"** (ou "Ingest Sample Data") est activée.
> Alternativement, lancez un agent **Auto Classification** depuis la page Service
> (onglet Agents → Add Auto Classification Agent) pour peupler Sample Data + détecter les PII.

### 4.3 Data Quality / Tests via ingestion

Les tests dbt (définis dans `_staging__models.yml` et `_marts__models.yml`) sont
automatiquement importés via l'agent dbt. Voir section 8 pour les tests natifs OM.

### 4.4 dbt Agent (lineage, descriptions, tags)

**Chemin UI** :
1. Toujours sur la page du service **`datawarehouse`**
2. Onglet **Agents** → **Add dbt Agent**

> **Prérequis** : Les artefacts dbt (`manifest.json`, `catalog.json`) doivent être
> présents sur S3 dans `data-lake/_reports/dbt_docs/latest/`. Si ce n'est pas le cas,
> lancez d'abord la transformation pipeline Airflow ou manuellement :
> ```bash
> docker run --rm --network data-engineering-pipeline_pipeline-network \
>   weather-pipeline-dbt:1.0.0 docs
> ```

**Configuration** :

| Paramètre | Valeur |
|-----------|--------|
| Name | `dbt_lineage` (auto-généré) |
| dbt Config Source | **S3** |
| S3 Bucket | `data-lake` |
| S3 Object Prefix | `_reports/dbt_docs/latest` |
| AWS Access Key | `minioadmin` |
| AWS Secret Key | `minioadmin` |
| AWS Region | `us-east-1` |
| Endpoint URL | `http://minio:9000` |
| dbt Update Descriptions | ✅ |
| Include dbt Tags | ✅ |
| Schedule | Quotidien — `0 6 * * *` |

**Ce que ça fait** :
- Lit `manifest.json` et `catalog.json` de dbt depuis S3
- Crée le lineage : `stg_weather_current` → `fct_weather_observation` → `weather_daily_summary`
- Importe les descriptions dbt dans les descriptions des tables/colonnes OM
- Importe les tests dbt comme tests de qualité dans OM
- Importe les tags dbt (`staging`, `weather`, `mart`)

**Lineage attendu après import** :

```
S3 Parquet (raw)
    │
    ▼
stg_weather_current ─────────┬──→ dim_city
    │                        │
    ▼                        ▼
fct_weather_observation    stg_weather_forecast
    │                        │
    ▼                        ▼
weather_daily_summary ◄──────┘
    │
    ├──→ weather_trend_analysis
    │
    ▼
city_weather_metrics
    │
    ▼
city_comparison_ranking
```

---

### 4.5 Vérification globale des Agents

Après exécution de tous les agents, la page Service `datawarehouse` → onglet **Agents** doit afficher :

| Agent | Type | Statut | Schedule |
|-------|------|--------|----------|
| `datawarehouse_metadata_...` | Metadata | ✅ Success | `0 4 * * *` |
| `datawarehouse_profiler_...` | Profiler | ✅ Success | `0 5 * * *` |
| `dbt_lineage_...` | dbt | ✅ Success | `0 6 * * *` |

**Actions disponibles sur chaque agent** (au survol ou clic droit) :
- **Run** : lancer immédiatement
- **Kill** : arrêter les exécutions en cours
- **Redeploy** : redéployer après modification de la connexion du service

---

## 5. Lineage — Traçabilité de bout en bout

### 5.1 Lineage automatique (dbt)

Après l'ingestion dbt (section 4.4), le lineage est automatiquement créé.

**Pour le visualiser** : Cliquer sur n'importe quelle table → onglet "Lineage"

### 5.2 Lineage manuel — Ajouter la source API

Le lineage automatique ne capture pas la source API OpenWeather. On peut l'ajouter manuellement.

**Étapes** :

1. Aller sur la table `fct_weather_observation`
2. Onglet "Lineage"
3. Cliquer "Edit" (icône crayon)
4. "Add Node" → Chercher le topic/dashboard source
5. Ou utiliser l'API :

```json
POST /api/v1/lineage
{
  "edge": {
    "fromEntity": {
      "id": "<id-stg-weather-current>",
      "type": "table"
    },
    "toEntity": {
      "id": "<id-fct-weather-observation>",
      "type": "table"
    },
    "lineageDetails": {
      "description": "dbt transformation: staging to core fact table",
      "source": "dbt"
    }
  }
}
```

### 5.3 Column-Level Lineage

OpenMetadata supporte le lineage au niveau colonne. Après l'ingestion dbt, vérifier :

1. Aller sur `fct_weather_observation` → Lineage
2. Cliquer sur une colonne (ex: `temperature_celsius`)
3. Voir qu'elle provient de `stg_weather_current.temperature_celsius`
4. Qui provient de `main__temp` dans le Parquet brut

**Enrichir manuellement si nécessaire** :

| Colonne destination | Source | Transformation |
|--------------------|--------|----------------|
| `fct_weather_observation.temperature_celsius` | `stg_weather_current.temperature_celsius` | Direct mapping |
| `weather_daily_summary.avg_temperature_celsius` | `fct_weather_observation.temperature_celsius` | `AVG()` aggregation |
| `city_comparison_ranking.temp_rank_warmest` | `city_weather_metrics.avg_temperature_celsius` | `RANK() OVER()` |

---

## 6. Domains — Organisation par domaine métier

Les **Domains** sont une fonctionnalité clé d'OM 1.12.x pour organiser vos data assets
selon la logique métier. Ils permettent de filtrer, gouverner et naviguer par domaine.

### 6.1 Créer les Domains

**Chemin UI** : **Domains** (menu latéral gauche) → **Add Domain**

| Domain | Display Name | Description | Type |
|--------|-------------|-------------|------|
| `weather-observations` | Weather Observations | Données d'observations météo en temps réel — couches Staging et Core (Silver/Gold) | `Source-aligned` |
| `weather-analytics` | Weather Analytics | Analyses et agrégats dérivés des observations — couches Mart et Analytic (Gold) | `Consumer-aligned` |
| `data-platform` | Data Platform | Infrastructure, pipelines et services de données (Airflow, DLT, MinIO) | `Source-aligned` |

**Types de Domain** :
- **Source-aligned** : organisé autour de la source de données (producteur)
- **Consumer-aligned** : organisé autour du consommateur (BI, API, analytics)

### 6.2 Assigner les Data Assets aux Domains

Après l'ingestion metadata, assignez chaque table à son domain :

1. Aller sur la table (ex: `fct_weather_observation`)
2. En haut de la page, cliquer sur **"Domains"** → sélectionner le domain

| Table | Domain |
|-------|--------|
| `stg_weather_current` | Weather Observations |
| `stg_weather_forecast` | Weather Observations |
| `quarantine_weather_current` | Weather Observations |
| `quarantine_weather_forecast` | Weather Observations |
| `dim_city` | Weather Observations |
| `fct_weather_observation` | Weather Observations |
| `weather_daily_summary` | Weather Analytics |
| `city_weather_metrics` | Weather Analytics |
| `weather_trend_analysis` | Weather Analytics |
| `city_comparison_ranking` | Weather Analytics |

### 6.3 Data Products (optionnel, avancé)

Les Data Products sont des ensembles de data assets regroupés sous un Domain
pour une consommation structurée.

**Chemin UI** : **Domains** → `weather-analytics` → **Add Data Product**

| Data Product | Description | Assets inclus |
|-------------|-------------|---------------|
| `daily-weather-report` | Résumé météo quotidien pour dashboards BI | `weather_daily_summary`, `city_weather_metrics` |
| `weather-trends` | Analyses de tendances et comparaisons | `weather_trend_analysis`, `city_comparison_ranking` |

### 6.4 Filtrage par Domain dans Explore

Une fois les domains configurés :
1. Aller dans **Explore** (menu latéral)
2. Utiliser le filtre **"All Domains"** (en haut de la page) pour sélectionner un domain
3. Seuls les assets du domain sélectionné apparaissent

---

## 7. Glossaire métier

Le glossaire donne un vocabulaire métier partagé, indépendant de l'implémentation technique.

### 7.1 Créer le glossaire

**Chemin UI** : Govern → Glossary → Add Glossary

| Champ | Valeur |
|-------|--------|
| Name | `Weather Data Glossary` |
| Description | Vocabulaire métier pour les données météorologiques OpenWeather |
| Owner | `admin` |
| Reviewers | `admin` |

### 7.2 Termes à créer

Ajouter ces termes via "Add Term" dans le glossaire :

#### Catégorie : Mesures atmosphériques

| Terme | Définition | Synonymes | Tables liées |
|-------|-----------|-----------|-------------|
| **Temperature** | Mesure de la chaleur de l'air en degrés Celsius à 2m du sol. Source: station météo ou estimation satellite. | temp, température | `fct_weather_observation.temperature_celsius` |
| **Feels Like Temperature** | Température ressentie tenant compte du vent et de l'humidité (windchill / heat index). | température ressentie, apparent temp | `fct_weather_observation.feels_like_celsius` |
| **Humidity** | Taux d'humidité relative de l'air en pourcentage (0-100%). | humidité, moisture | `fct_weather_observation.humidity_percent` |
| **Atmospheric Pressure** | Pression atmosphérique au niveau de la mer en hectopascals (hPa). | pression, barometric pressure | `fct_weather_observation.pressure_hpa` |
| **Wind Speed** | Vitesse du vent en mètres par seconde (m/s) mesurée à 10m du sol. | vent, wind velocity | `fct_weather_observation.wind_speed_ms` |
| **Cloud Coverage** | Pourcentage de couverture nuageuse (0% = ciel dégagé, 100% = couvert). | nébulosité, cloudiness | `fct_weather_observation.cloud_coverage_percent` |
| **Visibility** | Distance de visibilité horizontale en mètres. Max 10000m dans l'API OpenWeather. | visibilité | `fct_weather_observation.visibility_meters` |

#### Catégorie : Conditions météo

| Terme | Définition | Valeurs possibles |
|-------|-----------|-------------------|
| **Weather Condition** | Classification principale de la météo par OpenWeather. | Clear, Clouds, Rain, Drizzle, Thunderstorm, Snow, Mist, Fog, Haze |
| **Precipitation Probability** | Probabilité de précipitation sur la période de prévision (0 à 1). | `stg_weather_forecast.precipitation_probability` |

#### Catégorie : Métriques calculées

| Terme | Définition | Formule |
|-------|-----------|---------|
| **7-Day Moving Average** | Moyenne glissante sur 7 jours pour lisser les variations quotidiennes. | `AVG(metric) OVER (ROWS 6 PRECEDING AND CURRENT ROW)` |
| **Day-over-Day Change** | Variation de la température par rapport à la veille. | `temperature[t] - temperature[t-1]` |
| **Dominant Weather Condition** | Condition météo la plus fréquente sur une journée. | `MODE(weather_condition)` |
| **Temperature vs Mean** | Écart de température moyenne d'une ville par rapport à la moyenne globale. | `city_avg - global_avg` |

#### Catégorie : Entités géographiques

| Terme | Définition |
|-------|-----------|
| **City** | Ville pour laquelle des données météo sont collectées. Identifiée par `city_name`. |
| **Country Code** | Code pays ISO 3166-1 alpha-2 (ex: FR, US, JP). |
| **Coordinates** | Couple (latitude, longitude) WGS84 de la ville. |

### 7.3 Lier les termes aux colonnes

Pour chaque terme, l'associer aux colonnes correspondantes :

1. Aller sur la table (ex: `fct_weather_observation`)
2. Cliquer sur la colonne `temperature_celsius`
3. "Add Glossary Term" → sélectionner "Temperature"
4. Répéter pour chaque colonne

> **Astuce** : Une fois un terme lié, il apparaît dans la recherche
> et permet de trouver toutes les colonnes liées à "Temperature" en un clic.

---

## 8. Tags et classification

### 8.1 Système de classification

**Chemin UI** : Govern → Tags → Add Classification

#### Classification : `Data Layer`

| Tag | Description | Couleur |
|-----|-------------|---------|
| `Bronze` | Données brutes non transformées (S3 Parquet) | 🟤 Marron |
| `Silver` | Données nettoyées et typées (staging DuckDB) | ⚪ Gris |
| `Gold` | Données métier prêtes à consommer (core/mart PG) | 🟡 Jaune |

#### Classification : `Data Domain`

| Tag | Description |
|-----|-------------|
| `Meteorology` | Données liées aux conditions atmosphériques |
| `Geography` | Données de localisation et référentiels de villes |
| `Analytics` | Métriques calculées et analyses dérivées |

#### Classification : `Data Sensitivity`

| Tag | Description |
|-----|-------------|
| `Public` | Données ouvertes, peuvent être partagées sans restriction |
| `Internal` | Données internes à l'organisation |
| `PII` | Données personnelles identifiables (si applicable) |

#### Classification : `Update Frequency`

| Tag | Description |
|-----|-------------|
| `Real-time` | Mise à jour toutes les 10 minutes |
| `Hourly` | Mise à jour toutes les heures |
| `Daily` | Mise à jour quotidienne |
| `Static` | Données de référence rarement modifiées |

### 8.2 Appliquer les tags aux tables

| Table | Data Layer | Domain | Sensitivity | Update Freq |
|-------|-----------|--------|-------------|-------------|
| `dim_city` | Gold | Geography | Public | Static |
| `fct_weather_observation` | Gold | Meteorology | Public | Hourly |
| `weather_daily_summary` | Gold | Meteorology | Public | Daily |
| `city_weather_metrics` | Gold | Analytics | Public | Daily |
| `weather_trend_analysis` | Gold | Analytics | Public | Daily |
| `city_comparison_ranking` | Gold | Analytics | Public | Daily |

**Pour appliquer** :

1. Aller sur chaque table
2. Cliquer "Add Tag" à droite du nom
3. Sélectionner la classification et le tag

---

## 9. Data Quality — Tests natifs OpenMetadata

En plus des tests dbt et Soda déjà en place, OpenMetadata propose ses propres tests.

### 9.1 Tests existants (importés de dbt)

Après l'ingestion dbt, ces tests apparaissent dans OM :

| Table | Test | Source |
|-------|------|--------|
| `stg_weather_current` | `not_null(city_name)` | dbt |
| `stg_weather_current` | `not_null(temperature_celsius)` | dbt |
| `stg_weather_current` | `not_null(measured_at)` | dbt |
| `city_weather_metrics` | `not_null(city_name)` | dbt |
| `city_weather_metrics` | `unique(city_name)` | dbt |

### 9.2 Tests natifs OpenMetadata (automatisés via `om_setup_full.py`)

> **Ces tests sont créés automatiquement** par `just om-setup` (qui exécute `metadata/om_setup_full.py`).
> Vous pouvez aussi les ajouter manuellement depuis l'UI.
>
> **Chemin UI** : Table → onglet **Data Observability** → sous-onglet **Data Quality** → bouton **Add**

#### Tests sur `fct_weather_observation`

| Test | Type | Configuration | Seuil |
|------|------|---------------|-------|
| **Table Row Count** | `tableRowCountToBeGreaterThan` | minValue: `0` | Fail si 0 lignes |
| **Temperature Range** | `columnValuesToBeBetween` | column: `temperature_celsius`, min: `-90`, max: `60` | Fail si hors range |
| **Humidity Range** | `columnValuesToBeBetween` | column: `humidity_percent`, min: `0`, max: `100` | Fail si > 100% |
| **Pressure Range** | `columnValuesToBeBetween` | column: `pressure_hpa`, min: `870`, max: `1085` | Records extremes |
| **Wind Speed** | `columnValuesToBeBetween` | column: `wind_speed_ms`, min: `0`, max: `120` | Vitesse max ouragan |
| **No Null Cities** | `columnValuesToBeNotNull` | column: `city_name` | 0% null |
| **No Null Timestamps** | `columnValuesToBeNotNull` | column: `measured_at` | 0% null |
| **Freshness** | `tableRowInsertedCountToBeGreaterThan` | column: `measured_at`, lookback: `2 days` | Data < 2j |
| **No Future Dates** | `columnValuesToBeBetween` | column: `measured_at`, max: `CURRENT_TIMESTAMP` | Pas de dates futures |

#### Tests sur `dim_city`

| Test | Type | Configuration |
|------|------|---------------|
| **Unique Cities** | `columnValuesToBeUnique` | column: `city_name` |
| **Valid Country Code** | `columnValueLengthsToBeBetween` | column: `country_code`, min: `2`, max: `2` |
| **Valid Latitude** | `columnValuesToBeBetween` | column: `latitude`, min: `-90`, max: `90` |
| **Valid Longitude** | `columnValuesToBeBetween` | column: `longitude`, min: `-180`, max: `180` |

#### Tests sur `weather_daily_summary`

| Test | Type | Configuration |
|------|------|---------------|
| **Positive Observations** | `columnValuesToBeBetween` | column: `observation_count`, min: `1` |
| **Temperature Coherence** | `customSQLQuery` | `SELECT COUNT(*) FROM weather_daily_summary WHERE min_temperature_celsius > max_temperature_celsius` → result = 0 |
| **No Missing Days** | custom SQL | Détecter les jours manquants dans la séquence |

#### Tests sur `city_weather_metrics`

| Test | Type | Configuration |
|------|------|---------------|
| **Unique Cities** | `columnValuesToBeUnique` | column: `city_name` |
| **Temperature Consistency** | custom SQL | `min_temperature <= avg_temperature <= max_temperature` |

### 9.3 Custom SQL Tests — Exemples avancés

**Chemin UI** : Table → **Data Observability** → **Data Quality** → **Add** → **Custom SQL Query**

**Test 1 — Cohérence température min/max journalière** :

```sql
SELECT COUNT(*)
FROM mart.weather_daily_summary
WHERE min_temperature_celsius > max_temperature_celsius
```

Résultat attendu : `0`

**Test 2 — Détection de doublons d'observations** :

```sql
SELECT COUNT(*) FROM (
    SELECT city_name, measured_at, COUNT(*)
    FROM core.fct_weather_observation
    GROUP BY city_name, measured_at
    HAVING COUNT(*) > 1
) duplicates
```

Résultat attendu : `0`

**Test 3 — Complétude des villes** :

```sql
SELECT COUNT(DISTINCT o.city_name) - COUNT(DISTINCT d.city_name)
FROM core.fct_weather_observation o
LEFT JOIN core.dim_city d ON o.city_name = d.city_name
```

Résultat attendu : `0` (toutes les villes observées sont dans la dimension)

**Test 4 — Fraîcheur des prévisions** :

```sql
SELECT CASE
    WHEN MAX(date_day) >= CURRENT_DATE - INTERVAL '2 days' THEN 0
    ELSE 1
END
FROM mart.weather_daily_summary
```

Résultat attendu : `0`

### 9.4 Planifier les tests

1. Table → **Data Observability** → **Data Quality** → Settings (icône engrenage)
2. Scheduler : `0 7 * * *` (quotidien à 7h, après ingestion + transformation)
3. Activer les notifications en cas de fail (voir section 10)

---

## 10. Gouvernance — Ownership et Tiers

### 10.1 Créer des équipes

**Chemin UI** : Settings → Team & User Management → Teams

| Équipe | Description | Rôle |
|--------|-------------|------|
| `data-engineering` | Equipe responsable des pipelines d'ingestion et transformation | Build & maintain |
| `data-analytics` | Equipe consommatrice des données mart et analytic | Consume & report |
| `data-governance` | Equipe responsable de la qualité et conformité | Review & approve |
| `weather-operations` | Equipe métier météo | Domain expert |

### 10.2 Créer des utilisateurs (fake pour la démo)

**Chemin UI** : Settings → Team & User Management → Users

| Utilisateur | Email | Équipe | Rôle |
|------------|-------|--------|------|
| Marie Dupont | `marie.dupont@weather-corp.com` | data-engineering | Data Engineer Owner |
| Pierre Martin | `pierre.martin@weather-corp.com` | data-analytics | Data Analyst |
| Sophie Leroy | `sophie.leroy@weather-corp.com` | data-governance | Data Steward |
| Jean-Paul Meteo | `jp.meteo@weather-corp.com` | weather-operations | Domain Expert |

### 10.3 Assigner l'ownership

| Table | Owner (Team) | Owner (User) | Tier |
|-------|-------------|-------------|------|
| `dim_city` | data-engineering | Marie Dupont | Tier 1 — Mission Critical |
| `fct_weather_observation` | data-engineering | Marie Dupont | Tier 1 — Mission Critical |
| `weather_daily_summary` | data-analytics | Pierre Martin | Tier 2 — Key |
| `city_weather_metrics` | data-analytics | Pierre Martin | Tier 2 — Key |
| `weather_trend_analysis` | data-analytics | Pierre Martin | Tier 3 — Departmental |
| `city_comparison_ranking` | data-analytics | Pierre Martin | Tier 3 — Departmental |

### 10.4 Tiers (niveaux d'importance)

OpenMetadata propose 5 niveaux de tier :

| Tier | Signification | Impact |
|------|--------------|--------|
| **Tier 1** — Mission Critical | Si cette table est en panne, l'activité est bloquée | SLA 99.9%, alertes immédiates |
| **Tier 2** — Key | Important pour les dashboards et reportings | SLA 99%, alertes sous 1h |
| **Tier 3** — Departmental | Utilisé par une équipe spécifique | SLA 95%, alertes quotidiennes |
| **Tier 4** — Team | Usage interne à l'équipe | Best effort |
| **Tier 5** — Unused / Deprecated | Tables à archiver ou supprimer | Candidat à la suppression |

**Pour assigner** : Table → icône "..." → "Update Tier"

---

## 11. Alertes et notifications

### 11.1 Configurer les observabilité alerts

**Chemin UI** : Settings → Notifications → Add Alert

#### Alert 1 — Échec de test de qualité

| Champ | Valeur |
|-------|--------|
| Name | `quality-test-failure` |
| Trigger | Test Suite → Status = Failed |
| Filter | Tables avec Tier 1 ou Tier 2 |
| Destination | Webhook vers Slack (ou email) |

#### Alert 2 — Changement de schéma

| Champ | Valeur |
|-------|--------|
| Name | `schema-change-alert` |
| Trigger | Table → Schema Change Detected |
| Filter | Toutes les tables du service `datawarehouse` |
| Destination | Email vers `data-engineering` |

#### Alert 3 — Données manquantes (freshness)

| Champ | Valeur |
|-------|--------|
| Name | `data-freshness-alert` |
| Trigger | Test Suite → Test = Freshness → Status = Failed |
| Filter | Tables Tier 1 |
| Destination | Slack `#data-alerts` |

#### Alert 4 — Nouvelle table détectée

| Champ | Valeur |
|-------|--------|
| Name | `new-table-detected` |
| Trigger | Table → Created |
| Filter | Service = `datawarehouse` |
| Destination | Email vers `data-governance` |

### 11.2 Webhooks Slack

1. Créer un Slack App avec Incoming Webhook
2. Settings → Notifications → Destinations → Add Slack
3. Coller le Webhook URL
4. Tester avec "Send Test Notification"

> **Pour la démo** : Utiliser MailHog (http://localhost:8025) comme destination email locale.

---

## 12. Custom Properties

Ajouter des propriétés métier personnalisées aux tables.

### 12.1 Créer des custom properties

**Chemin UI** : Settings → Custom Properties → Tables → Add Property

| Propriété | Type | Description | Exemple de valeur |
|-----------|------|-------------|-------------------|
| `data_source` | String | API ou système source | `OpenWeather API v2.5` |
| `refresh_frequency` | String | Fréquence de rafraîchissement | `Every 3 hours` |
| `sla_hours` | Integer | SLA de disponibilité en heures | `4` |
| `data_retention_days` | Integer | Durée de rétention en jours | `365` |
| `business_owner` | String | Responsable métier | `Département Météo` |
| `gdpr_relevant` | Boolean | Contient des données RGPD | `false` |
| `cost_center` | String | Centre de coût | `METEO-001` |
| `api_endpoint` | String | URL de l'API source | `/data/2.5/weather` |

### 12.2 Remplir les propriétés

| Table | data_source | refresh_frequency | sla_hours | retention_days |
|-------|-----------|-------------------|-----------|----------------|
| `fct_weather_observation` | OpenWeather API v2.5 | Every 3 hours | 4 | 730 |
| `dim_city` | OpenWeather API v2.5 | Weekly | 24 | Unlimited |
| `weather_daily_summary` | Derived (dbt) | Daily at 03:00 | 6 | 365 |
| `city_weather_metrics` | Derived (dbt) | Daily at 03:00 | 6 | 365 |
| `weather_trend_analysis` | Derived (dbt) | Daily at 03:00 | 12 | 180 |
| `city_comparison_ranking` | Derived (dbt) | Daily at 03:00 | 12 | 180 |

---

## 13. Policies et rôles

### 13.1 Rôles personnalisés

**Chemin UI** : Settings → Roles → Add Role

| Rôle | Description | Permissions |
|------|-------------|-------------|
| `Weather Data Steward` | Peut éditer descriptions, tags, glossaire | EditDescription, EditTags, EditGlossary |
| `Weather Data Consumer` | Peut voir les données, commenter, suivre | ViewAll, EditFollowers |
| `Weather Pipeline Admin` | Peut gérer les ingestion pipelines | EditAll on Ingestion Pipelines |

### 13.2 Policies (règles d'accès)

**Chemin UI** : Settings → Policies → Add Policy

#### Policy : `weather-data-access`

```yaml
rules:
  - name: "Allow analytics team to view mart tables"
    effect: allow
    operations: [ViewAll]
    resources: ["table"]
    condition:
      match:
        - anyOf:
            - tags: ["Data Layer.Gold"]
  - name: "Restrict write on Tier 1 tables"
    effect: deny
    operations: [EditAll, Delete]
    resources: ["table"]
    condition:
      match:
        - anyOf:
            - tier: ["Tier1"]
    excludeRoles: ["DataSteward", "Admin"]
```

### 13.3 Assigner les rôles aux équipes

| Équipe | Rôle | Policy |
|--------|------|--------|
| data-engineering | Weather Pipeline Admin | Full access |
| data-analytics | Weather Data Consumer | weather-data-access |
| data-governance | Weather Data Steward | Full access on metadata |
| weather-operations | Weather Data Consumer | Read-only |

---

## 14. KPIs de données

### 14.1 Configurer des Data Insights KPIs

**Chemin UI** : Insights → KPIs → Add KPI

| KPI | Description | Target | Deadline |
|-----|-----------|--------|----------|
| **Description Coverage** | % de tables avec une description | 100% | 30 jours |
| **Ownership Coverage** | % de tables avec un owner | 100% | 14 jours |
| **Tier Assignment** | % de tables avec un tier assigné | 100% | 14 jours |
| **Glossary Coverage** | % de colonnes liées à un terme de glossaire | 80% | 60 jours |

### 14.2 Data Insights Dashboard

Après quelques jours d'activité, le dashboard Insights montre :

- **Total Data Assets** : 6 tables, ~50 colonnes
- **Description Coverage** : objectif 100%
- **Owner Coverage** : objectif 100%
- **Tier Coverage** : objectif 100%
- **Most Active Assets** : tables les plus consultées/commentées
- **Most Used Assets** : requêtes les plus fréquentes (si query usage activé)

---

## 15. Activités et collaboration

### 15.1 Conversations sur les tables

1. Aller sur une table (ex: `fct_weather_observation`)
2. Onglet **Activity Feeds & Tasks**
3. Poster un commentaire : "Attention : les données de Tokyo montrent des anomalies de température depuis le 25/03. Investigation en cours."
4. Mentionner un utilisateur : "@Marie Dupont peux-tu vérifier le pipeline d'ingestion ?"

### 15.2 Tâches (Assignments)

1. Cliquer "Create Task" sur une table
2. Type : "Request Description" ou "Request Tag" ou "Generic"
3. Assigner à un utilisateur
4. Exemples :
   - "Ajouter la description des colonnes `cloud_coverage_percent` et `visibility_meters`"
   - "Valider les valeurs acceptables pour `humidity_percent`"
   - "Revoir le test de fraîcheur — est-ce que 2 jours est suffisant ?"

### 15.3 Suivre des tables

1. Cliquer l'icône "Follow" (étoile) sur une table
2. Recevoir des notifications quand :
   - Le schéma change
   - Un test échoue
   - Quelqu'un commente
   - Les données sont mises à jour

### 15.4 Announcements

**Chemin UI** : Table → Announcements → Add

Exemple :
> **Maintenance planifiée** — Le pipeline d'ingestion sera arrêté le 05/04 de 02:00 à 06:00
> pour migration de la base PostgreSQL. Les données seront indisponibles pendant cette fenêtre.

---

## 16. Automatisation — Import/Export de configuration

### 16.1 Stratégie d'automatisation

OpenMetadata expose une API REST v1 complète pour piloter toute la configuration.
Le projet inclut un **script de bootstrap** qui provisionne automatiquement :

| Ce qui est automatisé | Source de vérité | Mécanisme |
|---|---|---|
| Descriptions de tables/colonnes | dbt `.yml` files | **dbt Agent** (import automatique) |
| Lineage (flux de données) | dbt `manifest.json` | **dbt Agent** (import automatique) |
| Tests dbt | dbt `.yml` tests | **dbt Agent** (import automatique) |
| Classifications & Tags | `scripts/om_bootstrap.py` | **API REST** (script) |
| Glossaire métier | `scripts/om_bootstrap.py` | **API REST** (script) |
| Équipes & Utilisateurs | `scripts/om_bootstrap.py` | **API REST** (script) |
| Tiers (Tier 1/2/3) | `scripts/om_bootstrap.py` | **API REST** (script) |
| Domains | `scripts/om_bootstrap.py` | **API REST** (script) |
| Custom Properties | `scripts/om_bootstrap.py` | **API REST** (script) |
| Ownership (propriétaires) | `scripts/om_bootstrap.py` | **API REST** (script) |
| Profil statistique | Profiler Agent | **Automatique** (schedule) |
| Sample Data | Profiler Agent | **Automatique** (schedule) |
| Validation de schéma (API) | `contracts/*.yaml` | **Pipeline DLT** (pre-ingestion) |

### 16.2 Script de bootstrap — Gouvernance (`metadata/om_bootstrap.py`)

Ce script Python provisionne toute la gouvernance en une seule commande :

```bash
# Premier lancement (après Metadata Agent + Profiler Agent)
just om-bootstrap
# ou directement : python metadata/om_bootstrap.py

# Mode dry-run (affiche les actions sans les exécuter)
just om-bootstrap --dry-run

# Exporter la configuration actuelle
just om-export

# URL personnalisée
python metadata/om_bootstrap.py --url http://openmetadata-server:8585
```

**Ce que fait le script** :
1. S'authentifie via JWT (`admin@open-metadata.org`)
2. Crée les Classifications : `DataLayer` (Bronze/Silver/Gold), `DataDomain`, `DataSensitivity`, `UpdateFrequency`
3. Crée le Glossaire "Weather Data Glossary" avec 8 termes métier
4. Crée les équipes : data-engineering, data-analytics, data-governance, weather-operations
5. Crée les utilisateurs fictifs (Marie, Pierre, Sophie, Jean-Paul)
6. Crée les Custom Properties pour les tables
7. Applique sur chaque table : description, tier, tags, ownership
8. Crée les Domains et assigne les tables
9. Exporte la config dans `metadata/om_config_export.json`

### 16.3 Script de setup complet — Tests, Profiler, Services (`metadata/om_setup_full.py`)

Ce script complémente `om_bootstrap.py` en configurant la data quality, le profiling avancé,
le service Airflow et les descriptions de colonnes (data contracts) :

```bash
# Lancement complet (après Metadata Agent)
just om-setup
# ou directement : python metadata/om_setup_full.py
```

**Ce que fait le script** :

| Étape | Action | Détails |
|-------|--------|---------|
| 1 | **Register Airflow** | Enregistre Airflow comme Pipeline Service (`http://airflow-webserver:8080`) |
| 2 | **Update Profiler** | Met à jour l'agent Profiler : ajoute `staging` au filtre de schémas, active toutes les métriques |
| 3 | **Create Test Suites** | Crée 10 test suites exécutables (1 par table, toutes couches medallion) |
| 4 | **Create Test Cases** | Crée 65+ test cases natifs OM couvrant toutes les tables |
| 5 | **Column Descriptions** | Applique ~40 descriptions détaillées sur les colonnes `core` et `mart` |
| 6 | **Trigger Agents** | Déclenche metadata → dbt → profiler dans l'ordre |

**Tables couvertes par les tests** :

| Schéma | Table | Tests |
|--------|-------|-------|
| `core` | `fct_weather_observation` | row count, column count, not null, unique, ranges (temp, humidity, pressure, wind), set membership (country_code) |
| `core` | `dim_city` | row count, column count, unique (city_name), ranges (lat/lon), not null |
| `mart` | `weather_daily_summary` | row count, not null, positive observation count, min ≤ max temperature |
| `mart` | `city_weather_metrics` | row count, unique city, positive observations, temperature ranges |
| `analytic` | `weather_trend_analysis` | row count, not null, temperature ranges |
| `analytic` | `city_comparison_ranking` | row count, unique city, rank ≥ 1 |
| `staging` | `stg_weather_current` | row count, not null, temperature/humidity/pressure ranges |
| `staging` | `stg_weather_forecast` | row count, not null, precipitation probability 0–1 |
| `staging_quarantine` | `quarantine_weather_current` | row count, not null (rejection_reason) |
| `staging_quarantine` | `quarantine_weather_forecast` | row count, not null (rejection_reason) |

**Descriptions de colonnes** (servent de data contracts documentaires) :

Les descriptions appliquées par le script incluent :
- `fct_weather_observation` : 15 colonnes documentées (row_id, city_name, temperature_celsius, feels_like_celsius, humidity_percent, pressure_hpa, wind_speed_ms, weather_condition, country_code, cloud_coverage_percent, visibility_meters, measured_at, load_id, loaded_at)
- `dim_city` : 4 colonnes (city_name, country_code, latitude, longitude)
- `weather_daily_summary` : 9 colonnes (city_name, date_day, avg/min/max temperature, avg humidity/pressure/wind, observation_count)
- `city_weather_metrics` : 7 colonnes (city_name, total_observations, avg/min/max temperature, first/last observation)

### 16.4 Ordre d'exécution recommandé

```bash
# 1. Démarrer le stack complet
just start-full

# 2. Attendre que OM soit healthy (~2-3 min)

# 3. Lancer le Metadata Agent depuis l'UI (ou attendre le schedule)

# 4. Provisionner la gouvernance
just om-bootstrap

# 5. Configurer tests + profiler + services + descriptions
just om-setup
```

### 16.5 Data Contracts vs. OpenMetadata Contracts

Il existe **deux niveaux** de contrats de données dans cette architecture :

| | Data Contracts (pipeline) | OM Contracts (catalogue) |
|---|---|---|
| **Fichiers** | `contracts/*.yaml` | UI OM → onglet Contract |
| **Quand** | Avant ingestion (pre-storage) | Après ingestion (post-catalogage) |
| **Valide** | Structure JSON brute de l'API | Schéma des tables PostgreSQL |
| **Mécanisme** | `validator.py` dans le pipeline DLT | OpenMetadata API/UI |
| **Bloquant** | Oui — rejette les données invalides | Non — signale les violations |
| **Scope** | Schéma source (API response) | Schéma destination (colonnes PG) |

> **Les Data Contracts (`contracts/`)** ne remplacent PAS la configuration OM.
> Ils valident les données **avant** qu'elles n'entrent dans le pipeline.
> La configuration OM (tags, tiers, descriptions, glossaire) concerne la **gouvernance**
> des données déjà stockées.

Pour créer un OM Contract sur une table :
1. Aller sur la table → onglet **Contract**
2. Cliquer **Add Contract**
3. Sélectionner les colonnes couvertes et les contraintes attendues

### 16.6 API REST — Exemples manuels

**Authentification** :

```bash
TOKEN=$(curl -s -X POST "http://localhost:8585/api/v1/users/login" \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@open-metadata.org","password":"YWRtaW4="}' \
  | jq -r '.accessToken')
```

**Lister les tables** :

```bash
curl -s "http://localhost:8585/api/v1/tables?limit=50" \
  -H "Authorization: Bearer $TOKEN" | jq '.data[].name'
```

**Créer un test de qualité** (OM 1.12.x utilise `PUT`, pas `POST`) :

```bash
# Test suite (lié à une table via executableEntityReference)
curl -X PUT "http://localhost:8585/api/v1/dataQuality/testSuites" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "fct_weather_observation_suite",
    "displayName": "Weather Observation Quality Suite",
    "description": "Data quality tests for core.fct_weather_observation.",
    "executableEntityReference": "datawarehouse.datawarehouse.core.fct_weather_observation"
  }'

# Test case (auto-linked to suite via entityLink)
curl -X PUT "http://localhost:8585/api/v1/dataQuality/testCases" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "fct_weather_observation_temperature_range",
    "entityLink": "<#E::table::datawarehouse.datawarehouse.core.fct_weather_observation::columns::temperature_celsius>",
    "testDefinition": "columnValuesToBeBetween",
    "parameterValues": [
      {"name": "minValue", "value": "-60"},
      {"name": "maxValue", "value": "60"}
    ]
  }'
```

> **Important OM 1.12.x** : Ne PAS inclure `testSuite` dans le payload du test case.
> Le test case est automatiquement lié à la test suite exécutable de la table via `entityLink`.

---

## 17. Checklist récapitulative

### Phase 1 — Setup (Jour 1)

- [ ] Démarrer le stack avec `just start-full`
- [ ] Se connecter à http://localhost:8585
- [ ] Compléter le wizard de bienvenue
- [ ] Configurer le service PostgreSQL `datawarehouse`
- [ ] Configurer le service S3 `minio-data-lake`
- [ ] Tester les deux connexions

### Phase 2 — Agents d'ingestion (Jour 1-2)

- [ ] Lancer le **Metadata Agent** (onglet Agents → Add Metadata Agent)
- [ ] Vérifier que les tables apparaissent dans Explore
- [ ] Lancer le **Profiler Agent** (onglet Agents → Add Profiler Agent)
- [ ] Vérifier les statistiques de colonnes (onglet **Data Observability** → **Column Profile** de chaque table)
- [ ] Vérifier les échantillons de données (onglet Sample Data)
- [ ] Lancer le **dbt Agent** (onglet Agents → Add dbt Agent)
- [ ] Vérifier le lineage dans l'UI (onglet Lineage de chaque table)

### Phase 3 — Domains (Jour 2)

- [ ] Créer le domain `weather-observations` (Domains → Add Domain)
- [ ] Créer le domain `weather-analytics` (Domains → Add Domain)
- [ ] Créer le domain `data-platform` (Domains → Add Domain)
- [ ] Assigner les tables aux domains correspondants
- [ ] Créer les Data Products (optionnel)

### Phase 4 — Gouvernance (Jour 2-3)

- [ ] Créer les équipes : data-engineering, data-analytics, data-governance, weather-operations
- [ ] Créer les utilisateurs fictifs
- [ ] Assigner l'ownership sur chaque table
- [ ] Assigner les tiers (Tier 1/2/3) sur chaque table
- [ ] Compléter les descriptions manquantes (core, analytic)

### Phase 5 — Classification (Jour 3-4)

- [ ] Créer la classification `DataLayer` (Bronze/Silver/Gold)
- [ ] Créer la classification `DataDomain` (Meteorology/Geography/Analytics)
- [ ] Créer la classification `DataSensitivity` (Public/Internal/PII)
- [ ] Créer la classification `UpdateFrequency`
- [ ] Appliquer les tags sur toutes les tables
- [ ] Créer les custom properties (`data_source`, `sla_hours`, etc.)
- [ ] Remplir les custom properties pour chaque table

### Phase 6 — Glossaire (Jour 4-5)

- [ ] Compléter le glossaire `WeatherDomain` existant
- [ ] Ajouter les termes atmosphériques (Temperature, Humidity, etc.)
- [ ] Ajouter les termes de conditions météo
- [ ] Ajouter les termes de métriques calculées
- [ ] Ajouter les termes géographiques
- [ ] Lier les termes aux colonnes correspondantes

### Phase 7 — Data Quality (Jour 5-6)

- [ ] Vérifier les tests dbt importés dans OM
- [ ] Exécuter `just om-setup` pour créer automatiquement 10 test suites + 65 test cases
- [ ] Vérifier les tests dans l'UI : Table → Data Observability → Data Quality
- [ ] Ajouter des tests custom SQL supplémentaires si nécessaire
- [ ] Planifier l'exécution automatique des tests
- [ ] Vérifier les résultats dans le dashboard Data Quality

### Phase 8 — Alertes et KPIs (Jour 6-7)

- [ ] Configurer l'alerte `quality-test-failure`
- [ ] Configurer l'alerte `schema-change-alert`
- [ ] Configurer l'alerte `data-freshness-alert`
- [ ] Configurer l'alerte `new-table-detected`
- [ ] Définir le KPI "Description Coverage → 100%"
- [ ] Définir le KPI "Ownership Coverage → 100%"
- [ ] Vérifier le dashboard Insights

### Phase 9 — Collaboration (Jour 7+)

- [ ] Poster des commentaires sur les tables
- [ ] Créer des tâches d'amélioration
- [ ] Suivre les tables critiques (Follow)
- [ ] Créer une annonce de maintenance
- [ ] Explorer les rôles et policies
- [ ] Tester l'API REST avec curl

---

## Annexe A — Ordonnancement des agents

```
02:00  ─── Ingestion Pipeline (Airflow) ──── DLT → S3 → dbt → PG
03:00  ─── Transformation Pipeline (Airflow) ─ dbt core/mart/analytic + quality
04:00  ─── OM Metadata Agent ──────────────── Discover tables & schemas
05:00  ─── OM Profiler Agent ──────────────── Compute column stats
06:00  ─── OM dbt Agent ───────────────────── Import lineage & descriptions
07:00  ─── OM Test Execution ──────────────── Run all quality tests
```

## Annexe B — Mapping complet des données

```
OpenWeather API (/data/2.5/weather)
    │
    ▼  DLT (ingestion)
S3: raw/openweather/data/weather_current/*.parquet
    │  Colonnes: name, sys__country, main__temp, main__feels_like,
    │            main__humidity, main__pressure, wind__speed, dt,
    │            coord__lat, coord__lon, clouds__all, visibility
    │
    ▼  dbt (staging — DuckDB in-memory)
stg_weather_current
    │  city_name, country_code, temperature_celsius, feels_like_celsius,
    │  humidity_percent, pressure_hpa, wind_speed_ms, weather_condition,
    │  latitude, longitude, cloud_coverage_percent, visibility_meters,
    │  measured_at, load_id, row_id, loaded_at
    │
    ├──▶  dim_city (core — PG)
    │     city_name, country_code, latitude, longitude
    │
    └──▶  fct_weather_observation (core — PG)
          row_id, city_name, country_code, temperature_celsius, ...
              │
              ▼
          weather_daily_summary (mart — PG)
          city_name, date_day, avg/min/max_temperature, observation_count
              │
              ├──▶  weather_trend_analysis (analytic — PG)
              │     temp_7d_moving_avg, temp_day_over_day_change
              │
              └──▶  city_weather_metrics (mart — PG)
                    total_observations, avg_temperature, first/last_observation
                        │
                        ▼
                    city_comparison_ranking (analytic — PG)
                    temp_rank_warmest, temp_vs_mean, data_completeness_rank
```

## Annexe C — Liens utiles

| Ressource | URL |
|-----------|-----|
| OpenMetadata UI | http://localhost:8585 |
| OM Ingestion Airflow | http://localhost:8086 |
| Airflow Pipeline | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Grafana | http://localhost:3000 |
| MailHog (test emails) | http://localhost:8025 |
| OpenMetadata Docs (1.12.x) | https://docs.open-metadata.org/v1.12.x |
| OpenMetadata API Swagger | http://localhost:8585/swagger.html |
