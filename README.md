# API EAU

## Objectif
recuperer les données des analyses d'eau pour la commune de Valenciennes


## Bronze layer
### Objectif
stocker les données brutes de l'api Hubeau, en ajoutant des metadonnées utiles a l'historisation, la rejouabilité.

### Source des données : 
`https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis`

### Paramètres utilisés

| Paramètre | Description | Exemple |
|----------|------------|--------|
| `code_commune` | Filtre sur la commune (Valenciennes) | `59606` |
| `page` | Numéro de page | `1` |
| `size` | Nombre de résultats par page | `500` |

exemple d'appel api : 
`https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis?code_commune=59606&page=1&size=500` 

### Colonnes ajoutées

| Colonne | Description | Type |
|--------|------------|-------|
| `source_url` | URL d’ingestion | string|
| `ingestion_type` | Type de source (`api`) |string|
| `ingested_at` | Timestamp d’ingestion |timestamp|
| `hash` | Hash SHA-256 de la ligne (déduplication) |string|
| `annee` | Année du prélèvement (partition technique) |int|

### Format des données

- Format de stockage : **Delta Lake**
- Accès : **Unity Catalog (table Delta)**
- Table :  `main.bronze.qualite_eau`

### Déduplication 
Identification de chaque ligne par hash pour garantif l'idempotence des ingestions et detecter les doublons

## Silver layer

### Source

- Table source : `main.bronze.qualite_eau`
- Format : données issues de l’API Hub’Eau (qualité de l’eau potable)
- Structure : données enrichies avec champs scalaires + arrays (`reseaux`)

### Principes de transformation

#### 1. Nettoyage des colonnes inutiles

Les colonnes suivantes sont supprimées car vides ou non exploitables dans le contexte Silver :

- `libelle_parametre_web`
- `code_installation_amont`
- `nom_installation_amont`


#### 2. Typage des données

##### Règles générales

- Tous les champs `code_*` → `string` (préservation des zéros et identifiants longs)
- Dates → `timestamp`
- Mesures numériques → `double` (après nettoyage)
- Champs textuels → `string`

#### 3. Flatten des structures imbriquées

Le champ `reseaux` (array<struct>) est aplati en deux colonnes :

- `reseau_code` (string)
- `reseau_nom` (string)

#### 4. Champs métier

Création et normalisation des champs analytiques :

- `parametre` → libellé du paramètre
- `libelle_parametre` → libellé original
- `unite` → unité de mesure
- `limite_qualite_parametre` → seuil réglementaire (si disponible)
- `reference_qualite_parametre` → valeur de référence
- `commune` → nom de la commune
- `departement` → nom du département

#### 5. Conformité

Les indicateurs de conformité sont conservés tels quels, sans agrégation :

- `conformite_limites_bact_prelevement`
- `conformite_limites_pc_prelevement`
- `conformite_references_bact_prelevement`
- `conformite_references_pc_prelevement`

Chaque champ représente un axe distinct de contrôle qualité (bactériologique / physico-chimique / limites / références).

#### 6. Principe de conformité

Aucune agrégation ou synthèse n’est réalisée dans le Silver layer.

Les différents indicateurs de conformité sont conservés séparément afin de permettre des analyses métier dans le Gold layer.

### Schéma final Silver

| Colonne | Type | Description |
|----------|------|-------------|
| date_prelevement | timestamp | Date du prélèvement |
| departement | string | Département |
| commune | string | Commune |
| parametre | string | Paramètre analysé |
| libelle_parametre | string | Libellé original |
| resultat_alphanumerique | string | Valeur brute (texte ou numérique) |
| resultat_numerique | double | Valeur exploitable pour calculs |
| unite | string | Unité de mesure |
| limite_qualite_parametre | string | Seuil réglementaire |
| reference_qualite_parametre | string | Valeur de référence |
| reseau_code | string | Code du réseau |
| reseau_nom | string | Nom du réseau |
| conformite_limites_bact_prelevement | string | Conformité bactériologique (limites) |
| conformite_limites_pc_prelevement | string | Conformité physico-chimique (limites) |
| conformite_references_bact_prelevement | string | Conformité bactériologique (références) |
| conformite_references_pc_prelevement | string | Conformité physico-chimique (références) |
| code_departement | string | Code département |
| code_commune | string | Code commune |
| reference_analyse | string | Identifiant d’analyse |


## Bronze layer
### Vues
Créé des vues aggrégées pour la BI
- vue evolution des resultats dans le temps
```
spark.sql("""
CREATE OR REPLACE VIEW main.gold.v_time_series AS
SELECT
  date_trunc('day', date_prelevement) AS jour,
  parametre,
  AVG(resultat_numerique) AS moyenne,
  MIN(resultat_numerique) AS min_val,
  MAX(resultat_numerique) AS max_val,
  COUNT(*) AS nb_mesures
FROM main.silver.qualite_eau
WHERE resultat_numerique IS NOT NULL
GROUP BY date_trunc('day', date_prelevement), parametre
""")
```
- vue nombre de mesures
```
spark.sql("""
CREATE OR REPLACE VIEW main.gold.v_parametre_stats AS
SELECT
  parametre,
  COUNT(*) AS nb_mesures,
  AVG(resultat_numerique) AS moyenne,
  MIN(resultat_numerique) AS min_val,
  MAX(resultat_numerique) AS max_val
FROM main.silver.qualite_eau
WHERE resultat_numerique IS NOT NULL
GROUP BY parametre
""")
```
- vue qualité du reseau
```
spark.sql("""
CREATE OR REPLACE VIEW main.gold.v_reseau_quality AS
SELECT
  reseau_nom,
  COUNT(*) AS nb_mesures,
  SUM(CASE WHEN upper(conformite_limites_pc_prelevement) = 'NC' THEN 1 ELSE 0 END) AS nc_pc,
  SUM(CASE WHEN upper(conformite_limites_bact_prelevement) = 'NC' THEN 1 ELSE 0 END) AS nc_bact
FROM main.silver.qualite_eau
GROUP BY reseau_nom
""")
```
- depassement des limites
```
spark.sql("""
CREATE OR REPLACE VIEW main.gold.v_depassements AS
WITH base AS (
  SELECT
    date_prelevement,
    commune,
    parametre,
    resultat_numerique,

    try_cast(
      regexp_replace(
        regexp_replace(limite_qualite_parametre, ',', '.'),
        '[^0-9.]',
        ''
      ) AS double
    ) AS limite_numerique

  FROM main.silver.qualite_eau
  WHERE resultat_numerique IS NOT NULL
)

SELECT
  date_prelevement,
  commune,
  parametre,
  resultat_numerique,
  limite_numerique,
  CASE
    WHEN limite_numerique IS NOT NULL
     AND resultat_numerique > limite_numerique
    THEN 1 ELSE 0
  END AS depassement
FROM base
""")
```
- vue qualité par commune
```
spark.sql("""
CREATE OR REPLACE VIEW main.gold.v_commune_quality AS
SELECT
  commune,
  COUNT(*) AS nb_mesures,
  AVG(resultat_numerique) AS moyenne,
  MIN(resultat_numerique) AS min_val,
  MAX(resultat_numerique) AS max_val
FROM main.silver.qualite_eau
GROUP BY commune
""")
```
- vue top anomalies
```
spark.sql("""
CREATE OR REPLACE VIEW main.gold.v_top_anomalies AS
SELECT
  date_prelevement,
  commune,
  parametre,
  resultat_numerique,
  limite_qualite_parametre
FROM main.silver.qualite_eau
WHERE resultat_numerique IS NOT NULL
ORDER BY resultat_numerique DESC
""")
```

### Great expectations
- installer great expectation (version compatible databricks)
```
%pip install great-expectations==0.18.21
```

- enlever les colonnes void (eviter le plantage spark)
```
void_cols = [c for c, t in df.dtypes if "void" in t.lower()]
df = df.drop(*void_cols)
```
- creer un df pandas 
```
pdf = df.limit(10000).toPandas()
```
- creer un df great Xpectations
```
gx_df = gx.from_pandas(pdf)
```
- check le nombre de colonnes superieur a 1
```
gx_df.expect_table_row_count_to_be_between(min_value=1)
```
voir si les colones d'enrichissement sont bien presente
```
gx_df.expect_column_to_exist("code_prelevement")
gx_df.expect_column_to_exist("hash")
gx_df.expect_column_to_exist("date_prelevement")
```
- verifier si le code du hash est bien effectif
```
gx_df.expect_column_values_to_not_be_null("hash")
gx_df.expect_column_values_to_be_unique("hash")
```
- check des nulls
```
gx_df.expect_column_values_to_not_be_null("libelle_parametre")
gx_df.expect_column_values_to_not_be_null("nom_commune")
```
- check si le code departement est correct
```
gx_df.expect_column_values_to_match_regex(
    "code_departement",
    r"^\d{2,3}$"
)
```
- check si le code commune est conforme
```
gx_df.expect_column_values_to_match_regex(
    "code_commune",
    r"^\d{5}$"
)
```
- check si l'année est bonne (entre 2000 et 2030)
```
gx_df.expect_column_values_to_be_between(
    "annee",
    min_value=2000,
    max_value=2030
)
```
- lancer les test
```
results = gx_df.validate()
```

### Deployement
#### Action workflow
- Checkout (recuperer le code)
- Install (databricks cli)
- Deploy le bundle (lancer la commande pour creer les jobs databricks via databricks.yml) en s'appuyant sur les secrets github suivants : 
    - databricks_host: (l'url de l'environnement databricks)
    - databricks_token : (le token d'acces a l'env databricks)
#### databricks.yml
permet de configurer le bundle databricks
- nommer le bundle ave bundle.name
- declarer un job databricks (resources.jobs.xx)
- creer une tache 
    - creer un cluster (spark_version, node_type_id, num_workers)
    - lancer le script python(hello.py)
- targets.dev.workspace : (host / root_path)