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
