# DataSport - Nations Ranking Pipeline (All Sports)

Pipeline Python open-source pour construire progressivement une base locale "classement des nations tous sports" avec provenance complète (`sources`, `raw_imports`) et exports réutilisables (CSV).

## Objectif

- Construire des dimensions robustes : `countries`, `sports`, `disciplines`.
- Ingest sport par sport via des connecteurs indépendants.
- Conserver snapshots bruts + métadonnées de build.
- Upsert dans SQLite portable : `data/processed/sports_nations.db`.

## Documentation opérationnelle

- Guide de remplissage des bases: `docs/DB_FILLING_PLAYBOOK.md`
- Backlog competitions mondiales: `docs/TODO_WORLD_COMPETITIONS.md`

## Arborescence

```
src/
  core/
  connectors/
  pipelines/
pipelines/                       # wrappers pour `python -m pipelines.*`
data/
  raw/
  processed/
exports/
meta/
```

## Schéma SQLite

Tables principales:
- `countries`
- `sports`
- `disciplines`
- `competitions`
- `events`
- `participants`
- `results`
- `sources`
- `raw_imports`
- `sport_federations` (optionnelle, enrichissement Wikidata)

Voir `meta/data_dictionary.md` pour les colonnes détaillées.

## Architecture des bases

Le projet utilise une base maître + 3 bases spécialisées au format CSV:
- `data/processed/sports_nations.db` (base maître unifiée)
- `data/processed/databases/reference/*.csv` (dimensions et référentiels)
- `data/processed/databases/competition/*.csv` (compétitions, events, participants, résultats)
- `data/processed/databases/lineage/*.csv` (provenance `sources` et `raw_imports`)

Génération/synchronisation depuis la base maître:

```bash
python -m pipelines.init_databases
```

Sorties:
- `meta/database_architecture.json`
- `exports/architecture/database_architecture.csv`

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Commandes CLI

### 1) Bootstrap dimensions

```bash
python -m pipelines.bootstrap_dimensions
```

Effets:
- crée/maj `data/processed/sports_nations.db`
- charge `countries` (pycountry si dispo, fallback sinon)
- ingère `data/raw/sport_name_seed.txt`
- applique `data/raw/sport_mapping.yaml`
- génère audit `exports/bootstrap_dimensions/discipline_mapping_audit.csv`
- exporte dimensions CSV
- génère `meta/build_meta.json` + `meta/data_dictionary.md`

### 2) Ingest Wikidata (CC0)

```bash
python -m pipelines.ingest --connector wikidata --year 2025
```

Effets:
- snapshot brut dans `data/raw/wikidata/<timestamp>/`
- enrichit `sports` + `sport_federations`
- exporte `exports/wikidata/year=2025/`

Note: si l'endpoint SPARQL n'est pas joignable, le connecteur utilise un petit payload fallback local pour garder un pipeline exécutable.

### 3) Ingest football-data

```bash
export FOOTBALL_DATA_TOKEN="..."
python -m pipelines.ingest --connector football_data --year 2025
```

Comportement:
- si token absent: skip propre (`status=skipped`)
- cible prioritairement des compétitions d'équipes nationales
- applique limites de taux simples (pause + retries)

### 4) Ingest NBA balldontlie

```bash
export BALDONTLIE_API_KEY="..."
python -m pipelines.ingest --connector balldontlie_nba --year 2025
```

Comportement:
- si clé absente: skip propre
- map pays des franchises (heuristique: `TOR -> CAN`, autres `USA`)
- limité car données de clubs (pas idéal pour classement des nations)

### 5) Ingest FIFA ranking historique (top 10 nations)

```bash
python -m pipelines.ingest --connector fifa_ranking_history --year 2026
```

Comportement:
- ingère l'historique des publications FIFA disponibles dans la source CSV open-source
- conserve une seule publication par année (la plus récente de l'année), puis le `top 10`
- remplit `sports`, `disciplines`, `competitions`, `events`, `participants`, `results`
- crée une compétition unique `competition_id=fifa_ranking`
- crée des events annuels lisibles: `fifa_ranking_92`, `fifa_ranking_93`, etc.
- exporte les tables normalisées dans `exports/fifa_ranking_history/year=2026/`
- avec la source par défaut actuelle, la couverture va de `1992-12-31` à `2024-09-19`

### 5b) Ingest FIFA Women ranking historique (top 10 nations)

```bash
python -m pipelines.ingest --connector fifa_women_ranking_history --year 2026
```

Comportement:
- ingère les snapshots historiques de ranking femmes (sources open data / seed local)
- conserve une publication par année (la plus récente), puis le `top 10`
- crée une compétition unique `competition_id=fifa_women_ranking`
- crée des events annuels lisibles: `fifa_women_ranking_03`, `fifa_women_ranking_04`, etc.
- exporte les tables normalisées dans `exports/fifa_women_ranking_history/year=2026/`
- seed local supporté: `data/raw/fifa_women/fifa_w_ranking_historical.csv` ou `data/raw/fifa_women/fifa_w_ranking-YYYY-MM-DD.csv`

### 5c) Ingest World Rugby rankings historiques (hommes + femmes, top 10 nations)

```bash
python -m pipelines.ingest --connector world_rugby_ranking_history --year 2026
```

Comportement:
- ingère les snapshots annuels World Rugby via l'API officielle (`mru` et `wru`)
- conserve une publication par année (date effective la plus récente de l'année demandée), puis le `top 10`
- crée deux compétitions:
  - `world_rugby_men_ranking`
  - `world_rugby_women_ranking`
- crée des events annuels lisibles: `world_rugby_men_ranking_25`, `world_rugby_women_ranking_25`, etc.
- exporte les tables normalisées dans `exports/world_rugby_ranking_history/year=2026/`
- seed local supporté: `data/raw/world_rugby/world_rugby_rankings_history.csv`
- couverture observée avec les sources actuelles:
  - men: `2003-10-13` -> `2017-02-20`, puis `2020-12-28` -> `2026-02-16`
  - women: `2016-12-26` -> `2026-02-16`
  - gap connu men: `2018-2019` non disponible via les sources branchées actuellement

### 5d) Ingest FIBA rankings historiques (hommes + femmes, top 10 nations)

```bash
python -m pipelines.ingest --connector fiba_ranking_history --year 2026
```

Comportement:
- ingère le seed historique local `data/raw/basketball/fiba_rankings_history_seed.csv`
- conserve une publication par année (la plus récente), puis le `top 10`
- crée deux compétitions:
  - `fiba_men_ranking`
  - `fiba_women_ranking`
- crée des events annuels lisibles: `fiba_men_ranking_25`, `fiba_women_ranking_25`
- exporte les tables normalisées dans `exports/fiba_ranking_history/year=2026/`

### 6) Validation

```bash
python -m pipelines.validate
```

Checks:
- intégrité FK
- sanity (`rank >= 1`, `country_id` connu ou null)

### 7) Construire l'architecture multi-bases

```bash
python -m pipelines.init_databases
```

### 8) Ingest Coupe du Monde (historique)

```bash
python -m pipelines.ingest --connector world_cup_history --year 2026
```

Comportement:
- ingère les données historiques open-source depuis `openfootball/world-cup`
- utilise en priorité le seed local `data/raw/world_cup/world_cup_top4_seed.csv` (reproductible offline)
- crée une compétition unique `competition_id=fifa_world_cup`
- crée un event par édition (`fifa_world_cup_30`, ..., `fifa_world_cup_22`)
- alimente le classement final top 4 par édition (1er, 2e, 3e, 4e)
- `participant_id` est le code pays

### 8b) Ingest Coupe du Monde feminine (historique)

```bash
python -m pipelines.ingest --connector fifa_women_world_cup_history --year 2026
```

Comportement:
- ingère le seed historique local `data/raw/world_cup/womens_world_cup_top4_seed.csv`
- crée une compétition unique `competition_id=fifa_womens_world_cup`
- crée un event par édition (`fifa_womens_world_cup_91`, ..., `fifa_womens_world_cup_23`)
- alimente le classement final top 4 par édition (1er, 2e, 3e, 4e)
- `participant_id` est le code pays

### 8c) Ingest Coupe du Monde de rugby (historique, hommes + femmes)

```bash
python -m pipelines.ingest --connector rugby_world_cup_history --year 2026
```

Comportement:
- ingère les seeds historiques locaux:
  - `data/raw/world_rugby/rugby_world_cup_top4_seed.csv`
  - `data/raw/world_rugby/womens_rugby_world_cup_top4_seed.csv`
- crée deux compétitions:
  - `rugby_world_cup_men`
  - `rugby_world_cup_women`
- crée un event par édition:
  - `rugby_world_cup_men_87`, ..., `rugby_world_cup_men_23`
  - `rugby_world_cup_women_91`, ..., `rugby_world_cup_women_25`
- alimente le classement final top 4 par édition (1er, 2e, 3e, 4e)
- `participant_id` est le code pays

### 8d) Ingest Coupe du Monde FIBA Basketball (historique, hommes + femmes)

```bash
python -m pipelines.ingest --connector fiba_basketball_world_cup_history --year 2026
```

Comportement:
- ingère les seeds historiques locaux:
  - `data/raw/basketball/fiba_world_cup_men_top4_seed.csv`
  - `data/raw/basketball/fiba_world_cup_women_top4_seed.csv`
- crée deux compétitions:
  - `fiba_basketball_world_cup_men`
  - `fiba_basketball_world_cup_women`
- crée un event par édition (suffixe `YY`)
- alimente le classement final top 4 par édition (1er, 2e, 3e, 4e)
- `participant_id` est le code pays

### 9) Ingest JO d'été Paris 2024

```bash
python -m pipelines.ingest --connector paris_2024_summer_olympics --year 2024
```

Comportement:
- importe la source demandée `KeithGalli/.../results.csv` (historique JO) pour traçabilité
- détecte que cette source s'arrête à 2022 et ne contient pas Paris 2024
- complète Paris 2024 via `taniki/paris2024-data` (médailles par épreuve)
- construit sports + disciplines en séparant bien les deux niveaux (ex: `Aquatics` > `Swimming`)
- ajoute la compétition `summer_olympics_paris_2024`
- ajoute un event par épreuve (ex: 100m, relais, etc.)
- format `event_id`: `paris2024_<discipline>_<event>` (sans code suffixe)
- remplit les résultats podium par épreuve (`rank` 1/2/3; bronze multiple si tie)
- crée des participants explicites:
  - `athlete_<nom_prenom>_<noc>`
  - `nation_<noc>`

### 10) Ingest JO historiques (jusqu'à une année cible)

```bash
python -m pipelines.ingest --connector olympics_keith_history --year 2000
```

Comportement:
- ingère les résultats historiques du dataset KeithGalli
- charge les éditions Summer + Winter depuis `--year` (ex: 2000)
- crée une compétition par édition (`olympics_summer_1996`, etc.)
- crée un event par épreuve et ne conserve que les résultats médaillés (or/argent/bronze)
- conserve des IDs explicites (`athlete_*` / `nation_*`)

## Politique licences et partage data

- Code du repo: `MIT`.
- Wikidata: CC0 (prioritaire pour contenu partageable).
- APIs tierces (football-data, balldontlie): respecter leurs ToS/licences.
  - Éviter republication brute sans droit explicite.
  - Privilégier stockage d'agrégats, IDs déterministes, et liens/provenance.
- JO Paris 2024:
  - source historique demandée: `KeithGalli/Olympics-Dataset` (README repo: CC BY 4.0)
  - source résultats Paris 2024 par épreuve: `taniki/paris2024-data` (jeu public GitHub)

## ID déterministes

- `sport_id = slug(sport_name)`
- `discipline_id = slug(discipline_name)`
- `country_id = ISO3`
- `competition_id/event_id/participant_id/import_id` via SHA1 stable:
  - ex: `sha1("football_data|competition|WC|2025")`

## Ajouter un connecteur

1. Créer `src/connectors/<new_connector>.py` qui hérite de `Connector`.
2. Implémenter:
   - `fetch(season_year, out_dir)`
   - `parse(raw_paths, season_year)`
   - `upsert(db, payload)`
3. Enregistrer dans `src/connectors/registry.py`.
4. Lancer:
   - `python -m pipelines.ingest --connector <id> --year 2025`

## Limites actuelles (MVP)

- Le classement "nations" est fort sur sports individuels/équipes nationales.
- Les ligues de clubs sont conservées avec prudence (attribution pays discutable).
- Le seed sports/disciplines est initial, à enrichir via overrides YAML et revue de l'audit.
