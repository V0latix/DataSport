# Playbook - Remplissage des bases DataSport

Ce document définit la façon correcte de remplir les bases pour éviter les incohérences dans `competition`, `reference` et `lineage`.

## 1) Principes obligatoires

- Toujours passer par le pipeline (`python -m pipelines.*`), jamais par édition manuelle des CSV finaux.
- Toujours conserver la traçabilité: `sources` + `raw_imports` doivent être renseignés pour chaque import.
- Prioriser les sources ouvertes; documenter clairement les limites de licence.
- Garder des IDs stables et lisibles quand possible.
- Exécuter la validation après chaque ingestion.

## 2) Architecture cible

- Base maître: `data/processed/sports_nations.db`
- Bases CSV spécialisées (générées depuis la base maître):
  - `data/processed/databases/reference/*.csv`
  - `data/processed/databases/competition/*.csv`
  - `data/processed/databases/lineage/*.csv`

Commande de synchronisation:

```bash
python -m pipelines.init_databases
```

## 3) Ordre de remplissage

1. Bootstrap dimensions:
```bash
python -m pipelines.bootstrap_dimensions
```
2. Ingestion connecteur(s):
```bash
python -m pipelines.ingest --connector <connector_id> --year <YYYY>
```
3. Validation:
```bash
python -m pipelines.validate
```
4. Export architecture CSV:
```bash
python -m pipelines.init_databases
```

## 4) Règles de modélisation par table

### `sources`
- Une ligne par connecteur (`source_id` unique).
- `base_url` doit être une URL réellement accessible.

### `raw_imports`
- Une ligne par exécution d’import.
- `status` doit être `success`, `skipped` ou `error`.
- `raw_path` doit pointer vers le snapshot dans `data/raw/<connector>/<timestamp>/`.

### `competitions`
- Éviter de multiplier artificiellement les compétitions.
- Si le domaine métier est une seule compétition logique (ex: FIFA ranking): 1 ligne.

### `events`
- 1 ligne par “publication sportive” retenue.
- IDs lisibles et stables quand possible (ex: `fifa_ranking_95`).

### `participants`
- `participant_id` doit être cohérent avec le type:
  - nation ranking: utiliser le code pays (`ISO3`) directement.
  - club/équipe: ID stable du participant.

### `results`
- Clé `(event_id, participant_id)` unique.
- `rank` positif (`>=1`) ou `NULL`.
- Sur un top 10: exactement 10 lignes par event (sauf égalités métier explicitement acceptées et documentées).

## 5) Règles pays / IDs

- `country_id` = ISO3 standard dès que possible.
- Si la source utilise un code non ISO (ex: `ENG`, `WAL`):
  - conserver le code si nécessaire pour rester fidèle au domaine,
  - documenter l’exception dans le connecteur.

## 6) Checklist qualité avant livraison

- `python -m pipelines.validate` retourne `passed=true`.
- `sources.csv` contient un lien fonctionnel (test HTTP 200).
- `raw_imports.csv` contient la dernière exécution.
- `competition/events/results` respectent les volumes attendus du connecteur.
- `exports/architecture/database_architecture.csv` est régénéré.

## 7) Workflow “nouveau connecteur”

1. Créer `src/connectors/<new_connector>.py`.
2. Implémenter `fetch`, `parse`, `upsert`.
3. Ajouter le connecteur dans `src/connectors/registry.py`.
4. Ajouter la commande dans la doc (`README`).
5. Lancer:
```bash
python -m pipelines.ingest --connector <new_connector> --year <YYYY>
python -m pipelines.validate
python -m pipelines.init_databases
```
6. Vérifier les CSV dans:
  - `data/processed/databases/competition/`
  - `data/processed/databases/reference/`
  - `data/processed/databases/lineage/`

## 8) Cas FIFA (référence actuelle)

- `competition_id` unique: `fifa_ranking`
- `event_id` annuel: `fifa_ranking_YY`
- Une seule publication par année (la plus récente de l’année)
- `participant_id` = code pays
- `results`: top 10 par année

## 8b) Cas FIFA Women Ranking (historique)

- `competition_id` unique: `fifa_women_ranking`
- `event_id` annuel: `fifa_women_ranking_YY`
- une publication retenue par année (la plus récente de l'année)
- `participant_id` = code pays
- `results`: top 10 par année

## 8c) Cas World Rugby Rankings (historique)

- connecteur: `world_rugby_ranking_history`
- compétitions:
  - `world_rugby_men_ranking`
  - `world_rugby_women_ranking`
- `event_id` annuel:
  - `world_rugby_men_ranking_YY`
  - `world_rugby_women_ranking_YY`
- `participant_id` = code pays (code World Rugby / ISO3 selon disponibilité)
- `results`: top 10 par année pour chaque genre
- source API: `https://api.wr-rims-prod.pulselive.com/rugby/v3/rankings/{mru|wru}?date=YYYY-12-31`

## 8d) Cas FIBA Rankings (historique)

- connecteur: `fiba_ranking_history`
- compétitions:
  - `fiba_men_ranking`
  - `fiba_women_ranking`
- `event_id` annuel:
  - `fiba_men_ranking_YY`
  - `fiba_women_ranking_YY`
- `participant_id` = code pays
- `results`: top 10 par année pour chaque genre
- source principale: pages FIBA + endpoint `getgdapfederationsranking`
- seed local fallback/cache: `data/raw/basketball/fiba_rankings_history_seed.csv`

## 8e) Cas ICC Team Rankings (historique, hommes Test/ODI/T20I)

- connecteur: `icc_team_ranking_history`
- compétitions:
  - `icc_men_test_team_ranking`
  - `icc_men_odi_team_ranking`
  - `icc_men_t20i_team_ranking`
- `event_id` annuel:
  - `icc_men_test_team_ranking_YY`
  - `icc_men_odi_team_ranking_YY`
  - `icc_men_t20i_team_ranking_YY`
- `participant_id` = code pays
- `results`: top 10 par année pour chaque format
- source principale: endpoint ICC assets `assets-icc.sportz.io/cricket/v1/ranking`
- seed local fallback/cache: `data/raw/cricket/icc_team_rankings_history_seed.csv`

## 9) Cas Coupe du Monde FIFA (historique)

- `competition_id` unique: `fifa_world_cup`
- `event_id` par édition: `fifa_world_cup_YY` (`30`, `34`, ..., `22`)
- `results`: top 4 par édition (1 à 4), pas de doublons `(event_id, participant_id)`
- `participant_id` = `country_id` (code pays)
- source principale: `https://github.com/openfootball/world-cup`
- seed local recommandé pour stabilité: `data/raw/world_cup/world_cup_top4_seed.csv`

## 9b) Cas Coupe du Monde FIFA feminine (historique)

- connecteur: `fifa_women_world_cup_history`
- `competition_id` unique: `fifa_womens_world_cup`
- `event_id` par édition: `fifa_womens_world_cup_YY`
- `results`: top 4 par édition (1 à 4), pas de doublons `(event_id, participant_id)`
- `participant_id` = `country_id` (code pays)
- seed local: `data/raw/world_cup/womens_world_cup_top4_seed.csv`

## 9c) Cas Coupe du Monde de rugby (historique, hommes + femmes)

- connecteur: `rugby_world_cup_history`
- compétitions:
  - `rugby_world_cup_men`
  - `rugby_world_cup_women`
- `event_id` par édition:
  - `rugby_world_cup_men_YY`
  - `rugby_world_cup_women_YY`
- `results`: top 4 par édition (1 à 4), pas de doublons `(event_id, participant_id)`
- `participant_id` = `country_id` (code pays)
- seeds locaux:
  - `data/raw/world_rugby/rugby_world_cup_top4_seed.csv`
  - `data/raw/world_rugby/womens_rugby_world_cup_top4_seed.csv`

## 9d) Cas Coupe du Monde FIBA Basketball (historique, hommes + femmes)

- connecteur: `fiba_basketball_world_cup_history`
- compétitions:
  - `fiba_basketball_world_cup_men`
  - `fiba_basketball_world_cup_women`
- `event_id` par édition:
  - `fiba_basketball_world_cup_men_YY`
  - `fiba_basketball_world_cup_women_YY`
- `results`: top 4 par édition (1 à 4), pas de doublons `(event_id, participant_id)`
- `participant_id` = `country_id` (code pays)
- seeds locaux:
  - `data/raw/basketball/fiba_world_cup_men_top4_seed.csv`
  - `data/raw/basketball/fiba_world_cup_women_top4_seed.csv`

## 9e) Cas IHF Handball World Championship (historique, hommes + femmes)

- connecteur: `ihf_handball_world_championship_history`
- compétitions:
  - `ihf_handball_world_championship_men`
  - `ihf_handball_world_championship_women`
- `event_id` par édition:
  - `ihf_handball_world_championship_men_YY`
  - `ihf_handball_world_championship_women_YY`
- `results`: top 4 par édition (1 à 4), pas de doublons `(event_id, participant_id)`
- `participant_id` = `country_id` (code pays, incluant codes historiques `URS`, `YUG`, `TCH`, `GDR`, `FRG`)
- seeds locaux:
  - `data/raw/handball/ihf_world_men_handball_championship_top4_seed.csv`
  - `data/raw/handball/ihf_world_women_handball_championship_top4_seed.csv`

## 9f) Cas ICC Cricket World Cup (historique, hommes + femmes)

- connecteur: `icc_cricket_world_cup_history`
- compétitions:
  - `icc_cricket_world_cup_men`
  - `icc_cricket_world_cup_women`
- `event_id` par édition:
  - `icc_cricket_world_cup_men_YY`
  - `icc_cricket_world_cup_women_YY`
- `results`: finale uniquement (rangs 1 et 2) par édition
- `participant_id` = `country_id` (code pays, incluant `ENG` et `WIS`)
- note métier: pas de match officiel 3e place sur la plupart des éditions -> top 2 retenu
- seeds locaux:
  - `data/raw/cricket/icc_cricket_world_cup_men_final_seed.csv`
  - `data/raw/cricket/icc_cricket_world_cup_women_final_seed.csv`

## 10) Cas JO d'été Paris 2024

- `competition_id` unique: `summer_olympics_paris_2024`
- `event_id` par épreuve: `paris2024_<discipline>_<event>`
- source demandée: `data/raw/olympics/keithgalli_results.csv` (audit, pas de lignes 2024)
- source Paris 2024 utilisée: `data/raw/olympics/paris2024_medals_by_event.csv`
- séparation sport / discipline appliquée (ex: `aquatics` > `swimming`)
- `results`: podium par épreuve (`rank` 1/2/3; ties possibles en bronze)
- `participant_id` explicite:
  - athlete: `athlete_<nom_prenom>_<noc>`
  - team: `nation_<noc>`

## 11) Cas JO historiques (KeithGalli)

- connecteur: `olympics_keith_history`
- source: `data/raw/olympics/keithgalli_results.csv` (fallback download depuis GitHub)
- filtre temporel: toutes les éditions `>= --year` (exemple `--year 2000`)
- `competition_id`: `olympics_<summer|winter>_<year>`
- `event_id`: `olympics_<summer|winter>_<year>_<discipline>_<event>`
- `results`: conserver uniquement les lignes avec médaille (`gold`, `silver`, `bronze`)
- `participant_id` explicite:
  - athlete: `athlete_<nom_prenom>_<noc>_<athlete_id>`
  - team fallback: `nation_<noc>`

## 12) Commandes standard (copier/coller)

```bash
python -m pipelines.bootstrap_dimensions
python -m pipelines.ingest --connector fifa_ranking_history --year 2026
python -m pipelines.ingest --connector fifa_women_ranking_history --year 2026
python -m pipelines.ingest --connector fifa_women_world_cup_history --year 2026
python -m pipelines.ingest --connector world_rugby_ranking_history --year 2026
python -m pipelines.ingest --connector rugby_world_cup_history --year 2026
python -m pipelines.ingest --connector fiba_ranking_history --year 2026
python -m pipelines.ingest --connector icc_team_ranking_history --year 2026
python -m pipelines.ingest --connector fiba_basketball_world_cup_history --year 2026
python -m pipelines.ingest --connector ihf_handball_world_championship_history --year 2026
python -m pipelines.ingest --connector icc_cricket_world_cup_history --year 2026
python -m pipelines.validate
python -m pipelines.init_databases
```
