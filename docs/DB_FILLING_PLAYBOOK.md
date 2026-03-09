# Playbook - Remplissage des bases DataSport

Ce document définit la façon correcte de remplir les bases pour éviter les incohérences dans `competition` et `lineage`.

## 1) Principes obligatoires

- Toujours passer par le pipeline (`python -m pipelines.*`), jamais par édition manuelle des CSV finaux.
- Toujours conserver la traçabilité: `sources` + `raw_imports` doivent être renseignés pour chaque import.
- Prioriser les sources ouvertes; documenter clairement les limites de licence.
- Garder des IDs stables et lisibles quand possible.
- Exécuter la validation après chaque ingestion.

## 2) Architecture cible

- Base maître: `data/processed/sports_nations.db`
- Bases CSV spécialisées (générées depuis la base maître):
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

### `disciplines`
- Ne pas confondre `sport` (famille), `discipline` (format), `event` (édition datée).
- Exemples cricket:
  - `sport_id=cricket`
  - `discipline_id=cricket-odi`, `cricket-t20`, `cricket-test`
- Exemple athletics:
  - `sport_id=athletics`
  - `discipline_id=athletics_100-m`, `athletics_marathon`

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

## 4b) Exemples concrets par table (anti-erreurs)

### `sources` (1 ligne par connecteur)
```csv
source_id,source_name,source_type,license_notes,base_url
icc_cricket_world_cup_history,ICC Cricket World Competitions Historical Results (ODI/Test/T20/Champions Trophy),csv,Historical seeds curated from ICC/Wikipedia public info,https://www.icc-cricket.com
```

### `raw_imports` (1 ligne par run)
```csv
import_id,source_id,fetched_at_utc,raw_path,status,error
import_xxx,icc_cricket_world_cup_history,2026-03-09T10:29:09+00:00,data/raw/icc_cricket_world_cup_history/20260309T102908Z,success,
```

### `sports` (famille sportive)
```csv
sport_id,sport_name,sport_slug,created_at_utc
cricket,Cricket,cricket,2026-03-09T10:29:08+00:00
```

### `disciplines` (format de pratique)
```csv
discipline_id,discipline_name,discipline_slug,sport_id,confidence,mapping_source,created_at_utc
cricket-odi,Cricket ODI,cricket-odi,cricket,1.0,connector_icc_cricket_world_cup_history,2026-03-09T10:29:08+00:00
cricket-t20,Cricket T20,cricket-t20,cricket,1.0,connector_icc_cricket_world_cup_history,2026-03-09T10:29:08+00:00
cricket-test,Cricket Test,cricket-test,cricket,1.0,connector_icc_cricket_world_cup_history,2026-03-09T10:29:08+00:00
```

### `competitions` (compétition logique)
```csv
competition_id,sport_id,name,season_year,level,start_date,end_date,source_id
icc_cricket_world_cup_men,cricket,ICC Cricket World Cup (ODI Men),,national_team_tournament,1975-12-31,2023-12-31,icc_cricket_world_cup_history
icc_mens_t20_world_cup,cricket,ICC Men's T20 World Cup,,national_team_tournament,2007-12-31,2026-12-31,icc_cricket_world_cup_history
```

### `events` (édition datée d’une compétition)
```csv
event_id,competition_id,discipline_id,gender,event_class,event_date
icc_cricket_world_cup_men_23,icc_cricket_world_cup_men,cricket-odi,men,final_ranking_top4,2023-12-31
icc_mens_t20_world_cup_24,icc_mens_t20_world_cup,cricket-t20,men,final_ranking_top4,2024-12-31
```

### `participants` (nation/équipe/athlète)
```csv
participant_id,type,display_name,country_id
IND,team,India,IND
AUS,team,Australia,AUS
```

### `results` (classement final de l’event)
```csv
event_id,participant_id,rank,medal,score_raw,points_awarded
icc_mens_t20_world_cup_24,IND,1,gold,icc_mens_t20_world_cup_final_rank=1,10.0
icc_mens_t20_world_cup_24,ZAF,2,silver,icc_mens_t20_world_cup_final_rank=2,7.0
icc_mens_t20_world_cup_24,AFG,3,bronze,icc_mens_t20_world_cup_final_rank=3,5.0
icc_mens_t20_world_cup_24,ENG,4,,icc_mens_t20_world_cup_final_rank=4,4.0
```

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

## 9f) Cas ICC Cricket competitions mondiales (historique, ODI/Test/T20/Champions Trophy)

- connecteur: `icc_cricket_world_cup_history`
- compétitions:
  - `icc_cricket_world_cup_men` (ODI, men)
  - `icc_cricket_world_cup_women` (ODI, women)
  - `icc_mens_t20_world_cup` (T20, men)
  - `icc_womens_t20_world_cup` (T20, women)
  - `icc_world_test_championship_men` (Test, men)
  - `icc_champions_trophy_men` (ODI, men)
- `event_id` par édition:
  - `icc_cricket_world_cup_men_YY`
  - `icc_cricket_world_cup_women_YY`
  - `icc_mens_t20_world_cup_YY`
  - `icc_womens_t20_world_cup_YY`
  - `icc_world_test_championship_men_YY`
  - `icc_champions_trophy_men_YY`
- disciplines séparées:
  - `cricket-odi`
  - `cricket-t20`
  - `cricket-test`
- `results`:
  - ODI World Cup (men/women): top 4 (rangs 1 a 4)
  - T20 World Cup (men/women) / Champions Trophy: top 4 (rangs 1 a 4, demi-finalistes inclus)
  - World Test Championship: finalistes (rangs 1 a 2)
- `participant_id` = `country_id` (code pays, incluant `ENG` et `WIS`)
- note métier:
  - ODI World Cup: pas de match officiel 3e place sur plusieurs éditions -> les rangs 3-4 proviennent des demi-finalistes
  - Women's T20 World Cup 2020: demi-finales abandonnées -> finalistes qualifiés via classement de groupe, mais top 4 conservé (2 finalistes + 2 autres demi-finalistes)
  - Champions Trophy 2002: co-vainqueurs Inde / Sri Lanka (deux lignes `rank=1`) + demi-finalistes (rangs 3/4)
- seeds locaux:
  - `data/raw/cricket/icc_cricket_world_cup_men_final_seed.csv`
  - `data/raw/cricket/icc_cricket_world_cup_women_final_seed.csv`
  - `data/raw/cricket/icc_mens_t20_world_cup_final_seed.csv`
  - `data/raw/cricket/icc_womens_t20_world_cup_final_seed.csv`
  - `data/raw/cricket/icc_world_test_championship_men_final_seed.csv`
  - `data/raw/cricket/icc_champions_trophy_men_final_seed.csv`

## 9g) Cas World Athletics Championships (historique, top 3 par discipline)

- connecteur: `world_athletics_championships_history`
- `competition_id` unique: `world_athletics_championships`
- distinction stricte sport / discipline:
  - sport unique: `Athletics`
  - une discipline par epreuve (`100 metres`, `Marathon`, `Pole vault`, etc.)
- `event_id` par edition / genre / discipline:
  - `world_athletics_championships_<YYYY>_<men|women|mixed>_<discipline>`
- `results`: podium uniquement (`rank` 1 a 3) par discipline
- `participant_id`:
  - athlete individuel: `athlete_<nom_prenom>_<noc>`
  - relais/mixed: code pays (`country_id`)
- regle anti-doublon athlete:
  - si un athlete existe deja dans `participants` (match nom + pays), reutiliser son `participant_id`
- seed local:
  - `data/raw/athletics/world_athletics_championships_top3_seed.csv`
  - couverture actuelle: editions >= 2000 (2001, 2003, ..., 2019, 2022, 2023, 2025)

## 9h) Cas World Aquatics Championships (historique, top 3 par epreuve)

- connecteur: `world_aquatics_championships_history`
- `competition_id` unique: `world_aquatics_championships`
- distinction stricte sport / discipline:
  - sport unique: `Aquatics`
  - disciplines au niveau epreuve (ex: `diving-10-m-platform`, `open-water-swimming-10-km`)
- `event_id` par edition / genre / discipline:
  - `world_aquatics_championships_<YYYY>_<men|women|mixed>_<discipline>`
- `results`: podium uniquement (`rank` 1 a 3) par epreuve
- `participant_id`:
  - athlete individuel: `athlete_<nom_prenom>_<noc>`
  - epreuves team/nation: code pays (`country_id`)
- regle anti-doublon athlete:
  - si un athlete existe deja dans `participants` (match nom + pays), reutiliser son `participant_id`
- seed local:
  - `data/raw/aquatics/world_aquatics_championships_top3_seed.csv`
  - couverture actuelle: editions >= 2000 (2001, 2003, ..., 2019, 2022, 2023, 2024, 2025)

## 9i) Cas FIVB Volleyball World Championship (historique, hommes + femmes, top 4)

- connecteur: `fivb_volleyball_world_championship_history`
- competitions:
  - `fivb_volleyball_world_championship_men`
  - `fivb_volleyball_world_championship_women`
- `event_id` par edition:
  - `fivb_volleyball_world_championship_men_YY`
  - `fivb_volleyball_world_championship_women_YY`
- `results`: top 4 (rangs 1 a 4) par edition
- `participant_id` = `country_id` (codes historiques inclus: `URS`, `TCH`, `GDR`, `YUG`, `SCG`)
- seed local:
  - `data/raw/volleyball/fivb_world_championship_men_top4_seed.csv`
  - `data/raw/volleyball/fivb_world_championship_women_top4_seed.csv`
  - couverture actuelle:
    - men: editions 1949 -> 2025
    - women: editions 1952 -> 2025

## 10) Cas JO d'été Paris 2024 (connecteur dédié, optionnel)

- `competition_id` unique: `summer_olympics_paris_2024`
- `event_id` par épreuve: `paris2024_<discipline>_<event>`
- source demandée: `data/raw/olympics/keithgalli_results.csv` (audit, pas de lignes 2024)
- source Paris 2024 utilisée: `data/raw/olympics/paris2024_medals_by_event.csv`
- séparation sport / discipline appliquée (ex: `aquatics` > `swimming`)
- `results`: podium par épreuve (`rank` 1/2/3; ties possibles en bronze)
- `participant_id` explicite:
  - athlete: `athlete_<nom_prenom>_<noc>`
  - team: `nation_<noc>`
- note: ce connecteur est optionnel; le flux JO unifié recommandé est `olympics_keith_history` (section suivante)

## 11) Cas JO historiques (KeithGalli)

- connecteur: `olympics_keith_history`
- source: `data/raw/olympics/keithgalli_results.csv` (fallback download depuis GitHub)
- sources complémentaires intégrées:
  - `data/raw/olympics/paris2024_medals_by_event.csv` (Paris 2024)
  - `data/raw/olympics/winter2026_medals_by_event_seed.csv` (JO hiver 2026, médaillés par épreuve)
- filtre temporel: toutes les éditions `>= --year` (exemple `--year 2000`)
- `competition_id`: `olympics_<summer|winter>`
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
python -m pipelines.ingest --connector world_athletics_championships_history --year 2026
python -m pipelines.ingest --connector world_aquatics_championships_history --year 2026
python -m pipelines.ingest --connector fivb_volleyball_world_championship_history --year 2026
python -m pipelines.validate
python -m pipelines.init_databases
```
