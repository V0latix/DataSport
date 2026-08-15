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

Le projet utilise une base maître + 2 bases spécialisées au format CSV:
- `data/processed/sports_nations.db` (base maître unifiée)
- `data/processed/databases/competition/*.csv` (dimensions, compétitions, events, participants, résultats)
- `data/processed/databases/lineage/*.csv` (provenance `raw_imports`)

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
- discipline utilisée: `rugby-union`
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
- récupère les dates de publication depuis les pages FIBA officielles (hommes/femmes)
- interroge l'endpoint FIBA `getgdapfederationsranking` pour chaque snapshot disponible
- met à jour le seed local `data/raw/basketball/fiba_rankings_history_seed.csv` après fetch réussi
- fallback automatique sur seed local en cas d'échec distant
- conserve une publication par année (la plus récente), puis le `top 10`
- crée deux compétitions:
  - `fiba_men_ranking`
  - `fiba_women_ranking`
- crée des events annuels lisibles: `fiba_men_ranking_25`, `fiba_women_ranking_25`
- exporte les tables normalisées dans `exports/fiba_ranking_history/year=2026/`

### 5e) Ingest ICC Team rankings historiques (hommes + femmes, top 10 nations)

```bash
python -m pipelines.ingest --connector icc_team_ranking_history --year 2026
```

Comportement:
- récupère les classements équipes ICC via l'endpoint rankings utilisé par le site ICC:
  - hommes: Test, ODI, T20I
  - femmes: ODI, T20I
- mode historique annuel: un snapshot par année avec `date=YYYY1231` (de 2000 à `--year`)
- met à jour le seed local `data/raw/cricket/icc_team_rankings_history_seed.csv` après fetch réussi
- fallback automatique sur seed local en cas d'échec distant
- conserve une publication par année (la plus récente), puis le `top 10`
- couverture observée (API ICC):
  - men test: 2000-2025
  - men odi: 2000-2025
  - men t20i: 2011-2025
  - women odi: 2018-2025
  - women t20i: 2018-2025
- note: certaines années ont moins de 10 équipes classées (ex. Test 2000/2009-2012, Women ODI 2020-2021)
- crée cinq compétitions:
  - `icc_men_test_team_ranking`
  - `icc_men_odi_team_ranking`
  - `icc_men_t20i_team_ranking`
  - `icc_women_odi_team_ranking`
  - `icc_women_t20i_team_ranking`
- disciplines normalisées: `cricket-test`, `cricket-odi`, `cricket-t20`
- crée des events annuels lisibles:
  - `icc_men_test_team_ranking_26`
  - `icc_men_odi_team_ranking_26`
  - `icc_men_t20i_team_ranking_26`
  - `icc_women_odi_team_ranking_26`
  - `icc_women_t20i_team_ranking_26`
- exporte les tables normalisées dans `exports/icc_team_ranking_history/year=2026/`

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
- discipline utilisée: `rugby-union`
- `participant_id` est le code pays

### 8c2) Ingest Rugby World Cup Sevens (historique, hommes + femmes)

```bash
python -m pipelines.ingest --connector rugby_world_cup_sevens_history --year 2026
```

Comportement:
- ingère les seeds historiques locaux:
  - `data/raw/world_rugby/rugby_world_cup_sevens_men_top4_seed.csv`
  - `data/raw/world_rugby/rugby_world_cup_sevens_women_top4_seed.csv`
- crée deux compétitions:
  - `rugby_world_cup_sevens_men`
  - `rugby_world_cup_sevens_women`
- crée un event par édition (suffixe `YY`)
- alimente le classement final top 4 par édition
- conserve les égalités de rang quand il n'y a pas de match pour la 3e place (ex: deux équipes classées `rank=3`)
- discipline utilisée: `rugby-sevens`
- `participant_id` est le code pays

### 8c3) Ingest Rugby League World Cup (historique, hommes + femmes)

```bash
python -m pipelines.ingest --connector rugby_league_world_cup_history --year 2026
```

Comportement:
- ingère les seeds historiques locaux:
  - `data/raw/world_rugby/rugby_league_world_cup_men_top4_seed.csv`
  - `data/raw/world_rugby/rugby_league_world_cup_women_top4_seed.csv`
- crée deux compétitions:
  - `rugby_league_world_cup_men`
  - `rugby_league_world_cup_women`
- crée un event par édition (suffixe `YY`)
- alimente le classement final top 4 par édition
- conserve les égalités de rang quand il n'y a pas de match de 3e place (deux équipes `rank=3`)
- discipline utilisée: `rugby-league`
- `participant_id` est le code pays quand disponible, sinon un code équipe stable

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

### 8e) Ingest IHF Handball World Championship (historique, hommes + femmes)

```bash
python -m pipelines.ingest --connector ihf_handball_world_championship_history --year 2026
```

Comportement:
- ingère les seeds historiques locaux:
  - `data/raw/handball/ihf_world_men_handball_championship_top4_seed.csv`
  - `data/raw/handball/ihf_world_women_handball_championship_top4_seed.csv`
- crée deux compétitions:
  - `ihf_handball_world_championship_men`
  - `ihf_handball_world_championship_women`
- crée un event par édition (suffixe `YY`)
- alimente le classement final top 4 par édition (1er, 2e, 3e, 4e)
- `participant_id` est le code pays (avec mapping historique pour `URS`, `YUG`, `TCH`, `GDR`, `FRG`)

### 8e2) Ingest IIHF Ice Hockey World Championship (historique, hommes + femmes)

```bash
python -m pipelines.ingest --connector iihf_ice_hockey_world_championship_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/ice_hockey/iihf_ice_hockey_world_championship_top4_seed.csv`
  - seed reproductible via: `data/raw/ice_hockey/build_iihf_ice_hockey_world_championship_seed.py`
- cree deux competitions:
  - `iihf_ice_hockey_world_championship_men`
  - `iihf_ice_hockey_world_championship_women`
- sport/discipline:
  - sport `ice-hockey`
  - discipline `ice-hockey`
- cree un event par edition (`<competition_id>_<YYYY>`)
- alimente le classement final top 4 par edition (profil strict `1,2,3,4`)
- couverture actuelle post-2000:
  - hommes: `2001` -> `2026`, hors `2020` annulee
  - femmes: `2001` -> `2025`, hors annees non disputees/annulees; `2026` future en novembre
- `participant_id` est le code pays

### 8e3) Ingest FIG Artistic Gymnastics World Championships (historique, podiums par agrès)

```bash
python -m pipelines.ingest --connector fig_artistic_gymnastics_world_championships_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/gymnastics/fig_artistic_gymnastics_world_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/gymnastics/build_fig_artistic_gymnastics_world_championships_seed.py`
- cree la competition:
  - `fig_artistic_gymnastics_world_championships`
- sport/discipline:
  - sport parent `gymnastics`
  - disciplines `artistic-gymnastics-*` par agres/format: all-around, team, floor, vault, bars, beam, rings, pommel horse
- cree un event par edition + genre + discipline:
  - `fig_artistic_gymnastics_world_championships_<YYYY>_<men|women>_<discipline_key>`
- stocke les podiums source en conservant les egalites:
  - profils acceptes: `1,2,3`, `1,2,2`, `1,1,3`, `1,2,3,3`, `1,1,3,3`, `1,1,1,1`
- couverture actuelle post-2000:
  - `2001` -> `2025`, 250 events, 760 resultats
  - annees sans edition mondiale senior artistique ou sans medailles publiees: `2004`, `2008`, `2012`, `2016`, `2020`, `2024`, `2026`
- participants:
  - `type=athlete` pour les epreuves individuelles
  - `type=team` pour les epreuves par equipes nationales
- notes source:
  - les podiums proviennent des tables medalistes Wikipedia par edition annuelle
  - `AIN` et `RGF` conservent les entites source non-ISO; les codes IOC historiques sont normalises vers ISO alpha-3 quand possible
  - scope strict `year > 2000`

### 8e4) Ingest FIS Alpine World Ski Championships (historique, podiums ski alpin)

```bash
python -m pipelines.ingest --connector fis_alpine_world_ski_championships_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/skiing/fis_alpine_world_ski_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/skiing/build_fis_alpine_world_ski_championships_seed.py`
- cree la competition:
  - `fis_alpine_world_ski_championships`
- sport/discipline:
  - sport parent `skiing`
  - discipline existante `alpine-skiing`
- cree un event par edition + genre + epreuve:
  - `fis_alpine_world_ski_championships_<YYYY>_<men|women|mixed>_<event_key>`
- couvre les epreuves:
  - downhill, super-G, giant slalom, slalom, combined, mixed team, team combined, parallel giant slalom
- stocke les podiums source en conservant les egalites:
  - profils acceptes: `1,2,3`, `1,2,2`, `1,1,3`, `1,2,3,3`
- couverture actuelle post-2000:
  - `2001` -> `2025`, 144 events, 434 resultats
  - championnats biennaux uniquement; `2026` sans edition FIS Alpine Worlds
- participants:
  - `type=athlete` pour les epreuves individuelles
  - `type=team` pour le mixed team et le team combined
- notes source:
  - les podiums proviennent de la page Wikipedia des medalistes FIS Alpine
  - l'edition 2003 est completee depuis la page annuelle 2003 car la page liste a ses cellules de medailles vides pour cette edition
  - scope strict `year > 2000`

### 8e5) Ingest World Figure Skating Championships (historique, podiums patinage artistique)

```bash
python -m pipelines.ingest --connector world_figure_skating_championships_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/skating/world_figure_skating_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/skating/build_world_figure_skating_championships_seed.py`
- cree la competition:
  - `world_figure_skating_championships`
- sport/discipline:
  - sport parent `skating`
  - discipline existante `figure-skating`
- cree un event par edition + genre + epreuve:
  - `world_figure_skating_championships_<YYYY>_<men|women|mixed>_<event_key>`
- couvre les epreuves:
  - men's singles, women's singles, pairs, ice dance
- stocke les podiums source en profil strict:
  - profils acceptes: `1,2,3`
- couverture actuelle post-2000:
  - `2001` -> `2026`, hors `2020` annulee
  - 100 events, 300 resultats
- participants:
  - `type=athlete` pour les simples H/F
  - `type=team` pour pairs et ice dance
- notes source:
  - les podiums proviennent des pages Wikipedia annuelles des championnats
  - les pages legacy 2001-2008 sont parsees depuis les tables de resultats annuels
  - scope strict `year > 2000`

### 8e6) Ingest World Weightlifting Championships (historique, podiums haltérophilie)

```bash
python -m pipelines.ingest --connector world_weightlifting_championships_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/weightlifting/world_weightlifting_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/weightlifting/build_world_weightlifting_championships_seed.py`
- cree la competition:
  - `world_weightlifting_championships`
- sport/discipline:
  - sport parent `weightlifting`
  - discipline existante `weightlifting`
- cree un event par edition + genre + categorie de poids:
  - `world_weightlifting_championships_<YYYY>_<men|women>_<event_key>`
- couvre les categories historiques:
  - flyweight, bantamweight, featherweight, lightweight, middleweight, light heavyweight, middle heavyweight, first heavyweight, heavyweight, super heavyweight
- stocke les podiums source en profil strict:
  - profils acceptes: `1,2,3`
- couverture actuelle post-2000:
  - `2001` -> `2025`, hors annees sans edition mondiale retenue dans la source (`2004`, `2008`, `2012`, `2016`, `2020`, `2026`)
  - 332 events, 996 resultats
- participants:
  - `type=athlete`
- notes source:
  - les podiums proviennent des pages Wikipedia de listes des medailles hommes/femmes
  - les tableaux source correspondent aux podiums Total par categorie de poids
  - les codes pays source sont conserves, y compris les codes CIO non ISO (`INA`, `NGR`, `TPE`, etc.)
  - scope strict `year > 2000`

### 8e7) Ingest World Karate Championships (historique, podiums karaté)

```bash
python -m pipelines.ingest --connector world_karate_championships_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/karate/world_karate_championships_top4_seed.csv`
  - seed reproductible via: `data/raw/karate/build_world_karate_championships_seed.py`
- cree la competition:
  - `world_karate_championships`
- sport/discipline:
  - sport parent `karate`
  - discipline existante `karate`
- cree un event par edition + genre + epreuve:
  - `world_karate_championships_<YYYY>_<men|women>_<event_key>`
- couvre les epreuves seniors WKF:
  - kata, team kata, kumite par categorie de poids, kumite open, team kumite selon les editions
- stocke les podiums source avec double bronze:
  - profils acceptes: `1,2,3,3`
- couverture actuelle post-2000:
  - `2002` -> `2025`, championnats biennaux ou decales selon la source
  - 192 events, 768 resultats
- participants:
  - `type=athlete` pour les epreuves individuelles
  - `type=team` pour team kata et team kumite
- notes source:
  - les podiums proviennent des pages Wikipedia annuelles des championnats
  - Para Karate et le Team World Championship/Cup separe sont exclus
  - les codes sportifs non ISO sont conserves (`ANA`, `RKF`, `WKF1`, `WKF2`, `YUG`, etc.)
  - scope strict `year > 2000`

### 8e8) Ingest World Boxing Championships (historique, podiums boxe)

```bash
python -m pipelines.ingest --connector world_boxing_championships_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/boxing/world_boxing_championships_top4_seed.csv`
  - seed reproductible via: `data/raw/boxing/build_world_boxing_championships_seed.py`
- cree trois competitions:
  - `iba_mens_world_boxing_championships`
  - `iba_womens_world_boxing_championships`
  - `world_boxing_championships`
- sport/discipline:
  - sport parent `boxing`
  - discipline existante `boxing`
- cree un event par competition + edition + genre + categorie de poids:
  - `<competition_id>_<YYYY>_<men|women>_<event_key>`
- stocke les podiums source:
  - profils acceptes: `1,2,3,3`
  - profil `1,2,3` accepte quand la source officielle ne liste qu'un bronze
- couverture actuelle post-2000:
  - `2001` -> `2025`, selon les editions IBA et World Boxing disponibles dans les sources
  - 262 events, 1047 resultats
- participants:
  - `type=athlete`
- notes source:
  - les podiums proviennent des pages Wikipedia annuelles IBA hommes/femmes et World Boxing 2025
  - les doubles bronzes compactes dans certaines cellules sources sont separes par athlete/pays
  - les codes sportifs non ISO sont conserves (`BUL`, `GER`, `PHI`, `SCG`, `TBF`, `TPE`, etc.)
  - scope strict `year > 2000`

### 8e9) Ingest Biathlon World Championships (historique, podiums biathlon)

```bash
python -m pipelines.ingest --connector biathlon_world_championships_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/biathlon/biathlon_world_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/biathlon/build_biathlon_world_championships_seed.py`
- cree la competition:
  - `biathlon_world_championships`
- sport/discipline:
  - sport parent `biathlon`
  - discipline existante `biathlon`
- cree un event par edition + genre + epreuve:
  - `biathlon_world_championships_<YYYY>_<men|women|mixed>_<event_key>`
- couvre les epreuves mondiales:
  - individuel, sprint, poursuite, mass start, relais H/F, relais mixte et single mixed relay
- stocke les podiums source:
  - profils acceptes: `1,2,3`
  - profil `1,3` accepte pour la poursuite femmes 2003, ou l'argent n'est pas attribue dans la source
- couverture actuelle post-2000:
  - `2001` -> `2025`, selon les tableaux event medalists de la page source
  - 216 events, 647 resultats
- participants:
  - `type=athlete` pour les epreuves individuelles
  - `type=team` pour les relais, avec membres conserves dans `score_raw`
- notes source:
  - les podiums proviennent de la page Wikipedia centrale Biathlon World Championships
  - les relais sont modelises par nation; les equipes d'une meme nation dans le meme event sont distinguees par leurs membres
  - les codes sportifs non ISO sont conserves (`GER`, `SLO`, `BUL`, etc.)
  - scope strict `year > 2000`

### 8e10) Ingest World Curling Championships (historique, podiums curling)

```bash
python -m pipelines.ingest --connector world_curling_championships_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/curling/world_curling_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/curling/build_world_curling_championships_seed.py`
- cree quatre competitions:
  - `world_mens_curling_championship`
  - `world_womens_curling_championship`
  - `world_mixed_curling_championship`
  - `world_mixed_doubles_curling_championship`
- sport/discipline:
  - sport parent existant `curling`
  - discipline existante `curling`
- cree un event par competition + edition:
  - `<competition_id>_<YYYY>`
- stocke les podiums source:
  - profil strict `1,2,3`
- couverture actuelle post-2000:
  - hommes: `2001` -> `2026`
  - femmes: `2001` -> `2026`
  - mixed: `2015` -> `2024`, selon editions tenues
  - mixed doubles: `2008` -> `2026`
  - 76 events, 228 resultats
- participants:
  - `type=team`, `participant_id` = code pays/equipe nationale
- notes source:
  - les podiums proviennent de la page Wikipedia centrale World Curling Championships
  - les epreuves wheelchair sont exclues du connecteur
  - les codes sportifs non ISO/domaines sont conserves (`SCO`, `ENG`, `GER`, `SUI`, etc.)
  - scope strict `year > 2000`

### 8e11) Ingest Davis Cup (historique, finalistes tennis par nations)

```bash
python -m pipelines.ingest --connector davis_cup_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/tennis/davis_cup_finalists_seed.csv`
  - seed reproductible via: `data/raw/tennis/build_davis_cup_seed.py`
- cree la competition:
  - `davis_cup`
- sport/discipline:
  - sport parent existant `tennis`
  - discipline existante `tennis`
- cree un event par edition:
  - `davis_cup_<YYYY>`
- stocke les finalistes source:
  - profil strict `1,2`
  - la Davis Cup ne publie pas de match pour la 3e place; aucun bronze n'est cree
- couverture actuelle post-2000:
  - `2001` -> `2025`, avec `2020-21` normalise en `2021`
  - 24 events, 48 resultats
- participants:
  - `type=team`, `participant_id` = code pays/equipe nationale
- notes source:
  - les finalistes proviennent de la page Wikipedia List of Davis Cup champions
  - `RTF` est conserve comme code source non ISO pour Russian Tennis Federation
  - scope strict `year > 2000`

### 8e12) Ingest Billie Jean King Cup (historique, finalistes tennis par nations)

```bash
python -m pipelines.ingest --connector billie_jean_king_cup_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/tennis/billie_jean_king_cup_finalists_seed.csv`
  - seed reproductible via: `data/raw/tennis/build_billie_jean_king_cup_seed.py`
- cree la competition:
  - `billie_jean_king_cup`
- sport/discipline:
  - sport parent existant `tennis`
  - discipline existante `tennis`
- cree un event par edition:
  - `billie_jean_king_cup_<YYYY>`
- stocke les finalistes source:
  - profil strict `1,2`
  - la Billie Jean King Cup ne publie pas de match pour la 3e place; aucun bronze n'est cree
- couverture actuelle post-2000:
  - `2001` -> `2025`, avec `2020-21` normalise en `2021`
  - 24 events, 48 resultats
- participants:
  - `type=team`, `participant_id` = code pays/equipe nationale
- notes source:
  - les finalistes proviennent de la page Wikipedia List of Billie Jean King Cup champions
  - `RTF` est conserve comme code source non ISO pour Russian Tennis Federation
  - scope strict `year > 2000`

### 8e13) Ingest World Netball Cup (historique, top 4 netball)

```bash
python -m pipelines.ingest --connector world_netball_cup_history --year 2026
```

Comportement:
- ingere le seed historique local:
  - `data/raw/netball/world_netball_cup_top4_seed.csv`
  - seed reproductible via: `data/raw/netball/build_world_netball_cup_seed.py`
- cree la competition:
  - `world_netball_cup`
- sport/discipline:
  - nouveau sport `netball`
  - nouvelle discipline `netball`
- cree un event par edition:
  - `world_netball_cup_<YYYY>`
- stocke le classement final top 4:
  - profil strict `1,2,3,4`
  - medailles `gold`, `silver`, `bronze`; le 4e rang n'a pas de medaille
- couverture actuelle post-2000:
  - `2003` -> `2023`
  - 6 events, 24 resultats
  - l'edition `2027` est exclue car future
- participants:
  - `type=team`, `participant_id` = code pays/equipe nationale
- notes source:
  - les classements proviennent de la table tournoi Wikipedia Netball World Cup
  - `ENG` et `ZAF` sont conserves comme codes equipes nationales
  - scope strict `year > 2000`

### 8f) Ingest ICC Cricket competitions mondiales (historique, ODI/Test/T20/Champions Trophy)

```bash
python -m pipelines.ingest --connector icc_cricket_world_cup_history --year 2026
```

Comportement:
- ingère les seeds historiques locaux:
  - `data/raw/cricket/icc_cricket_world_cup_men_final_seed.csv`
  - `data/raw/cricket/icc_cricket_world_cup_women_final_seed.csv`
  - `data/raw/cricket/icc_mens_t20_world_cup_final_seed.csv`
  - `data/raw/cricket/icc_womens_t20_world_cup_final_seed.csv`
  - `data/raw/cricket/icc_world_test_championship_men_final_seed.csv`
  - `data/raw/cricket/icc_champions_trophy_men_final_seed.csv`
- crée six compétitions:
  - `icc_cricket_world_cup_men` (ODI)
  - `icc_cricket_world_cup_women` (ODI)
  - `icc_mens_t20_world_cup` (T20)
  - `icc_womens_t20_world_cup` (T20)
  - `icc_world_test_championship_men` (Test)
  - `icc_champions_trophy_men` (ODI)
- crée un event par édition (suffixe `YY`) pour chaque compétition
- sépare les disciplines cricket par format:
  - `cricket-odi`
  - `cricket-t20`
  - `cricket-test`
- résultats:
  - ODI World Cup (men/women): top 4 (1er à 4e)
  - T20 World Cup (men/women) / Champions Trophy: top 4 (1er à 4e)
  - World Test Championship: finalistes (1er, 2e)
- `participant_id` est le code pays (incluant `ENG`, `WIS`)

### 8g) Ingest World Athletics Championships (historique, top 3 par discipline)

```bash
python -m pipelines.ingest --connector world_athletics_championships_history --year 2026
```

Comportement:
- ingère le seed historique local:
  - `data/raw/athletics/world_athletics_championships_top3_seed.csv`
  - couverture actuelle: éditions >= 2000 (2001, 2003, ..., 2019, 2022, 2023, 2025)
- crée une compétition unique:
  - `world_athletics_championships`
- distingue strictement `sport` vs `discipline`:
  - sport unique: `Athletics`
  - une discipline par épreuve (ex: `100 metres`, `Pole vault`, `Marathon`)
- crée un event par édition / genre / discipline (ex: `world_athletics_championships_2023_men_100-metres`)
- alimente uniquement le podium (`rank` 1/2/3) par discipline
- `participant_id`:
  - athlète individuel: `athlete_<nom_prenom>_<noc>`
  - relais/mixed: code pays (`country_id`)
- contrainte d'upsert: réutilise un `participant_id` athlète déjà existant (match nom + pays), n'ajoute pas de doublons

### 8h) Ingest World Aquatics Championships (historique, top 3 par epreuve)

```bash
python -m pipelines.ingest --connector world_aquatics_championships_history --year 2026
```

Comportement:
- ingère le seed historique local:
  - `data/raw/aquatics/world_aquatics_championships_top3_seed.csv`
  - couverture actuelle: éditions >= 2000 (2001, 2003, ..., 2019, 2022, 2023, 2024, 2025)
- crée une compétition unique:
  - `world_aquatics_championships`
- distingue strictement `sport` vs `discipline`:
  - sport unique: `Aquatics`
  - disciplines au niveau épreuve (ex: `diving-10-m-platform`, `open-water-swimming-10-km`)
- crée un event par édition / genre / discipline-épreuve
- alimente uniquement le podium (`rank` 1/2/3) par épreuve
- `participant_id`:
  - athlète individuel: `athlete_<nom_prenom>_<noc>`
  - épreuves par nation/équipe: code pays (`country_id`)
- contrainte d'upsert: réutilise un `participant_id` athlète déjà existant (match nom + pays), n'ajoute pas de doublons

### 8i) Ingest FIVB Volleyball World Championship (historique, hommes + femmes, top 4)

```bash
python -m pipelines.ingest --connector fivb_volleyball_world_championship_history --year 2026
```

Comportement:
- ingère les seeds historiques locaux:
  - `data/raw/volleyball/fivb_world_championship_men_top4_seed.csv`
  - `data/raw/volleyball/fivb_world_championship_women_top4_seed.csv`
  - couverture actuelle:
    - men: éditions `1949 -> 2025`
    - women: éditions `1952 -> 2025`
- crée deux compétitions:
  - `fivb_volleyball_world_championship_men`
  - `fivb_volleyball_world_championship_women`
- crée un event par édition (suffixe `YY`) pour chaque genre
- alimente le classement final top 4 par édition (1er, 2e, 3e, 4e)
- `participant_id` est le code pays (avec mapping historique `URS`, `TCH`, `GDR`, `YUG`, `SCG`)

### 8j) Ingest WBSC Baseball/Softball World Championships (historique, hommes + femmes, top 4)

```bash
python -m pipelines.ingest --connector wbsc_baseball_softball_world_championship_history --year 2026
```

Comportement:
- ingère les seeds historiques locaux:
  - `data/raw/baseball/wbsc_baseball_world_cup_men_top4_seed.csv`
  - `data/raw/baseball/wbsc_womens_baseball_world_cup_top4_seed.csv`
  - `data/raw/baseball/wbsc_mens_softball_world_cup_top4_seed.csv`
  - `data/raw/baseball/wbsc_womens_softball_world_cup_top4_seed.csv`
- crée quatre compétitions:
  - `wbsc_baseball_world_cup_men`
  - `wbsc_womens_baseball_world_cup`
  - `wbsc_mens_softball_world_cup`
  - `wbsc_womens_softball_world_cup`
- crée un event par édition (suffixe `YY`) pour chaque compétition
- alimente les résultats top 4 par édition (avec gestion des égalités historiques, ex: édition 1976 en softball hommes)
- sport utilisé: `baseball`
- disciplines utilisées:
  - `baseball` (sport `baseball`)
  - `softball` (discipline du sport `baseball`)
- `participant_id` est le code pays (avec mapping pour codes non-ISO usuels, ex: `Chinese Taipei -> TPE`)

### 8k) Ingest BWF World Championships (historique, 5 disciplines, top 4)

```bash
python -m pipelines.ingest --connector bwf_world_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/badminton/bwf_world_championships_top4_by_discipline_seed.csv`
- cree la competition:
  - `bwf_world_championships`
- cree 5 disciplines badminton:
  - `badminton_mens-singles`
  - `badminton_womens-singles`
  - `badminton_mens-doubles`
  - `badminton_womens-doubles`
  - `badminton_mixed-doubles`
- cree un event par edition et discipline (`bwf_world_championships_<YYYY>_<discipline_key>`)
- stocke le top 4 par event (rangs `1, 2, 3, 3`; deux bronzes)
- sport/discipline:
  - sport `badminton`
  - disciplines par epreuve (`singles/doubles/mixed`)
- `participant_id`:
  - singles: `athlete_<nom>_<country_code>`
  - doubles: `pair_<nom1_nom2>_<country_code>`

### 8k2) Ingest BWF Thomas Cup + Uber Cup (historique, equipes nationales, top 4)

```bash
python -m pipelines.ingest --connector bwf_thomas_uber_cup_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/badminton/thomas_uber_cup_top4_seed.csv`
  - seed reproductible via: `data/raw/badminton/build_thomas_uber_cup_seed.py`
- cree 2 competitions:
  - `bwf_thomas_cup`
  - `bwf_uber_cup`
- cree un event par edition:
  - `bwf_thomas_cup_<YYYY>`
  - `bwf_uber_cup_<YYYY>`
- stocke un top 4 avec bronze partage:
  - profil attendu `1,2,3,3`
- sport/discipline:
  - sport `badminton`
  - discipline `badminton-team` (format equipes nationales)
- `participant_id`:
  - code pays (`country_id`)
- couverture observee avec la source actuelle:
  - `2002` -> `2024` (scope strict `year > 2000`)
  - edition `2026` ignoree tant que les finalistes/demi-finalistes ne sont pas complets

### 8l) Ingest ITTF World Table Tennis Championships (historique, 7 disciplines, top 4/podium)

```bash
python -m pipelines.ingest --connector ittf_world_table_tennis_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/table_tennis/ittf_world_table_tennis_championships_podium_seed.csv`
- cree la competition:
  - `ittf_world_table_tennis_championships`
- cree 7 disciplines:
  - `table-tennis_mens-team`
  - `table-tennis_womens-team`
  - `table-tennis_mens-singles`
  - `table-tennis_womens-singles`
  - `table-tennis_mens-doubles`
  - `table-tennis_womens-doubles`
  - `table-tennis_mixed-doubles`
- cree un event par edition et discipline (`ittf_world_table_tennis_championships_<YYYY>_<discipline_key>`)
- preserve les profils historiques du podium (ex: top 3 simple, doubles medailles partagees, egalites argent/bronze selon editions)
- sport/discipline:
  - sport `table-tennis`
  - disciplines par format (`team/singles/doubles/mixed`)
- `participant_id`:
  - team (par equipes nationales): code pays
  - singles: `athlete_<nom>_<country_code>`
  - doubles/mixte: `pair_<nom1_nom2>_<country_code>`

### 8m) Ingest Cyclisme route majeur (historique, top 3, hommes + competitions femmes existantes)

```bash
python -m pipelines.ingest --connector uci_road_cycling_major_competitions_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/cycling/uci_road_cycling_major_competitions_top3_seed.csv`
  - seed reproductible via: `data/raw/cycling/build_uci_road_cycling_major_competitions_seed.py`
- couvre les 9 competitions de reference demandees:
  - `uci_road_world_championships` (2 disciplines: `road-race`, `time-trial`)
  - `tour_de_france`, `giro_d_italia`, `vuelta_a_espana`
  - `milan_san_remo`, `tour_of_flanders`, `paris_roubaix`, `liege_bastogne_liege`, `il_lombardia`
- ajoute les competitions femmes disponibles dans les memes families:
  - `tour_de_france_femmes`, `giro_d_italia_women`, `vuelta_a_espana_femenina`
  - `milan_san_remo_women`, `tour_of_flanders_women`, `paris_roubaix_femmes`, `liege_bastogne_liege_women`
- cree un event par competition/annee/discipline/genre:
  - `<competition_id>_<YYYY>_<discipline_key>_<gender>`
- stocke un top 3 strict par event (`rank` = `1,2,3`)
- sport/discipline:
  - sport `cycling`
  - disciplines `road-race` et `time-trial`
- note integrite seed:
  - editions avec podium incomplet (disqualifications/annulations historiques) exclues du seed pour conserver le profil strict top 3

### 8n) Ingest UCI Track Cycling World Championships (historique, top 3, hommes + femmes depuis 2000)

```bash
python -m pipelines.ingest --connector uci_track_cycling_world_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/cycling/uci_track_world_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/cycling/build_uci_track_world_championships_seed.py`
- cree la competition:
  - `uci_track_world_championships`
- couvre les disciplines piste historiques (apparitions/disparitions conservees par annee):
  - `track-sprint`, `track-team-sprint`, `track-keirin`
  - `track-individual-pursuit`, `track-team-pursuit`
  - `track-points-race`, `track-scratch`, `track-madison`, `track-omnium`, `track-elimination-race`
  - `track-time-trial-1km`, `track-time-trial-500m`
- cree un event par annee/discipline/genre:
  - `uci_track_world_championships_<YYYY>_<discipline_key>_<gender>`
- stocke un podium top 3 par event avec profils historiques autorises:
  - standard: `1,2,3`
  - exceptions source: `1,2` ou `1,3`
- sport/discipline:
  - sport `cycling`
  - disciplines piste specialisees (pas de nouveau sport cree)

### 8n-b) Ingest UCI Cyclo-cross World Championships (historique, elite H/F, post-2000)

```bash
python -m pipelines.ingest --connector uci_cyclocross_world_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/cycling/uci_cyclocross_world_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/cycling/build_uci_cyclocross_world_championships_seed.py`
- cree la competition:
  - `uci_cyclocross_world_championships`
- cree un event par edition + genre elite:
  - `uci_cyclocross_world_championships_<YYYY>_<men|women>_elite`
- stocke le podium elite:
  - profil attendu strict: `1,2,3`
- sport/discipline:
  - sport `cycling`
  - discipline `cycling-cyclo-cross`
- couverture observee avec la source actuelle:
  - `2001` -> `2026`
  - categories retenues: elite hommes et elite femmes
  - categories exclues: U23 et juniors
  - scope strict `year > 2000`

### 8n-c) Ingest UCI Mountain Bike World Championships (historique, elite H/F, post-2000)

```bash
python -m pipelines.ingest --connector uci_mountain_bike_world_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/cycling/uci_mountain_bike_world_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/cycling/build_uci_mountain_bike_world_championships_seed.py`
- cree la competition:
  - `uci_mountain_bike_world_championships`
- couvre les epreuves elite hommes/femmes:
  - `xco` (cross-country)
  - `downhill`
- cree un event par edition + epreuve + genre elite:
  - `uci_mountain_bike_world_championships_<YYYY>_<xco|downhill>_<men|women>_elite`
- stocke un podium top 3 strict par event:
  - profil attendu: `1,2,3`
- sport/discipline:
  - sport `cycling`
  - disciplines `cycling-mountain-bike-cross-country` et `cycling-mountain-bike-downhill`
- couverture observee avec la source actuelle:
  - `2001` -> `2025`
  - categories retenues: elite cross-country et downhill hommes/femmes
  - categories exclues: U23, juniors, relais, trials, marathon, eliminator, e-MTB et four-cross
  - scope strict `year > 2000`

### 8n-d) Ingest Sailing World Championships (historique, podium par classe, post-2000)

```bash
python -m pipelines.ingest --connector world_sailing_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/sailing/world_sailing_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/sailing/build_world_sailing_championships_seed.py`
- cree la competition:
  - `world_sailing_championships`
- couvre les editions combinees ISAF/Sailing World Championships:
  - `2003`, `2007`, `2011`, `2014`, `2018`, `2023`
- cree un event par edition + classe + genre:
  - `world_sailing_championships_<YYYY>_<class_key>_<men|women|mixed>`
- stocke un podium top 3 strict par event:
  - profil attendu: `1,2,3`
- sport/discipline:
  - sport `sailing`
  - disciplines par classe (`sailing-470`, `sailing-49er`, `sailing-ilca-7`, `sailing-iqfoil`, etc.)
- participants:
  - `type=team`, representant l'entree/equipage medaliste avec `country_id`
- couverture observee avec la source actuelle:
  - 68 events, 204 resultats
  - 21 classes, dont les classes para sailing presentes en 2023
  - championnats annuels isoles par classe exclus
  - scope strict `year > 2000`

### 8n-e) Ingest World Archery Championships (historique, recurve + compound, post-2000)

```bash
python -m pipelines.ingest --connector world_archery_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/archery/world_archery_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/archery/build_world_archery_championships_seed.py`
- cree la competition:
  - `world_archery_championships`
- couvre les editions outdoor:
  - `2001` -> `2025`
- couvre les disciplines:
  - `archery-recurve`
  - `archery-compound`
- cree un event par edition + discipline + epreuve + genre:
  - `world_archery_championships_<YYYY>_<recurve|compound>_<individual|team>_<men|women|mixed>`
- stocke un podium top 3 strict par event:
  - profil attendu: `1,2,3`
- participants:
  - `type=athlete` pour les epreuves individuelles
  - `type=team` pour les epreuves par equipes et mixtes
- notes source:
  - `RAF` conserve l'entite source `Russian Archery Federation` en 2021
  - championnats indoor, field, youth et para exclus
  - scope strict `year > 2000`

### 8n-f) Ingest World Triathlon Championship Series (historique, classements finaux H/F)

```bash
python -m pipelines.ingest --connector world_triathlon_championship_series_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/triathlon/world_triathlon_championship_series_top3_seed.csv`
  - seed reproductible via: `data/raw/triathlon/build_world_triathlon_championship_series_seed.py`
- cree la competition:
  - `world_triathlon_championship_series`
- couvre l'ere Championship Series:
  - `2009` -> `2025`
  - pages sources ITU World Championship Series / ITU World Triathlon Series / World Triathlon Championship Series
- cree un event annuel par genre:
  - `world_triathlon_championship_series_<YYYY>_<men|women>`
- stocke un podium top 3 strict par event:
  - profil attendu: `1,2,3`
- sport/discipline:
  - sport `triathlon`
  - discipline `triathlon`
- notes source:
  - la saison 2020 est incluse comme championnat du monde single-race COVID depuis la page annuelle de la serie, avec `score_raw` en temps de course
  - les autres annees stockent les points finaux WTCS/ITU dans `score_raw`
  - scope strict `year > 2000`

### 8n-g) Ingest FEI World Championships (historique, podiums equestrian)

```bash
python -m pipelines.ingest --connector fei_world_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/equestrian/fei_world_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/equestrian/build_fei_world_championships_seed.py`
- cree la competition:
  - `fei_world_championships`
- couvre le format global FEI post-2000:
  - FEI World Equestrian Games: `2002`, `2006`, `2010`, `2014`, `2018`
  - FEI World Championships: `2022`
- cree un event par edition + discipline + epreuve + genre:
  - `fei_world_championships_<YYYY>_<discipline_key>_<event_key>_<men|women|mixed>`
- stocke un podium top 3 strict par event:
  - profil attendu: `1,2,3`
- sport/discipline:
  - sport `equestrian`
  - disciplines reutilisees: dressage, eventing, jumping
  - disciplines ajoutees: driving, endurance, para-dressage, reining, vaulting
- notes source:
  - les podiums proviennent des tables medalistes Wikipedia par edition
  - les epreuves annulees ou abandonnees sont exclues
  - les epreuves open sont codees `gender=mixed`; vaulting hommes/femmes conserve `men`/`women`
  - scope strict `year > 2000`

### 8n-h) Ingest World Amateur Team Championships (historique, golf par nations)

```bash
python -m pipelines.ingest --connector world_amateur_team_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/golf/world_amateur_team_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/golf/build_world_amateur_team_championships_seed.py`
- cree la competition:
  - `world_amateur_team_championships`
- couvre les trophees par genre:
  - hommes: Eisenhower Trophy
  - femmes: Espirito Santo Trophy
- couverture observee:
  - `2002` -> `2025`
  - edition `2020` exclue car annulee COVID
- cree un event par edition + genre:
  - `world_amateur_team_championships_<YYYY>_<men|women>`
- stocke le podium source en conservant les egalites:
  - profils acceptes: `1,2,3`, `1,2,2`, `1,2,3,3`, `1,2,3,3,3`
- sport/discipline:
  - sport `golf`
  - discipline `golf`
- participants:
  - `type=team`
  - `participant_id=country_code`
- notes source:
  - les podiums proviennent des tables Wikipedia Eisenhower Trophy et Espirito Santo Trophy
  - les equipes constitutives comme `England` et `Scotland` conservent leurs codes existants (`ENG`, `SCO`)
  - scope strict `year > 2000`

### 8o) Ingest UCI Road World Nation Ranking (historique, top 10 nations)

```bash
python -m pipelines.ingest --connector uci_road_nation_ranking_history --year 2026
```

Comportement:
- recupere les snapshots UCI DataRide route via:
  - `GetDisciplineSeasons` (`disciplineId=10`)
  - `RankingsDiscipline` (selection `World Ranking` + `Nation ranking`)
  - `ObjectRankings` (table classements)
- met a jour le seed local:
  - `data/raw/cycling/uci_road_nation_rankings_history_seed.csv`
  - seed reproductible via: `data/raw/cycling/build_uci_road_nation_rankings_seed.py`
- cree la competition:
  - `uci_road_world_nation_ranking`
- cree un event annuel:
  - `uci_road_world_nation_ranking_<YYYY>`
- stocke un top 10 strict par event (`rank` = `1..10`)
- sport/discipline:
  - sport `cycling`
  - discipline `road-race`
- couverture observee avec la source UCI actuelle:
  - `2015` -> `2026`
  - annees manquantes detectees par le connecteur: `2009-2014` (pas de ranking mondial nations route publie sur ces saisons dans DataRide)

### 8p) Ingest World Judo Championships (historique, podium par categorie de poids, post-2000)

```bash
python -m pipelines.ingest --connector world_judo_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/judo/world_judo_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/judo/build_world_judo_championships_seed.py`
- cree la competition:
  - `world_judo_championships`
- cree un event par edition + genre + categorie de poids:
  - `world_judo_championships_<YYYY>_<gender>_<discipline_key>`
- stocke le podium par categorie de poids:
  - profils attendus: `1,2,3,3` (double bronze) et cas rare `1,1,3,3` (double or sans argent)
- sport/discipline:
  - sport `judo`
  - une discipline par categorie de poids (hommes/femmes)
- couverture observee avec la source actuelle:
  - `2001` -> `2025` (scope strict `year > 2000`)

### 8q) Ingest World Wrestling Championships (historique, freestyle + greco-romaine, podium par categorie de poids, post-2000)

```bash
python -m pipelines.ingest --connector world_wrestling_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/wrestling/world_wrestling_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/wrestling/build_world_wrestling_championships_seed.py`
- cree 2 competitions:
  - `world_wrestling_championships_freestyle` (hommes + femmes)
  - `world_wrestling_championships_greco_roman` (hommes)
- cree un event par edition + genre + categorie de poids:
  - `<competition_id>_<YYYY>_<gender>_<weight_class>`
- stocke le podium par categorie de poids:
  - profils attendus: `1,2,3`, `1,2,3,3` (double bronze) et cas rare `1,1,3,3` (double or)
- sport/discipline:
  - sport `wrestling`
  - disciplines `wrestling-freestyle` et `wrestling-greco-roman`
- couverture observee avec la source actuelle:
  - `2001` -> `2025` (scope strict `year > 2000`)

### 8r) Ingest World Rowing Championships (historique, podium par epreuve, post-2000)

```bash
python -m pipelines.ingest --connector world_rowing_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/rowing/world_rowing_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/rowing/build_world_rowing_championships_seed.py`
- cree la competition:
  - `world_rowing_championships`
- cree un event par edition + code d'epreuve:
  - `world_rowing_championships_<YYYY>_<discipline_key>`
- stocke le podium par epreuve:
  - profils attendus: `1`, `1,2`, `1,2,3`
- sport/discipline:
  - sport `rowing`
  - disciplines rowing prefixees par format (`rowing-single-sculls`, `rowing-lightweight-single-sculls`, `rowing-pr1-single-sculls`, etc.)
- couverture observee avec la source actuelle:
  - `2001` -> `2025`
  - annees manquantes detectees par le connecteur: `2020`, `2021`
  - scope strict `year > 2000`

### 8r-b) Ingest ICF Canoe Sprint/Slalom World Championships (historique, podium par epreuve, post-2000)

```bash
python -m pipelines.ingest --connector icf_canoe_world_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/canoe/icf_canoe_world_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/canoe/build_icf_canoe_world_championships_seed.py`
- cree 2 competitions:
  - `icf_canoe_sprint_world_championships`
  - `icf_canoe_slalom_world_championships`
- cree un event par edition + discipline + genre + epreuve:
  - `<competition_id>_<YYYY>_<sprint|slalom>_<gender>_<event_key>`
- stocke le podium par epreuve:
  - profils attendus: `1,2,3`, `1,2,3,3` et cas source rare `1,1,3` (double or sans argent)
- sport/discipline:
  - sport `canoe`
  - disciplines `canoe-sprint` et `canoe-slalom`
- couverture observee avec la source actuelle:
  - sprint: `2001` -> `2025`
  - slalom: `2002` -> `2025`
  - Paracanoe exclu; scope strict `year > 2000`

### 8r-c) Ingest FIE World Championships (historique, epee/foil/sabre, post-2000)

```bash
python -m pipelines.ingest --connector fie_world_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/fencing/fie_world_championships_top3_seed.csv`
  - seed reproductible via: `data/raw/fencing/build_fie_world_championships_seed.py`
- cree la competition:
  - `fie_world_championships`
- cree un event par edition + genre + arme + format:
  - `fie_world_championships_<YYYY>_<gender>_<individual|team>_<epee|foil|sabre>`
- stocke le podium par epreuve:
  - individuel: profil attendu `1,2,3,3` (double bronze)
  - equipe: profil attendu `1,2,3`
- sport/discipline:
  - sport `fencing`
  - disciplines existantes reutilisees: `epee`, `foil`, `sabre`
- couverture observee avec la source actuelle:
  - `2001` -> `2025`
  - annees sans edition retenue: `2020`, `2021`, `2024`, `2026`
  - scope strict `year > 2000`

### 8r-d) Ingest World Taekwondo Championships (historique, podium par categorie, post-2000)

```bash
python -m pipelines.ingest --connector world_taekwondo_championships_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/taekwondo/world_taekwondo_championships_top4_seed.csv`
  - seed reproductible via: `data/raw/taekwondo/build_world_taekwondo_championships_seed.py`
- cree la competition:
  - `world_taekwondo_championships`
- cree un event par edition + genre + categorie:
  - `world_taekwondo_championships_<YYYY>_<gender>_<weight_class>`
- stocke le podium par categorie:
  - profil attendu standard: `1,2,3,3`
  - profil officiel simple bronze accepte: `1,2,3`
  - events officiels exclus si la page source publie un profil non standard ou le meme athlete plusieurs fois dans le meme event
- sport/discipline:
  - sport `taekwondo`
  - discipline existante reutilisee: `taekwondo`
- couverture observee avec la source actuelle:
  - `2001` -> `2025`
  - annees sans edition senior retenue: `2020`, `2021`, `2024`, `2026`
  - events retenus: `199`
  - events exclus pour anomalie source: `2001 men feather`, `2003 women welter`, `2005 men bantam`, `2005 women bantam`, `2007 men heavy`, `2009 men light`, `2009 women middle`, `2011 women heavy`
  - scope strict `year > 2000`

### 8s) Ingest Formula 1 World Championship (classement final top 10 pilotes + constructeurs, post-2000)

```bash
python -m pipelines.ingest --connector formula1_world_championship_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/formula1/formula1_world_standings_top10_seed.csv`
  - seed reproductible via: `data/raw/formula1/build_formula1_world_standings_seed.py`
- cree 2 competitions:
  - `formula1_drivers_world_championship`
  - `formula1_constructors_world_championship`
- cree un event annuel par competition:
  - `formula1_drivers_world_championship_<YYYY>`
  - `formula1_constructors_world_championship_<YYYY>`
- stocke un top 10 strict par event:
  - profil attendu `1,2,3,4,5,6,7,8,9,10`
- sport/discipline:
  - sport `motorsport`
  - discipline `formula-one`
- couverture observee avec la source actuelle:
  - `2001` -> `2025`
  - annee courante exclue tant que la saison n'est pas complete (ex: `2026`)

### 8t) Ingest Formula E Championship (classement final top 10 pilotes + equipes, post-2000)

```bash
python -m pipelines.ingest --connector formulae_world_championship_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/formulae/formulae_world_standings_top10_seed.csv`
  - seed reproductible via: `data/raw/formulae/build_formulae_world_standings_seed.py`
- cree 2 competitions:
  - `formulae_drivers_world_championship`
  - `formulae_teams_world_championship`
- cree un event annuel par competition:
  - `formulae_drivers_world_championship_<YYYY>`
  - `formulae_teams_world_championship_<YYYY>`
- stocke un top 10 strict par event:
  - profil attendu `1,2,3,4,5,6,7,8,9,10`
- sport/discipline:
  - sport `motorsport`
  - discipline `formula-e`
- couverture observee avec la source actuelle:
  - `2015` -> `2025`
  - annee courante exclue tant que la saison n'est pas complete (ex: `2026`)
  - note source historique: quand une table equipes ne liste pas explicitement les equipes a 0 point, le seed complete jusqu'au rang 10 avec les entrants de la saison (points `0`)

### 8u) Ingest FIH Hockey World Cup (historique, top 4 men/women, post-2000)

```bash
python -m pipelines.ingest --connector fih_hockey_world_cup_history --year 2026
```

Comportement:
- ingere le seed local:
  - `data/raw/hockey/fih_hockey_world_cup_top4_seed.csv`
  - seed reproductible via: `data/raw/hockey/build_fih_hockey_world_cup_seed.py`
- cree 2 competitions:
  - `fih_hockey_world_cup_men`
  - `fih_hockey_world_cup_women`
- cree un event par edition:
  - `fih_hockey_world_cup_men_<YYYY>`
  - `fih_hockey_world_cup_women_<YYYY>`
- stocke un top 4 strict par event:
  - profil attendu `1,2,3,4`
- sport/discipline:
  - sport `hockey`
  - discipline `hockey` (reutilisee, pas de doublon)
- couverture observee avec la source actuelle:
  - hommes: `2002`, `2006`, `2010`, `2014`, `2018`, `2023`
  - femmes: `2002`, `2006`, `2010`, `2014`, `2018`, `2022`
  - scope strict `year > 2000`

### 9) Ingest JO d'été Paris 2024 (connecteur dédié, optionnel)

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

Note:
- ce connecteur reste disponible, mais le flux recommandé est `olympics_keith_history` (section suivante), qui intègre désormais Paris 2024 dans `olympics_summer`.

### 10) Ingest JO historiques (jusqu'à une année cible)

```bash
python -m pipelines.ingest --connector olympics_keith_history --year 2000
```

Comportement:
- ingère les résultats historiques du dataset KeithGalli
- charge les éditions Summer + Winter depuis `--year` (ex: 2000)
- crée une compétition par type JO:
  - `olympics_summer`
  - `olympics_winter`
- intègre Paris 2024 dans `olympics_summer` via `data/raw/olympics/paris2024_medals_by_event.csv`
- intègre les JO d'hiver 2026 dans `olympics_winter` via `data/raw/olympics/winter2026_medals_by_event_seed.csv` (médaillés par épreuve, top 3)
- crée un event par épreuve et ne conserve que les résultats médaillés (or/argent/bronze)
- `event_id` porte le niveau édition+épreuve (ex: `olympics_summer_2020_athletics_100m-men`)
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
