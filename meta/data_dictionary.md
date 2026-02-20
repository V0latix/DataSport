# Data Dictionary

## countries

| column | description |
|---|---|
| country_id | ISO3 code used as stable country key |
| iso2 | ISO2 code |
| iso3 | ISO3 code duplicate for convenience |
| name_en | Country name in English |
| name_fr | Country name in French if available |

## sports

| column | description |
|---|---|
| sport_id | Slug identifier for sport |
| sport_name | Human-readable sport name |
| sport_slug | Slug version of sport name |
| created_at_utc | UTC timestamp for insertion |

## disciplines

| column | description |
|---|---|
| discipline_id | Slug identifier for discipline |
| discipline_name | Human-readable discipline/event name |
| discipline_slug | Slug version of discipline name |
| sport_id | Parent sport foreign key |
| confidence | Mapping confidence score |
| mapping_source | Mapping origin (heuristic/override) |
| created_at_utc | UTC timestamp for insertion |

## competitions

| column | description |
|---|---|
| competition_id | Deterministic hashed ID |
| sport_id | Sport foreign key |
| name | Competition name |
| season_year | Season year |
| level | Competition level/category |
| start_date | Competition start date |
| end_date | Competition end date |
| source_id | Source foreign key |

## events

| column | description |
|---|---|
| event_id | Deterministic hashed ID |
| competition_id | Competition foreign key |
| discipline_id | Discipline foreign key |
| gender | Event gender category |
| event_class | Class/stage of event |
| event_date | Event date |

## participants

| column | description |
|---|---|
| participant_id | Deterministic hashed ID |
| type | athlete/team/pair |
| display_name | Display name |
| country_id | Country foreign key |

## results

| column | description |
|---|---|
| event_id | Event foreign key |
| participant_id | Participant foreign key |
| rank | Rank/position |
| medal | Medal label |
| score_raw | Raw score payload |
| points_awarded | Points allocated in normalized model |

## sources

| column | description |
|---|---|
| source_id | Source key |
| source_name | Source display name |
| source_type | API/SPARQL/seed type |
| license_notes | License context |
| base_url | Base endpoint |

## raw_imports

| column | description |
|---|---|
| import_id | Import operation key |
| source_id | Source key |
| fetched_at_utc | Import timestamp UTC |
| raw_path | Snapshot folder path |
| status | success/skipped/error |
| error | Error message if any |

## sport_federations

| column | description |
|---|---|
| sport_id | Sport key |
| federation_qid | Wikidata federation QID |
| federation_name | Federation label |
