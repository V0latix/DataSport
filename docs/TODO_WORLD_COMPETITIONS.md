# TODO - Competitions et classements mondiaux

Objectif: prioriser les prochaines ingestions mondiales pour enrichir `competition/events/results`.

## Etat actuel (deja ingere)

### Derniers ajouts (2026-02-20 -> 2026-03-09)

- [x] IHF Handball World Championship (men/women) ajoute via `ihf_handball_world_championship_history` (commit `959b7ac`, 2026-02-20)
- [x] ICC Cricket World Cup (ODI men/women) ajoute via `icc_cricket_world_cup_history` (commits `eda104c` et `3376ab8`, 2026-02-20 -> 2026-02-21)
- [x] ICC Men's Team Rankings (Test/ODI/T20I) ajoute via `icc_team_ranking_history` (commit `675bebc`, 2026-02-20)
- [x] Harmonisation disciplines/sports sur competitions deja ingerees (rugby, fiba, fifa women, cricket, handball) (commit `79d7145`, 2026-02-21)
- [x] World Athletics Championships (top 3 par discipline) ajoute via `world_athletics_championships_history` (2026-02-24)
- [x] JO unifies: Paris 2024 integre dans `olympics_summer` + JO hiver 2026 ajoutes dans `olympics_winter` (medailles par epreuve) via `olympics_keith_history` (2026-02-24)
- [x] World Aquatics Championships (top 3 par epreuve) ajoute via `world_aquatics_championships_history` (2026-02-25)
- [x] FIVB Volleyball Men's World Championship (top 4 par edition) ajoute via `fivb_volleyball_world_championship_history` (2026-03-09)
- [x] FIVB Women's Volleyball World Championship (top 4 par edition) ajoute via `fivb_volleyball_world_championship_history` (2026-03-09)
- [x] Competitions mondiales ICC hommes splittees par format (ODI World Cup, T20 World Cup, World Test Championship, Champions Trophy) via `icc_cricket_world_cup_history` (2026-03-09)
- [x] ICC Women's T20 World Cup (top 4 par edition) ajoute via `icc_cricket_world_cup_history` (2026-03-09)
- [x] ICC Team Rankings (men Test/ODI/T20I + women ODI/T20I, top 10) mis a jour via `icc_team_ranking_history` (2026-03-09)
- [x] ICC Team Rankings historiques annuels etendus (2000-2025 selon disponibilite API) via `icc_team_ranking_history` (2026-03-18)

### Competitions mondiales

- [x] FIFA World Cup (`world_cup_history`)
- [x] FIFA Women's World Cup (`fifa_women_world_cup_history`)
- [x] Rugby World Cup (men) (`rugby_world_cup_history`)
- [x] Rugby World Cup (women) (`rugby_world_cup_history`)
- [x] FIBA Basketball World Cup (men) (`fiba_basketball_world_cup_history`)
- [x] FIBA Women's Basketball World Cup (`fiba_basketball_world_cup_history`)
- [x] IHF Handball World Championship (men) (`ihf_handball_world_championship_history`)
- [x] IHF Women's Handball World Championship (`ihf_handball_world_championship_history`)
- [x] ICC Cricket World Cup (ODI men) (`icc_cricket_world_cup_history`)
- [x] ICC Women's Cricket World Cup (`icc_cricket_world_cup_history`)
- [x] ICC Men's T20 World Cup (`icc_cricket_world_cup_history`)
- [x] ICC Women's T20 World Cup (`icc_cricket_world_cup_history`)
- [x] ICC World Test Championship (men) (`icc_cricket_world_cup_history`)
- [x] World Athletics Championships (`world_athletics_championships_history`)
- [x] Summer/Winter Olympics historiques (`olympics_keith_history`)
- [x] Paris 2024 Summer Olympics (`paris_2024_summer_olympics`)
- [x] World Aquatics Championships (`world_aquatics_championships_history`)
- [x] FIVB Volleyball World Championship (men) (`fivb_volleyball_world_championship_history`)
- [x] FIVB Women's Volleyball World Championship (`fivb_volleyball_world_championship_history`)
- [x] ICC Champions Trophy (`icc_cricket_world_cup_history`)
- [ ] Rugby World Cup Sevens
- [ ] World Baseball Classic
- [ ] BWF World Championships (badminton)
- [ ] ITTF World Team Championships (table tennis)
- [ ] UCI Road World Championships (cyclisme)
- [ ] World Judo Championships
- [ ] World Wrestling Championships
- [ ] Formula 1 World Championship (pilotes + constructeurs)
- [ ] Formula E World Championship
- [ ] Sailing World Championships (selon classes)


### Classements mondiaux (Top 10)

- [x] FIFA Men's Ranking (`fifa_ranking_history`)
- [x] FIFA Women's Ranking (`fifa_women_ranking_history`)
- [x] FIBA World Ranking (men) (`fiba_ranking_history`)
- [x] FIBA World Ranking (women) (`fiba_ranking_history`)
- [x] World Rugby Rankings (men) (`world_rugby_ranking_history`)
- [x] World Rugby Rankings (women) (`world_rugby_ranking_history`)
- [x] ICC Men's Team Rankings (Test, ODI, T20I) (`icc_team_ranking_history`)
- [x] ICC Women's Team Rankings (ODI, T20I) (`icc_team_ranking_history`)
- [ ] Top 10 World Baseball Softball Confederation rankings
- [ ] Top 10 BWF World Ranking (joueurs/paires -> agreger par nation)
- [ ] Top 10 ITTF World Ranking (joueurs -> agreger par nation)
- [ ] Top 10 UCI Nation Ranking (route)
- [ ] Top 10 ATP ranking / WTA ranking (joueurs -> agreger par nation)
- [ ] Top 10 World Sailing rankings (selon classes)


### Qualite des donnees `competition/events`

- [x] Bien dissocier les disciplines/formats du cricket (Test, ODI, T20I, T10, The Hundred, etc.) dans `sports`/`competitions`/`events`.
- [x] Faire une distinction stricte entre `competition` JO (ex: `olympics_summer`) et `event` JO (ex: `olympics_summer_athletics_100m__men_2020`) pour eviter les melanges de granularite.
- [ ] Régler le probleme des disciplines, il y a encore des doublons et des choses qui ne sont pas des disciplines

## Regles de selection

- Prioriser les formats `nation vs nation` (simple pour `participant_id=country_id`).
- Prioriser les sources ouvertes et versionnees (CSV/JSON publics, GitHub, federation avec API documentee).
- Eviter les connecteurs sans garantie licence/reproductibilite.
- Pour les rankings individuels (ATP/WTA/BWF/ITTF): documenter d'abord la methode d'agregation par pays.
- Standard rankings: stocker le `top 10` uniquement (sauf besoin metier explicite).

## Definition of done (nouvel ajout)

- Connecteur cree dans `src/connectors/<connector>.py`
- Enregistrement dans `src/connectors/registry.py`
- Source tracee dans `sources` + `raw_imports`
- Ingestion OK: `python -m pipelines.ingest --connector <id> --year <YYYY>`
- Validation OK: `python -m pipelines.validate`
- Synchronisation CSV OK: `python -m pipelines.init_databases`
- Documentation mise a jour (`README.md`, playbook si necessaire, TODO)
