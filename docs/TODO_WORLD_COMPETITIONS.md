# TODO - Competitions et classements mondiaux

Objectif: prioriser les prochaines ingestions mondiales pour enrichir `competition/events/results`.

## Etat actuel (deja ingere)

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
- [x] Summer/Winter Olympics historiques (`olympics_keith_history`)
- [x] Paris 2024 Summer Olympics (`paris_2024_summer_olympics`)

### Classements mondiaux (Top 10)

- [x] FIFA Men's Ranking (`fifa_ranking_history`)
- [x] FIFA Women's Ranking (`fifa_women_ranking_history`)
- [x] FIBA World Ranking (men) (`fiba_ranking_history`)
- [x] FIBA World Ranking (women) (`fiba_ranking_history`)
- [x] World Rugby Rankings (men) (`world_rugby_ranking_history`)
- [x] World Rugby Rankings (women) (`world_rugby_ranking_history`)

## Prochaine vague (priorite haute)

### Competitions mondiales

- [ ] World Athletics Championships
- [ ] World Aquatics Championships
- [ ] FIVB Volleyball World Championship (men)
- [ ] FIVB Women's Volleyball World Championship

### Classements mondiaux (Top 10)

- [ ] Top 10 ICC Team Rankings (Test, ODI, T20)

## Backlog (priorite moyenne)

### Competitions mondiales

- [ ] FIFA Club World Cup (clubs)
- [ ] ICC Champions Trophy
- [ ] Rugby World Cup Sevens
- [ ] World Baseball Classic
- [ ] BWF World Championships (badminton)
- [ ] ITTF World Team Championships (table tennis)
- [ ] UCI Road World Championships (cyclisme)
- [ ] World Judo Championships
- [ ] World Wrestling Championships

### Classements mondiaux (Top 10)

- [ ] Top 10 World Baseball Softball Confederation rankings
- [ ] Top 10 BWF World Ranking (joueurs/paires -> agreger par nation)
- [ ] Top 10 ITTF World Ranking (joueurs -> agreger par nation)
- [ ] Top 10 UCI Nation Ranking (route)

## Backlog (priorite basse)

### Competitions mondiales

- [ ] Formula 1 World Championship (pilotes + constructeurs)
- [ ] Formula E World Championship
- [ ] Sailing World Championships (selon classes)

### Classements mondiaux (Top 10)

- [ ] Top 10 ATP ranking / WTA ranking (joueurs -> agreger par nation)
- [ ] Top 10 World Sailing rankings (selon classes)

## Regles de selection

- Prioriser les formats `nation vs nation` (simple pour `participant_id=country_id`).
- Prioriser les sources ouvertes et versionnees (CSV/JSON publics, GitHub, federation avec API documentee).
- Eviter les connecteurs sans garantie licence/reproductibilite.
- Pour les rankings individuels (ATP/WTA/BWF/ITTF): documenter d'abord la methode d'agregation par pays.
- Standard rankings: stocker le `top 10` uniquement (sauf besoin metier explicite).

## Definition of done (nouvel ajout)

- [ ] Connecteur cree dans `src/connectors/<connector>.py`
- [ ] Enregistrement dans `src/connectors/registry.py`
- [ ] Source tracee dans `sources` + `raw_imports`
- [ ] Ingestion OK: `python -m pipelines.ingest --connector <id> --year <YYYY>`
- [ ] Validation OK: `python -m pipelines.validate`
- [ ] Synchronisation CSV OK: `python -m pipelines.init_databases`
- [ ] Documentation mise a jour (`README.md`, playbook si necessaire)
