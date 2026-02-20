# TODO - Competitions et classements mondiaux

Objectif: prioriser les prochaines ingestions de competitions mondiales et de classements mondiaux pour enrichir `competition/events/results`.

## Deja en place

- [x] FIFA Men's Ranking (`fifa_ranking`)
- [x] FIFA World Cup (`fifa_world_cup`)
- [x] Summer Olympics / Winter Olympics (plusieurs editions)

## TODO priorite haute (nations, couverture mondiale)

### Competitions mondiales

- [x] FIFA Women's World Cup (football) (connecteur `fifa_women_world_cup_history`)
- [x] FIBA Basketball World Cup (men) (connecteur `fiba_basketball_world_cup_history`)
- [x] FIBA Women's Basketball World Cup (connecteur `fiba_basketball_world_cup_history`)
- [x] Rugby World Cup (men) (connecteur `rugby_world_cup_history`)
- [x] Women's Rugby World Cup (connecteur `rugby_world_cup_history`)
- [ ] ICC Cricket World Cup (ODI men)
- [ ] ICC Women's Cricket World Cup
- [ ] World Athletics Championships
- [ ] World Aquatics Championships
- [ ] IHF Handball World Championship (men)
- [ ] IHF Women's Handball World Championship
- [ ] FIVB Volleyball World Championship (men)
- [ ] FIVB Women's Volleyball World Championship

### Classements mondiaux (Top 10)

- [x] Top 10 FIFA Women's World Ranking (connecteur `fifa_women_ranking_history`)
- [x] Top 10 FIBA World Ranking (men) (connecteur `fiba_ranking_history`)
- [x] Top 10 FIBA World Ranking (women) (connecteur `fiba_ranking_history`)
- [x] Top 10 World Rugby Rankings (men) (connecteur `world_rugby_ranking_history`)
- [x] Top 10 World Rugby Rankings (women) (connecteur `world_rugby_ranking_history`)
- [ ] Top 10 ICC Team Rankings (Test, ODI, T20)

## TODO priorite moyenne (mondial, mais modelisation parfois plus complexe)

### Competitions mondiales

- [ ] FIFA Club World Cup (clubs, competition mondiale)
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

## TODO priorite basse (forte complexite data/licence)

### Competitions mondiales

- [ ] Formula 1 World Championship (pilotes + constructeurs)
- [ ] Formula E World Championship
- [ ] Sailing World Championships (selon classes)

### Classements mondiaux (Top 10)

- [ ] Top 10 ATP ranking / WTA ranking (joueurs -> agreger par nation)
- [ ] Top 10 World Sailing rankings (selon classes)

## Regles de selection proposees

- Prioriser les formats `nation vs nation` (plus simple pour `participant_id=country_id`).
- Prioriser les sources ouvertes et versionnees (CSV/JSON publics, GitHub, federation avec API documentee).
- Eviter d'ajouter un connecteur sans garantie sur licence et reproductibilite.
- Pour les rankings individuels (ATP/WTA/BWF/ITTF): documenter la methode d'agregation par pays avant ingestion.
- Standard rankings: stocker le `top 10` uniquement (pas le classement complet) sauf besoin metier explicite.

## Definition of done pour chaque nouvel ajout

- [ ] Connecteur cree dans `src/connectors/<connector>.py`
- [ ] Enregistrement dans `src/connectors/registry.py`
- [ ] Source tracee dans `sources` + `raw_imports`
- [ ] Ingestion OK: `python -m pipelines.ingest --connector <id> --year <YYYY>`
- [ ] `results`: exactement 10 lignes par `event_id` (hors egalite metier documentee)
- [ ] `results.rank`: entier `1..10`
- [ ] Validation OK: `python -m pipelines.validate`
- [ ] Synchronisation CSV OK: `python -m pipelines.init_databases`
- [ ] Documentation mise a jour (`README.md`, playbook si necessaire)
