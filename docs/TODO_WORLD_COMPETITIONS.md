# TODO - Competitions et classements mondiaux

Objectif: prioriser les prochaines ingestions mondiales pour enrichir `competition/events/results`.

## Etat actuel (deja ingere)

### Derniers ajouts (2026-02-20 -> 2026-03-21)

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
- [x] Rugby World Cup Sevens (men/women, top 4 avec egalites de 3e conservees) ajoute via `rugby_world_cup_sevens_history` (2026-03-18)
- [x] Rugby League World Cup (men/women, top 4 avec egalites de 3e conservees) ajoute via `rugby_league_world_cup_history` (2026-03-18)
- [x] WBSC Baseball World Cup + Women's Baseball World Cup + Men's Softball World Cup + Women's Softball World Cup (top 4, hommes/femmes) ajoutes via `wbsc_baseball_softball_world_championship_history` (2026-03-18)
- [x] BWF World Championships (badminton, 5 disciplines: SH/SF/DH/DF/DM, top 4 par event) ajoute via `bwf_world_championships_history` (2026-03-19)
- [x] ITTF World Table Tennis Championships (7 disciplines: equipe H/F, simple H/F, double H/F, double mixte; podium historique) ajoute via `ittf_world_table_tennis_championships_history` (2026-03-19)
- [x] Cyclisme route majeur (UCI Road Worlds route/contre-la-montre, 3 Grands Tours, 5 Monuments; top 3 strict; hommes + competitions femmes existantes) ajoute via `uci_road_cycling_major_competitions_history` (2026-03-19)
- [x] UCI Track Cycling World Championships (cyclisme sur piste, top 3 H/F depuis 2000, 12 disciplines avec apparition/disparition historique conservee) ajoute via `uci_track_cycling_world_championships_history` (2026-03-20)
- [x] UCI Road World Nation Ranking (cyclisme sur route, top 10 nations hommes; couverture 2015-2026 selon disponibilite UCI DataRide) ajoute via `uci_road_nation_ranking_history` (2026-03-20)
- [x] World Judo Championships (podium par categorie de poids H/F, post-2000) ajoute via `world_judo_championships_history` (2026-03-20)
- [x] World Wrestling Championships (freestyle + greco-romaine, podium par categorie de poids, post-2000) ajoute via `world_wrestling_championships_history` (2026-03-20)
- [x] Formula 1 World Championship (classements finaux top 10 pilotes + constructeurs, post-2000) ajoute via `formula1_world_championship_history` (2026-03-21)
- [x] Formula E World Championship (classements finaux top 10 pilotes + equipes, post-2000) ajoute via `formulae_world_championship_history` (2026-03-21)
- [x] FIH Hockey World Cup (men/women, top 4 par edition, post-2000) ajoute via `fih_hockey_world_cup_history` (2026-03-22)

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
- [x] Rugby World Cup Sevens (`rugby_world_cup_sevens_history`)
- [x] Rugby League World Cup (`rugby_league_world_cup_history`)
- [x] WBSC Baseball World Cup (men) (`wbsc_baseball_softball_world_championship_history`)
- [x] WBSC Women's Baseball World Cup (`wbsc_baseball_softball_world_championship_history`)
- [x] WBSC Men's Softball World Cup (`wbsc_baseball_softball_world_championship_history`)
- [x] WBSC Women's Softball World Cup (`wbsc_baseball_softball_world_championship_history`)
- [x] BWF World Championships (badminton, 5 disciplines) (`bwf_world_championships_history`)
- [x] ITTF World Table Tennis Championships (7 disciplines) (`ittf_world_table_tennis_championships_history`)
- [x] UCI Road World Championships (cyclisme, route + contre-la-montre, H/F) (`uci_road_cycling_major_competitions_history`)
- [x] UCI Track Cycling World Championships (cyclisme sur piste, H/F) (`uci_track_cycling_world_championships_history`)
- [x] World Judo Championships (`world_judo_championships_history`)
- [x] World Wrestling Championships (`world_wrestling_championships_history`)
- [x] Formula 1 World Championship (pilotes + constructeurs) (`formula1_world_championship_history`)
- [x] Formula E World Championship (`formulae_world_championship_history`)
- [x] FIH Hockey World Cup (men) (`fih_hockey_world_cup_history`)
- [x] FIH Hockey World Cup (women) (`fih_hockey_world_cup_history`)
- [ ] BWF Sudirman Cup (equipes nationales mixtes)
- [ ] Thomas Cup (badminton, equipes nationales hommes)
- [ ] Uber Cup (badminton, equipes nationales femmes)
- [ ] World Rowing Championships
- [ ] ICF Canoe Sprint World Championships
- [ ] ICF Canoe Slalom World Championships
- [ ] FIE World Championships (fencing, epee/foil/sabre)
- [ ] World Taekwondo Championships
- [ ] UCI Cyclo-cross World Championships
- [ ] UCI Mountain Bike World Championships
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
- [x] Top 10 UCI Nation Ranking (route, hommes, couverture actuelle 2015-2026; pas de ranking mondial nations route expose avant 2015 dans UCI DataRide)
- [ ] FIH World Ranking (men)
- [ ] FIH World Ranking (women)
- [ ] UCI Track Nation Ranking (si expose par UCI DataRide)
- [ ] Top 10 ATP ranking / WTA ranking (joueurs -> agreger par nation)
- [ ] Top 10 BWF ranking / ITTF ranking (joueurs -> agreger par nation)
- [ ] Top 10 World Triathlon ranking (athletes -> agreger par nation)
- [ ] Top 10 World Sailing rankings (selon classes)

### Sports deja presents dans `sports.csv` mais encore sans couverture competition dediee

- [x] hockey (FIH World Cup integree via `fih_hockey_world_cup_history`)
- [ ] rowing
- [ ] canoe
- [ ] fencing
- [ ] taekwondo
- [ ] sailing
- [ ] archery
- [ ] triathlon
- [ ] equestrian
- [ ] golf
- [x] motorsport hors F1 (Formula E integree via `formulae_world_championship_history`)


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
