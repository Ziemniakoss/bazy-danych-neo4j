# Projekt z neo4j

## Przed importem

- uruchmoć bazę danych neo4j z użyciem
```bash
docker run --publish=7474:7474 --publish=7687:7687  --env=NEO4J_AUTH=none --name  my-neo  neo4j
```

## Import danych 

- ustawić ścieżkę do pliku z pełnymi danymi oraz limit importowanych użytkowników w names_extractor.py
- uruchomić names_extractor.py
- uruchomić gh_data_fetcher.py (to może długo chodzić w zależności od limitu userów)
- uruchomić neo4j_importer.py
