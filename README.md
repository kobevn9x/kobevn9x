- 👋 Hi, I’m @kobevn9x
- 👀 I’m interested in Coder, Data Analysis
- 🌱 I’m currently learning SQL
- ⚡ Fun fact: Like Code but dont know code

## JSON ingest tool

Use `json_event_ingest.py` to load SECS-like JSON payloads (like the samples in the prompt) into a SQLite table with the columns `STREAM`, `FUNTIONC`, `CEID`, `RPTID`, `EQPID`, `LOTID`, `CARIERID`, `JIGID`, `MATID`, and `MATERIALID`. Missing values are stored as `NULL`.

```bash
python json_event_ingest.py payloads.json --db events.db --table events
```

The `payloads.json` file may contain one or more JSON payloads separated by whitespace; each payload can be either a JSON array or a JSON string containing that array.

<!---
kobevn9x/kobevn9x is a ✨ special ✨ repository because its `README.md` (this file) appears on your GitHub profile.
You can click the Preview link to take a look at your changes.
--->
