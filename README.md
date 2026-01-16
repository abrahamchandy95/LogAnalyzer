# LogAnalyzer (TigerGraph RESTPP/GPE Log Comparison)

LogAnalyzer compares the performance of **two query variants** by parsing TigerGraph **RESTPP** and **GPE** logs, linking events by request ID, and producing CSV artifacts + an optional plot.

It supports two ways to run:

1. **Zero-argument mode** (recommended): run `loganalyzer` and it uses your `.env`.
2. **CLI mode**: pass `--run ...` arguments explicitly.

---

## Requirements

- Python (project is configured for Python **3.14**)
- A POSIX shell environment (Linux/macOS recommended) for `scripts/bootstrap.sh`

---

## Log Directory Layout (IMPORTANT)

Your run directory must contain **node folders** (by default: `m1 m2 m3 m4`), and each node folder contains log files that match:

- `restpp*`
- `gpe*`

If you only have logs from a single node/cluster, you can set `NODES=m1` and place your logs under that single node folder.

---

## Setup (one-time)

Use the bootstrap script to create a virtual environment and install the project (including dev tools):

```bash
./scripts/bootstrap.sh
```

## Configure `.env`

Create a `.env` file in the repo root and set:

- `OUT_DIR`: where artifacts will be written
- `RUN_1_DIR` / `RUN_2_DIR`: absolute paths to each run’s log directory
- `NODES`: space-separated list of node folder names (e.g. `m1 m2 m3 m4`)
- `BASE_QUERY` / `OPT_QUERY`: the two query names you want to compare
- `OPEN_PLOT`: `1` to open the generated plot automatically (optional)

Example keys (values will be specific to your environment):

```bash
OUT_DIR=/abs/path/to/LogAnalyzer_outputs

RUN_1_DIR=/abs/path/to/run_A_logs
RUN_2_DIR=/abs/path/to/run_B_logs

NODES=m1 m2 m3 m4

BASE_QUERY=your_first_query_name
OPT_QUERY=your_other_query_name

OPEN_PLOT=1
```
Note that the program depends on the regular expressions, which in turn
depends on the Logs.

After you run scripts/bootstrap.sh, the project is installed into the virtual
environment as a command-line tool named `loganalyzer`. 


With your .env configured,
you can simply run loganalyzer from the repo root and it will execute the full 
pipeline and write artifacts to OUT_DIR. 

If you prefer, you can also run it in 
CLI mode using loganalyzer --help to see the available arguments. The command 
name loganalyzer comes from the [project.scripts] section in pyproject.toml, 
and you can rename it there if you want a different executable name.
