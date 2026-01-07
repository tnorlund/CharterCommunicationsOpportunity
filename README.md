# CharterCommunicationsOpportunity

IMDB Actor Comparison: Compares movie ratings for Bill Murray and Owen Wilson.

## Requirements

- Python 3.9+
- pandas
- requests

## Installation

### Option 1: Using pip with pyproject.toml

```bash
# Create a virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the package
pip install -e .
```

### Option 2: Install dependencies directly

```bash
python3 -m venv venv
source venv/bin/activate

pip install pandas requests
```

## Running the Script

```bash
# After activating your virtual environment
python imdb_compare.py
```

The script will:
1. Download IMDB's Non-Commercial Datasets (~1.5GB total) to `./imdb_data/`
2. Process the TSV files to find Bill Murray and Owen Wilson's filmographies
3. Calculate and display average ratings for:
   - Movies they starred in together
   - Movies Bill Murray starred in alone
   - Movies Owen Wilson starred in alone

### Custom Data Directory

You can specify a different data directory:

```bash
IMDB_DATA_DIR=/path/to/data python imdb_compare.py
```

## Data Sources

This script uses [IMDB Non-Commercial Datasets](https://developer.imdb.com/non-commercial-datasets/):
- `name.basics.tsv.gz` - Actor names and IDs
- `title.basics.tsv.gz` - Movie titles
- `title.principals.tsv.gz` - Cast information
- `title.ratings.tsv.gz` - Movie ratings

Data is cached locally after the first download.
