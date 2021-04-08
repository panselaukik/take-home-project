# Take Home Test

Python version used 3.7

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install requirements file in root dir.

```bash
in take-home-project/
python3 -m venv /path/to/new/virtual/environment
pip install -r requirements.txt
python setup.py develop
```

## Usage

```python
data_processing/runner.py

runs using named parameters --race_id and --season_year

Sample Paramaters -

python runner.py --race_id "841" --season_year "2009"

This will create new csv files in dimensional_data/
```