from process_data import ProcessData
from dataclasses import dataclass
import argparse


@dataclass
class Runner:
    parser = argparse.ArgumentParser()

    parser.add_argument('--race_id', type=str)
    parser.add_argument('--season_year', type=str)

    args = parser.parse_args()

    def __init__(self):
        if self.args.race_id and self.args.season_year:
            ProcessData(race_id=self.args.race_id, season_year=self.args.season_year).run_main()


if __name__ == '__main__':
    Runner()
