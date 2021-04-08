from data_processing_base_utils import Base
from dataclasses import dataclass
import pandas as pd
import pandasql as ps


@dataclass
class ProcessData(Base):
    def __init__(self, race_id, season_year):
        super().__init__()
        self.race_id = race_id
        self.year = season_year
        if self.sanity_passed:
            print('Sanitation was successful')
            print(self.sanity_report)
        else:
            print('Sanitation checks Failed')
            print(self.sanity_report)

    def run_main(self):
        self.fix_driver_codes()
        if self.race_id:
            self.find_avg_time_spent_at_pit_stop(race_id=self.race_id)
        if self.year:
            self.find_youngest_and_oldest(year=self.year)

    # - Insert the missing code(e.g: ALO for Alonso) for all drivers
    def fix_driver_codes(self):
        if 'drivers.csv' not in self.invalid_csv_list:
            df = pd.read_csv(f"../data/drivers.csv")
            df["code"] = df['surname'].astype(str).str[0:3]
            df['code'] = df['code'].str.upper()
            df.to_csv('../dimensional_data/driver_codes.csv', sep='\t', encoding='utf-8')
            print("\n>> Driver codes fixed <<")

    # What was the average time each driver spent at the pit stop for any given race?
    def find_avg_time_spent_at_pit_stop(self, race_id: str):
        if 'pit_stops.csv' not in self.invalid_csv_list:
            df = pd.read_csv(f"../data/pit_stops.csv")
            pd.DataFrame(ps.sqldf(
                f"SELECT RACEID, DRIVERID, AVG(DURATION) AS AVG_TIME_SPENT_MILLISECONDS FROM df WHERE RACEID ={race_id} GROUP BY "
                f"DRIVERID")).to_csv(f"../dimensional_data/avg_time_spent_in_pit_stop_race_{race_id}.csv")
            print(f"\n>> Average pit-stops successfully calculated for {race_id} saved in dimensional_data"
                  f"/avg_time_spent_in_pit_stop_race_{race_id}.csv <<")

    def find_youngest_and_oldest(self, year: str):
        if 'drivers.csv' not in self.invalid_csv_list:
            drivers = pd.read_csv(f"../data/drivers.csv")
            results = pd.read_csv(f"../data/results.csv")
            races = pd.read_csv(f"../data/races.csv")
            start_end_of_season = pd.DataFrame(ps.sqldf(f"SELECT MIN(DATE) AS SEASON_START, MAX(DATE) AS SEASON_END "
                                                        f"FROM races "
                                                        f"WHERE YEAR = {year}"))
            # Find all the drivers who participated in a RACE
            participants = ps.sqldf(
                """ 
                    SELECT 
                        E.DRIVERID, D.RACEID
                        FROM drivers E
                    INNER JOIN 
                        results D ON E.DRIVERID=D.DRIVERID
                """
            )

            drivers_in_season_start = ps.sqldf(f"SELECT DRIVERID, RACEID FROM participants WHERE RACEID "
                                               f"IN(SELECT DISTINCT RACEID FROM races WHERE year = {year}  AND DATE IN ("
                                               f"SELECT MIN(DATE) FROM races WHERE year = {year}))")

            drivers_in_season_end = ps.sqldf(f"SELECT DRIVERID, RACEID FROM participants WHERE RACEID "
                                             f"IN(SELECT DISTINCT RACEID FROM races WHERE year = {year}  AND DATE IN ("
                                             f"SELECT MAX(DATE) FROM races WHERE year = {year}))")

            # This gives us rhe list of Drivers who participated in any given season on first and last race
            season_start_driver_details = ps.sqldf("SELECT DRIVERID, "
                                                   "FORENAME, SURNAME, DOB FROM drivers WHERE DRIVERID IN (SELECT "
                                                   "DRIVERID FROM "
                                                   "drivers_in_season_start) ORDER BY DOB")

            season_end_driver_details = ps.sqldf("SELECT DRIVERID, "
                                                 "FORENAME, SURNAME, DOB FROM drivers WHERE DRIVERID IN (SELECT "
                                                 "DRIVERID FROM "
                                                 "drivers_in_season_end) ORDER BY DOB")

            final = ps.sqldf(
                "SELECT DRIVERID, FORENAME, SURNAME, DOB, 'YOUNGEST_AT_SEASON_START' AS STATUS FROM season_start_driver_details WHERE DOB IN (SELECT MAX(DOB) FROM season_start_driver_details) UNION ALL "
                "SELECT DRIVERID, FORENAME, SURNAME, DOB, 'OLDEST_AT_SEASON_START' AS STATUS FROM season_start_driver_details WHERE DOB IN (SELECT MIN(DOB) FROM season_start_driver_details) UNION ALL "
                "SELECT DRIVERID, FORENAME, SURNAME, DOB, 'YOUNGEST_AT_SEASON_END' AS STATUS FROM season_end_driver_details WHERE DOB IN (SELECT MAX(DOB) FROM season_end_driver_details) UNION ALL "
                "SELECT DRIVERID, FORENAME, SURNAME, DOB, 'OLDEST_AT_SEASON_END' AS STATUS FROM season_end_driver_details WHERE DOB IN (SELECT MIN(DOB) FROM season_end_driver_details)")

            final['SEASON'] = year
            pd.DataFrame(final).to_csv(f"../dimensional_data/youngest_and_oldest_{year}.csv")
            print(f"\n>> Youngest and Oldest drivers for Season {year} processed saved in "
                  f"dimensional_data/youngest_and_oldest_{year}.csv <<")

    def winners_in_each_race(self):
        if 'drivers.csv' not in self.invalid_csv_list:
            results = pd.read_csv(f"../data/results.csv")
            drivers = pd.read_csv(f"../data/drivers.csv")
            try:
                pd.DataFrame(ps.sqldf(f"SELECT a.RACEID, a.DRIVERID, a.RANK,"
                                      f" b.FORENAME, B.SURNAME "
                                      f"FROM results a  JOIN drivers b  on "
                                      f"a.DRIVERID = b.DRIVERID WHERE "
                                      f"a.RANK = 1").to_csv("../dimensional_data/race_winners.csv"))
            except Exception as e:
                print(e)
