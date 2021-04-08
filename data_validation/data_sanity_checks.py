from dataclasses import dataclass
import pandas as pd
from data_validation.data_validation_base_utils import Base
import grequests
import os


@dataclass
class DataSanitization(Base):
    valid_urls = []
    invalid_urls = []
    missing_dates = []
    file_list = os.listdir('../data/')
    invalid_csv_list = list()

    def start_sanitization(self):
        if not self.validate_all_csv_files():
            return self.sanity_checks

        for file in self.file_list:
            print(f'\n >>> PERFORMING SANITY CHECKS ON {file} <<<')
            df = pd.read_csv(f'../data/{file}')
            self.validate_urls(dataframe=df)
            self.validate_all_dates(dataframe=df)
            self.validate_if_any_year_missing(dataframe=df)
        for checks, results in self.sanity_checks.items():
            if isinstance(results, dict):
                self.sanity_passed = False

        return self.sanity_checks

    def validate_all_csv_files(self):
        print('- - Performing Sanity Checks - -')
        for file in self.file_list:
            try:
                df = pd.read_csv(f'../data/{file}')
                # check if any df is empty
                if df.empty:
                    self.invalid_csv_list.append(file)
                    self.sanity_checks['CSV_VALIDATION'] = {'STATUS': 'FAILURE',
                                                            'INVALID_CSV': self.invalid_csv_list}
                    return False
            except Exception as e:
                print(f"Error parsing file: {file} -> {e}")
                self.invalid_csv_list.append(file)
                self.sanity_checks['CSV_VALIDATION'] = {'STATUS': 'FAILURE',
                                                        'INVALID_CSV': self.invalid_csv_list}
                return False
        # return only those files who have valid data in them
        self.sanity_checks['CSV_VALIDATION'] = 'SUCCESS'
        return [x for x in self.file_list if x not in self.invalid_csv_list]

    def validate_urls(self, dataframe):
        current_url = None
        if 'url' in dataframe.columns:
            try:
                rs = (grequests.get(u) for u in dataframe['url'])
                requests = grequests.map(rs)
                for response, url in zip(requests, dataframe['url']):
                    current_url = url
                    # Validate if all URLS returns status code of 200
                    if response.status_code != 200:
                        self.invalid_urls.append(url)
                    else:
                        self.valid_urls.append(url)
            except Exception as e:
                self.invalid_urls.append(current_url)

        if len(self.invalid_urls) > 0:
            self.sanity_checks['URL_VALIDATION'] = {'STATUS': 'FAILED', 'INVALID_URL': self.invalid_urls}
        else:
            self.sanity_checks['URL_VALIDATION'] = 'SUCCESS'
            return True

    def validate_all_dates(self, dataframe):
        if 'date' in dataframe.columns:
            # Validate if dates match the specified format
            if pd.to_datetime(dataframe['date'], format='%Y-%m-%d', errors='coerce').notnull().all():
                self.sanity_checks['DATE_FORMAT_VALIDATION'] = 'SUCCESS'
            else:
                self.sanity_checks['DATE_FORMAT_VALIDATION'] = 'FAILED'

    def validate_if_any_year_missing(self, dataframe):
        if 'year' in dataframe.columns:
            # Get all unique years from the dataframes
            years = dataframe['year'].unique()
            # find all the years missing in the series
            missing_years = [y for y in range(min(years), max(years) + 1) if y not in years]
            # keep appending to avoid any issues in new files
            for year in missing_years:
                if year not in self.missing_dates:
                    self.missing_dates.append(year)
            if len(self.missing_dates) > 0:
                self.sanity_checks['RACE YEAR VALIDATION'] = {'Status': 'FAILED', 'MISSING YEARS': self.missing_dates}
            else:
                self.sanity_checks['RACE YEAR VALIDATION'] = 'Success'
