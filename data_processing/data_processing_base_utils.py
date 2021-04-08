from dataclasses import dataclass
from data_validation.data_sanity_checks import DataSanitization


@dataclass
class Base(DataSanitization):
    def __init__(self):
        self.sanity_report = self.start_sanitization()
