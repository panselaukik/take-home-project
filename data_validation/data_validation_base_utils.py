from dataclasses import dataclass
import logging
import sys
import warnings


@dataclass
class Base:
    sanity_checks = {}
    logger = logging.getLogger(__name__)
    sanity_passed = True
    if not sys.warnoptions:
        warnings.simplefilter("ignore")
