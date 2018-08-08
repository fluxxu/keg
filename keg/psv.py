import csv
from typing import Dict, List


PSVRow = Dict[str, str]
PSVFile = List[PSVRow]


def load(fp) -> PSVFile:
	values = csv.DictReader(
		filter(lambda row: not row.startswith("#"), fp),
		delimiter="|"
	)

	return [{
		k.split("!")[0]: v for k, v in row.items()
	} for row in values]
