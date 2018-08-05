import csv
from typing import Dict, List


def load(fp) -> List[Dict[str, str]]:
	values = csv.DictReader(
		filter(lambda row: not row.startswith("#"), fp),
		delimiter="|"
	)

	return [{
		k.split("!")[0]: v for k, v in row.items()
	} for row in values]
