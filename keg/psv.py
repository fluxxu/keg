import csv


def load(fp) -> dict:
	values = csv.DictReader(
		filter(lambda row: not row.startswith("#"), fp),
		delimiter="|"
	)

	return [{
		k.split("!")[0]: v for k, v in row.items()
	} for row in values]
