import csv
from collections import namedtuple
from typing import IO, Any, List


PSVRow = Any


class PSVFile:
	def __init__(self):
		self.raw_header = []
		self.header = []
		self.rows: List[PSVRow] = []
		self.row_format = None

	def __iter__(self):
		return self.rows.__iter__()

	def read_file(self, fp: IO) -> None:
		reader = csv.reader(
			filter(lambda row: not row.startswith("#"), fp),
			delimiter="|"
		)
		self.raw_header = next(reader)
		self.header = [f.split("!")[0] for f in self.raw_header]
		self.row_format = namedtuple("PSVRow", self.header)
		self.rows = [self.row_format(*row) for row in reader]


def load(fp) -> PSVFile:
	ret = PSVFile()
	ret.read_file(fp)
	return ret
