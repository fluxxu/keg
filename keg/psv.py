import csv
import re
from collections import namedtuple
from io import StringIO
from typing import IO, Any, List


PSVRow = Any
SEQN_RE = re.compile(r"## seqn = (\d+)")


def parse_seqn(line: str) -> int:
	sre = SEQN_RE.match(line)
	if not sre:
		raise ValueError(f"Invalid seqn line: {line!r}")
	seqn, = sre.groups()
	return int(seqn)


class PSVFile:
	def __init__(self):
		self.raw_header = []
		self.header = []
		self.rows: List[PSVRow] = []
		self.row_format = None
		self.seqn = 0

	def __iter__(self):
		return self.rows.__iter__()

	def read_file(self, fp: IO) -> None:
		def filter_row(row):
			if row.startswith("#"):
				# "#" is ignored (comment)
				# But if it's ## seqn = 12345, we want to parse it
				if row.startswith("## seqn = "):
					if self.seqn:
						raise ValueError(f"Duplicate seqn in psv: {row!r}")
					self.seqn = parse_seqn(row)
				return False
			return True

		reader = csv.reader(filter(filter_row, fp), delimiter="|")
		self.raw_header = next(reader)
		self.header = [f.split("!")[0] for f in self.raw_header]
		self.row_format = namedtuple("PSVRow", self.header)
		self.rows = [self.row_format(*row) for row in reader]


def load(fp: IO) -> PSVFile:
	ret = PSVFile()
	ret.read_file(fp)
	return ret


def loads(data: str) -> PSVFile:
	return load(StringIO(data))
