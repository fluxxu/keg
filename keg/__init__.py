from enum import IntEnum
from typing import Any, List, Tuple

from . import psv
from .http import CDNs, HttpBackend, StateCache


class Source(IntEnum):
	HTTP = 1


class Keg(HttpBackend):
	def __init__(self, remote: str, cache_dir: str, cache_db) -> None:
		super().__init__(remote)
		self.cache_dir = cache_dir
		self.cache_db = cache_db

	def get_blob(self, name: str) -> Tuple[Any, Any]:
		ret, response = super().get_blob(name)
		response.write_to_cache(self.cache_dir)
		return ret, response

	def cache_psv(self, psvfile, key: str, path: str, cursor) -> None:
		table_name = path.strip("/")
		cursor.execute("""
			DELETE FROM "%s" where remote = ? and key = ?
		""" % (table_name), (self.remote, key, ))

		insert_tpl = 'INSERT INTO "%s" (remote, key, row, %s) values (?, ?, ?, %s)' % (
			table_name,
			", ".join(psvfile.header),
			", ".join(["?"] * len(psvfile.header))
		)
		rows = [[self.remote, key, i, *row] for i, row in enumerate(psvfile)]
		cursor.executemany(insert_tpl, rows)

	def cache_response(self, response, path: str, cursor) -> None:
		cursor.execute("""
			INSERT INTO "responses"
				(remote, path, timestamp, digest, source)
			VALUES
				(?, ?, ?, ?, ?)
		""", (self.remote, path, response.timestamp, response.digest, Source.HTTP))

	def get_psv(self, path: str):
		psvfile, response = super().get_psv(path)
		response.write_to_cache(self.cache_dir)

		cursor = self.cache_db.cursor()

		self.cache_psv(psvfile, response.digest, path, cursor)
		self.cache_response(response, path, cursor)

		self.cache_db.commit()

		return psvfile, response

	def get_cached_psv(self, path: str, key: str, cache_dir: str) -> psv.PSVFile:
		data = StateCache(cache_dir).read(path, key)
		return psv.loads(data)

	def get_cached_cdns(self, remote: str, cache_dir: str) -> List[CDNs]:
		cursor = self.cache_db.cursor()
		cursor.execute("""
			SELECT digest
			FROM responses
			WHERE
				remote = ? AND
				path = ?
			ORDER BY timestamp DESC
			LIMIT 1
		""", (remote, "/cdns"))
		results = cursor.fetchone()
		if not results:
			# Fall back to querying live
			return self.get_cdns()

		psvfile = self.get_cached_psv("/cdns", results[0], cache_dir)
		return [CDNs(row) for row in psvfile]
