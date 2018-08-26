from enum import IntEnum
from typing import Any, List, Tuple

from . import psv
from .http import CDNs, HttpRemote, Versions


class Source(IntEnum):
	HTTP = 1


class CacheableHttpRemote(HttpRemote):
	def __init__(self, remote: str, cache_dir: str, cache_db, state_cache) -> None:
		super().__init__(remote)
		self.cache_dir = cache_dir
		self.cache_db = cache_db
		self.state_cache = state_cache

	def get_blob(self, name: str) -> Tuple[Any, Any]:
		ret, response = super().get_blob(name)
		self.state_cache.write_response(response)
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
		rows = []
		for i, row in enumerate(psvfile):
			# Always ensure lowercase entry of hexes
			cleaned_row = [
				cell.lower() if "!HEX:" in h.upper() else cell
				for cell, h in zip(row, psvfile.raw_header)
			]
			rows.append([self.remote, key, i, *cleaned_row])

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
		self.state_cache.write_response(response)

		cursor = self.cache_db.cursor()

		self.cache_psv(psvfile, response.digest, path, cursor)
		self.cache_response(response, path, cursor)

		self.cache_db.commit()

		return psvfile, response

	def get_cached_psv(self, remote: str, path: str) -> psv.PSVFile:
		cursor = self.cache_db.cursor()
		cursor.execute("""
			SELECT digest
			FROM responses
			WHERE
				remote = ? AND
				path = ?
			ORDER BY timestamp DESC
			LIMIT 1
		""", (remote, path))
		results = cursor.fetchone()
		if not results:
			# Fall back to querying live
			return self.get_psv(path)

		key = results[0]
		return self.state_cache.read_psv(path, key)

	def get_cached_cdns(self, remote: str) -> List[CDNs]:
		return [CDNs(row) for row in self.get_cached_psv(remote, "/cdns")]

	def get_cached_versions(self, remote: str) -> List[Versions]:
		return [Versions(row) for row in self.get_cached_psv(remote, "/versions")]
