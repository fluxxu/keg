from typing import Any, Tuple

from .http import HttpBackend


class Keg(HttpBackend):
	def __init__(self, remote: str, cache_dir: str, cache_db) -> None:
		super().__init__(remote)
		self.cache_dir = cache_dir
		self.cache_db = cache_db

	def get_blob(self, name: str) -> Tuple[Any, Any]:
		ret, response = super().get_blob(name)
		response.write_to_cache(self.cache_dir)
		return ret, response

	def get_psv(self, path: str):
		psvfile, response = super().get_psv(path)
		response.write_to_cache(self.cache_dir)

		table_name = path.strip("/")
		cursor = self.cache_db.cursor()
		cursor.execute("""
			DELETE FROM "%s" where remote = ? and key = ?
		""" % (table_name), (self.remote, response.digest, ))

		insert_tpl = 'INSERT INTO "%s" (remote, key, row, %s) values (?, ?, ?, %s)' % (
			table_name,
			", ".join(psvfile.header),
			", ".join(["?"] * (len(psvfile.header)))
		)
		cursor.executemany(insert_tpl, [
			[self.remote, response.digest, i, *row] for i, row in enumerate(psvfile)
		])

		cursor.execute("""
			INSERT INTO "responses"
				(remote, path, timestamp, digest)
			VALUES
				(?, ?, ?, ?)
		""", (self.remote, path, response.timestamp, response.digest))

		self.cache_db.commit()

		return psvfile, response
