import os
from datetime import datetime
from hashlib import md5

from .http import HttpBackend
from .utils import partition_hash


class Keg(HttpBackend):
	def __init__(self, remote: str, cache_dir: str, cache_db) -> None:
		super().__init__(remote)
		self.cache_dir = cache_dir
		self.cache_db = cache_db

	def get_psv(self, path: str):
		psvfile, data = super().get_psv(path)
		timestamp = int(datetime.now().timestamp())
		digest = md5(data).hexdigest()

		cache_path = os.path.join(
			self.cache_dir,
			path.lstrip("/"),
			partition_hash(digest)
		)

		if not os.path.exists(cache_path):
			cache_dir = os.path.dirname(cache_path)
			if not os.path.exists(cache_dir):
				os.makedirs(cache_dir)

			temp_name = cache_path + ".keg_temp"
			with open(temp_name, "wb") as f:
				f.write(data)
			os.rename(temp_name, cache_path)

		table_name = path.strip("/")
		cursor = self.cache_db.cursor()
		cursor.execute("""
			DELETE FROM "%s" where key = ?
		""" % (table_name), (digest, ))

		insert_tpl = 'INSERT INTO "%s" (remote, key, row, %s) values (?, ?, ?, %s)' % (
			table_name,
			", ".join(psvfile.header),
			", ".join(["?"] * (len(psvfile.header)))
		)
		cursor.executemany(insert_tpl, [
			[self.remote, digest, i, *row] for i, row in enumerate(psvfile)
		])

		cursor.execute("""
			INSERT INTO "responses"
				(remote, path, timestamp, digest)
			VALUES
				(?, ?, ?, ?)
		""", (self.remote, path, timestamp, digest))

		self.cache_db.commit()

		return psvfile, data
