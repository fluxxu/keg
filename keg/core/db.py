import sqlite3
from typing import Iterable, List, Tuple

from .. import psv
from ..remote.http import StatefulResponse


TABLE_DEFINITIONS = [
	"""
	CREATE TABLE IF NOT EXISTS "responses" (
		remote text,
		path text,
		timestamp int64,
		digest text,
		source int
	)""",
	"""
	CREATE TABLE IF NOT EXISTS "blobs" (
		remote text,
		key text,
		row int,
		Region text,
		InstallBlobMD5 text,
		GameBlobMD5 text
	)""",
	"""
	CREATE TABLE IF NOT EXISTS "cdns" (
		remote text,
		key text,
		row int,
		Name text,
		Path text,
		Hosts text,
		Servers text,
		ConfigPath text
	)""",
]


for table_name in ("versions", "bgdl"):
	TABLE_DEFINITIONS.append(f"""
		CREATE TABLE IF NOT EXISTS "{table_name}" (
			remote text,
			key text,
			row int,
			BuildConfig text,
			BuildID int,
			CDNConfig text,
			KeyRing text,
			ProductConfig text,
			Region text,
			VersionsName text
		)
	""")


class AmbiguousVersionError(Exception):
	def __init__(self, msg: str, hints: Iterable[str]) -> None:
		super().__init__(msg)
		self._hints = hints

	def __str__(self):
		message = super().__str__()
		hints = "\n    ".join([""] + self._hints)
		return message + "\n\nThe candidates are:" + hints


class KegDB:
	def __init__(self, db_path: str) -> None:
		self.db_path = db_path
		self.db = sqlite3.connect(self.db_path)

	def create_tables(self) -> None:
		for statement in TABLE_DEFINITIONS:
			self.db.execute(statement)

	def cursor(self):
		return self.db.cursor()

	def commit(self):
		return self.db.commit()

	def get_build_configs(self, *, remote: str="") -> List[Tuple[str, str]]:
		"""
		Returns a list of all BuildConfigs and their corresponding CDNConfig.
		Specify `remote` to filter down to only those for that remote.
		"""
		cursor = self.cursor()
		if remote:
			cursor.execute("""
				SELECT distinct(BuildConfig), CDNConfig
				FROM versions
				WHERE remote = ?
				GROUP BY BuildConfig
			""", (remote, ))
		else:
			cursor.execute("""
				SELECT distinct(BuildConfig), CDNConfig
				FROM versions
				GROUP BY BuildConfig
			""")
		return cursor.fetchall()

	def get_cdn_configs(self, *, remotes: List[str]=None) -> List[str]:
		"""
		Returns a list of all BuildConfigs and their corresponding CDNConfig.
		Specify `remote` to filter down to only those for that remote.
		"""

		cursor = self.cursor()
		if remotes:
			_placeholder = ", ".join(["?"] * len(remotes))
			cursor.execute(f"""
				SELECT distinct(CDNConfig)
				FROM versions
				WHERE remote IN ({_placeholder})
				GROUP BY CDNConfig
				ORDER BY CDNConfig
			""", (*remotes, ))
		else:
			cursor.execute("""
				SELECT distinct(CDNConfig)
				FROM versions
				GROUP BY CDNConfig
				ORDER BY CDNConfig
			""")

		return [k for k, in cursor.fetchall()]

	def get_versions(self, *, remote: str) -> List[Tuple[str, int, str]]:
		"""
		Returns a list of all BuildConfigs and their corresponding
		BuildID and VersionsName, filtered by `remote`.
		"""
		cursor = self.cursor()
		cursor.execute("""
			SELECT
				distinct(BuildConfig), BuildID, VersionsName
			FROM versions
			WHERE
				remote = ?
			ORDER BY BuildID ASC
		""", (remote,))

		return cursor.fetchall()

	def get_responses(self, *, remote: str, path: str) -> List[Tuple[str, int]]:
		"""
		Returns a list of all response digests and their timestamp,
		for a specific `remote` and `path`.
		"""
		cursor = self.cursor()
		cursor.execute("""
			SELECT digest, timestamp
			FROM "responses"
			WHERE
				remote = ? AND
				path = ?
			ORDER BY timestamp
		""", (remote, path))

		return cursor.fetchall()

	def find_version(self, *, remote: str, version: str) -> Tuple[str, str]:
		"""
		Find the BuildConfig, CDNConfig pair for a `remote` and `version`.
		The `version` can be a VersionsName, BuildID or BuildConfig.

		Only BuildConfig is guaranteed to be unambiguous. If the `version`
		is ambiguous, an AmbiguousVersionError exception will be raised.
		"""
		cursor = self.db.cursor()
		cursor.execute("""
			SELECT distinct(BuildConfig), CDNConfig
			FROM versions
			WHERE
				REMOTE = ? AND
				VersionsName = ? OR BuildID = ? OR BuildConfig = ?
			GROUP BY BuildConfig
		""", (remote, version, version, version))
		results = cursor.fetchall()

		if not results:
			raise ValueError(f"Version not found: {version}")
		elif len(results) == 1:
			return results[0]
		else:
			raise AmbiguousVersionError(
				f"Version {repr(version)} is ambiguous",
				sorted(set(k for k, _ in results))
			)

	def write_response(
		self, response: StatefulResponse, remote: str, path: str, source: int
	) -> None:
		cursor = self.cursor()
		cursor.execute("""
			INSERT INTO "responses"
				(remote, path, timestamp, digest, source)
			VALUES
				(?, ?, ?, ?, ?)
		""", (remote, path, response.timestamp, response.digest, source))
		self.commit()

	def get_response_key(self, remote: str, path: str) -> str:
		cursor = self.cursor()
		cursor.execute("""
			SELECT digest
			FROM responses
			WHERE
				remote = ? AND
				path = ?
			ORDER BY timestamp DESC
			LIMIT 1
		""", (remote, path))
		ret = cursor.fetchone()
		if not ret:
			return ""
		return ret[0]

	def write_psv(self, psvfile: psv.PSVFile, key: str, remote: str, path: str) -> None:
		cursor = self.cursor()
		table_name = path.strip("/")
		cursor.execute(f"""
			DELETE FROM "{table_name}"
			WHERE
				remote = ? AND
				key = ?
		""", (remote, key))

		insert_tpl = """
			INSERT INTO "%s"
				(remote, key, row, %s)
			VALUES
				(?, ?, ?, %s)
			""" % (
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
			rows.append([remote, key, i, *cleaned_row])

		cursor.executemany(insert_tpl, rows)
		self.commit()
