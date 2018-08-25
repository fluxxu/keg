import sqlite3


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
