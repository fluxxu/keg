from keg.core.db import KegDB


def test_db_tables():
	db = KegDB(":memory:")
	db.create_tables()

	cursor = db.cursor()
	cursor.execute("SELECT count(*) FROM sqlite_master WHERE type = 'table'")
	assert cursor.fetchone() == (5,)
