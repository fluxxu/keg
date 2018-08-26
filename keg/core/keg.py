import os

from .. import CacheableHttpRemote
from ..cdn import LocalCDN
from .config import KegConfig
from .db import KegDB
from .statecache import StateCache


class Keg:
	def __init__(self, path: str) -> None:
		self.path = os.path.abspath(path)
		self.objects_path = os.path.join(self.path, "objects")
		self.fragments_path = os.path.join(self.path, "fragments")
		self.response_cache_dir = os.path.join(self.path, "responses")
		self.config_path = os.path.join(self.path, "keg.conf")
		self.db_path = os.path.join(self.path, "keg.db")
		self.state_cache = StateCache(self.response_cache_dir)

		self.initialized = os.path.exists(self.path)

		if self.initialized:
			self.db = KegDB(self.db_path)
		else:
			self.db = KegDB(":memory:")

		self.config = KegConfig(self.config_path)
		self.local_cdn = LocalCDN(self.objects_path, self.fragments_path)

	def initialize(self) -> bool:
		if not os.path.exists(self.path):
			reinitialized = True
			os.makedirs(self.path)
		else:
			reinitialized = False

		self.config.initialize()

		self.db = KegDB(self.db_path)
		self.db.create_tables()

		return reinitialized

	def get_remote(self, remote: str) -> CacheableHttpRemote:
		return CacheableHttpRemote(
			remote,
			cache_dir=self.response_cache_dir,
			cache_db=self.db,
			state_cache=self.state_cache
		)

	def clean_remote(self, remote: str) -> str:
		"""
		Cleans a remote by adding the configured default remote prefix
		if it's missing a scheme.
		"""
		if "://" not in remote:
			remote = self.config.default_remote_prefix + remote
		return remote
