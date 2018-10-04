from typing import Any, List, Tuple

from ..core.db import KegDB
from ..core.statecache import StateCache
from ..psv import PSVFile
from ..psvresponse import CDNs, Versions
from .base import BaseRemote
from .http import HttpRemote
from .ribbit import RibbitRemote


class CacheableRemote(BaseRemote):
	def __init__(
		self, remote: str, cache_dir: str, cache_db: KegDB, state_cache: StateCache
	) -> None:
		super().__init__(remote)
		self.cache_dir = cache_dir
		self.cache_db = cache_db
		self.state_cache = state_cache


class CacheableHttpRemote(CacheableRemote, HttpRemote):
	def get_blob(self, name: str) -> Tuple[Any, Any]:
		ret, response = super().get_blob(name)
		self.state_cache.write_http_response(response)
		return ret, response

	def get_psv(self, name: str):
		psvfile, response = super().get_psv(name)
		self.state_cache.write_http_response(response)
		self.cache_db.write_psv(psvfile, response.digest, self.remote, name)
		self.cache_db.write_http_response(response, self.remote, response.path)
		return psvfile, response

	def get_cached_psv(self, name: str) -> PSVFile:
		key = self.cache_db.get_response_key(self.remote, name)
		if not key:
			# Fall back to querying live
			return self.get_psv(name)
		return self.state_cache.read_psv(name, key)

	def get_cached_cdns(self) -> List[CDNs]:
		return [CDNs(row) for row in self.get_cached_psv("cdns")]

	def get_cached_versions(self) -> List[Versions]:
		return [Versions(row) for row in self.get_cached_psv("versions")]


class CacheableRibbitRemote(CacheableRemote, RibbitRemote):
	def get_psv(self, name: str):
		psvfile, response = super().get_psv(name)
		self.state_cache.write_ribbit_response(response)
		self.cache_db.write_psv(psvfile, response.checksum, self.remote, name)
		self.cache_db.write_ribbit_response(response, self.remote, response.request.path)
		return psvfile, response
