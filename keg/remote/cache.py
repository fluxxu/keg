from enum import IntEnum
from typing import Any, List, Tuple

from ..psv import PSVFile
from ..psvresponse import CDNs, Versions
from .http import HttpRemote


class Source(IntEnum):
	INVALID = 0
	HTTP = 1
	RIBBIT = 2


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

	def get_psv(self, path: str):
		psvfile, response = super().get_psv(path)
		self.state_cache.write_response(response)
		self.cache_db.write_psv(psvfile, response.digest, self.remote, path)
		self.cache_db.write_response(response, self.remote, path, Source.HTTP)
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
