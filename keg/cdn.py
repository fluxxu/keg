import hashlib
import json
import os
from typing import IO

import requests

from . import blizini, blte
from .archive import ArchiveIndex
from .configfile import BuildConfig, CDNConfig, PatchConfig
from .utils import partition_hash


class BaseCDN:
	def get_item(self, path: str):
		raise NotImplementedError()

	def get_config_item(self, path: str):
		raise NotImplementedError()

	def fetch_config(self, key: str, verify: bool=True) -> bytes:
		with self.get_item(f"/config/{partition_hash(key)}") as resp:
			data = resp.read()
		if verify:
			assert hashlib.md5(data).hexdigest() == key
		return data

	def fetch_config_data(self, key: str, verify: bool=False) -> bytes:
		with self.get_config_item("/" + partition_hash(key)) as resp:
			data = resp.read()
		if verify:
			assert hashlib.md5(data).hexdigest() == key
		return data

	def fetch_index(self, key: str) -> bytes:
		with self.get_item(f"/data/{partition_hash(key)}.index") as resp:
			return resp.read()

	def fetch_patch(self, key: str, verify: bool=False) -> bytes:
		with self.get_item(f"/patch/{partition_hash(key)}") as resp:
			data = resp.read()
			if verify:
				assert hashlib.md5(data).hexdigest() == key
			return data

	def load_config(self, key: str) -> dict:
		return blizini.load(self.fetch_config(key).decode())

	def get_build_config(self, key: str) -> BuildConfig:
		return BuildConfig(self.load_config(key))

	def get_cdn_config(self, key: str) -> CDNConfig:
		return CDNConfig(self.load_config(key))

	def get_patch_config(self, key: str) -> PatchConfig:
		return PatchConfig(self.load_config(key))

	def get_product_config(self, key: str) -> dict:
		return json.loads(self.fetch_config_data(key))

	def get_index(self, key: str, verify: bool=False) -> ArchiveIndex:
		return ArchiveIndex(self.fetch_index(key), key, verify=verify)

	def download_blte_data(self, key: str, verify: bool=False) -> bytes:
		with self.get_item(f"/data/{partition_hash(key)}") as resp:
			data = blte.BLTEDecoder(resp, key, verify=verify)
			return b"".join(data.blocks)

	def download_data(self, key: str) -> IO:
		return self.get_item(f"/data/{partition_hash(key)}")


class RemoteCDN(BaseCDN):
	def __init__(self, cdn):
		assert cdn.all_servers
		self.server = cdn.all_servers[0]
		self.path = cdn.path
		self.config_path = cdn.config_path

	def get_response(self, path: str) -> requests.Response:
		url = f"{self.server}/{path}"
		return requests.get(url, stream=True)

	def get_item(self, path: str) -> IO:
		return self.get_response(self.path + path).raw

	def get_config_item(self, path: str) -> IO:
		return self.get_response(self.config_path + path).raw


class LocalCDN(BaseCDN):
	def __init__(self, base_dir: str) -> None:
		self.base_dir = base_dir

	def get_full_path(self, path: str) -> str:
		return os.path.join(self.base_dir, path.lstrip("/"))

	def get_config_path(self, path: str) -> str:
		return os.path.join(
			self.base_dir, "configs", "data", path.lstrip("/")
		)

	def get_item(self, path: str) -> IO:
		return open(self.get_full_path(path), "rb")

	def get_config_item(self, path: str) -> IO:
		return open(self.get_config_path(path), "rb")

	def exists(self, path: str) -> bool:
		return os.path.exists(self.get_full_path(path))

	def config_exists(self, path: str) -> bool:
		return os.path.exists(self.get_config_path(path))


class CacheableCDNWrapper(BaseCDN):
	def __init__(self, cdns_response, base_dir: str) -> None:
		if not os.path.exists(base_dir):
			os.makedirs(base_dir)
		self.local_cdn = LocalCDN(base_dir)
		self.remote_cdn = RemoteCDN(cdns_response)

	def get_item(self, path: str) -> IO:
		if not self.local_cdn.exists(path):
			cache_file_path = self.local_cdn.get_full_path(path)
			remote_path = self.remote_cdn.path + path
			response = self.remote_cdn.get_response(remote_path)
			f = HTTPCacheWrapper(response, cache_file_path)
			f.close()

		return self.local_cdn.get_item(path)

	def get_config_item(self, path: str) -> IO:
		if not self.local_cdn.config_exists(path):
			cache_file_path = self.local_cdn.get_config_path(path)
			remote_path = self.remote_cdn.config_path + path
			response = self.remote_cdn.get_response(remote_path)
			f = HTTPCacheWrapper(response, cache_file_path)
			f.close()

		return self.local_cdn.get_config_item(path)

	def has_config(self, key: str) -> bool:
		path = f"/config/{partition_hash(key)}"
		return self.local_cdn.exists(path)

	def has_data(self, key: str) -> bool:
		path = f"/data/{partition_hash(key)}"
		return self.local_cdn.exists(path)

	def has_index(self, key: str) -> bool:
		path = f"/data/{partition_hash(key)}.index"
		return self.local_cdn.exists(path)

	def has_patch(self, key: str) -> bool:
		path = f"/patch/{partition_hash(key)}"
		return self.local_cdn.exists(path)


class HTTPCacheWrapper:
	def __init__(self, response: requests.Response, path: str) -> None:
		self._response = response.raw

		dir_path = os.path.dirname(path)
		if not os.path.exists(dir_path):
			os.makedirs(dir_path)

		self._real_path = path
		self._temp_path = path + ".keg_temp"
		self._cache_file = open(self._temp_path, "wb")

	def __enter__(self):
		return self

	def __exit__(self, *exc):
		self.close()
		return False

	def close(self):
		self.read()
		self._cache_file.close()

		# Atomic write&move; make sure there's no partially-written caches.
		os.rename(self._temp_path, self._real_path)

		return self._response.close()

	def read(self, size: int=-1) -> bytes:
		if size == -1:
			ret = self._response.read()
		else:
			ret = self._response.read(size)
		if ret:
			self._cache_file.write(ret)
		return ret
