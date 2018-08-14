import json
import os
from typing import IO
from urllib.parse import urljoin

import requests

from . import blizini, blte
from .archive import ArchiveIndex
from .configfile import BuildConfig, CDNConfig, PatchConfig
from .exceptions import NetworkError
from .utils import partition_hash, verify_data


DEFAULT_CONFIG_PATH = "tpr/configs/data"


class BaseCDN:
	def get_item(self, path: str):
		raise NotImplementedError()

	def get_config_item(self, path: str):
		raise NotImplementedError()

	def fetch_config(self, key: str, verify: bool=False) -> bytes:
		with self.get_item(f"/config/{partition_hash(key)}") as resp:
			data = resp.read()
		verify_data("config file", data, key, verify)
		return data

	def fetch_config_data(self, key: str, verify: bool=False) -> bytes:
		with self.get_config_item("/" + partition_hash(key)) as resp:
			data = resp.read()
		verify_data("config data", data, key, verify)
		return data

	def fetch_index(self, key: str, verify: bool=False) -> bytes:
		with self.get_item(f"/data/{partition_hash(key)}.index") as resp:
			return resp.read()

	def fetch_patch(self, key: str, verify: bool=False) -> bytes:
		with self.get_item(f"/patch/{partition_hash(key)}") as resp:
			data = resp.read()
		verify_data("patch file", data, key, verify)
		return data

	def load_config(self, key: str, verify: bool=False) -> dict:
		return blizini.load(
			self.fetch_config(key, verify=verify).decode()
		)

	def get_build_config(self, key: str, verify: bool=False) -> BuildConfig:
		return BuildConfig(self.load_config(key, verify))

	def get_cdn_config(self, key: str, verify: bool=False) -> CDNConfig:
		return CDNConfig(self.load_config(key, verify))

	def get_patch_config(self, key: str, verify: bool=False) -> PatchConfig:
		return PatchConfig(self.load_config(key, verify))

	def get_product_config(self, key: str, verify: bool=False) -> dict:
		return json.loads(self.fetch_config_data(key, verify))

	def get_index(self, key: str, verify: bool=False) -> ArchiveIndex:
		return ArchiveIndex(self.fetch_index(key), key, verify=verify)

	def download_blte_data(self, key: str, verify: bool=False) -> bytes:
		with self.get_item(f"/data/{partition_hash(key)}") as resp:
			data = blte.BLTEDecoder(resp, key, verify=verify)
			return b"".join(data.blocks)

	def download_data(self, key: str, verify: bool=False) -> IO:
		return self.get_item(f"/data/{partition_hash(key)}")


class RemoteCDN(BaseCDN):
	def __init__(self, server: str, path: str, config_path: str) -> None:
		self.server = server
		self.path = path
		self.config_path = config_path

	def _join_path(self, base_path: str, path: str):
		# Final path always has to end with a "/"
		# Actual path can't begin with a "/"
		# urljoin("/foo/bar", "baz") => "/foo/baz"
		# urljoin("/foo/bar/", "baz") => "/foo/bar/baz"
		# urljoin("/foo/bar//", "baz") => "/foo/bar/baz"
		# urljoin("/foo/bar/", "/baz") => "/baz"
		return urljoin(base_path + "/", path.lstrip("/"))

	def get_response(self, path: str) -> requests.Response:
		url = urljoin(self.server, path)
		ret = requests.get(url, stream=True)
		if ret.status_code != 200:
			raise NetworkError(f"Unexpected status code {ret.status_code} for {url}")
		return ret

	def get_item(self, path: str) -> IO:
		final_path = self._join_path(self.path, path)
		return self.get_response(final_path).raw

	def get_config_item(self, path: str) -> IO:
		final_path = self._join_path(self.config_path, path)
		return self.get_response(final_path).raw


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
	def __init__(
		self, base_dir: str, server: str, path: str, config_path: str=DEFAULT_CONFIG_PATH
	) -> None:
		if not os.path.exists(base_dir):
			os.makedirs(base_dir)
		self.local_cdn = LocalCDN(base_dir)
		self.remote_cdn = RemoteCDN(server, path, config_path)

	def get_item(self, path: str) -> IO:
		if not self.local_cdn.exists(path):
			cache_file_path = self.local_cdn.get_full_path(path)
			remote_path = self.remote_cdn._join_path(self.remote_cdn.path, path)
			response = self.remote_cdn.get_response(remote_path)
			f = HTTPCacheWrapper(response, cache_file_path)
			f.close()

		return self.local_cdn.get_item(path)

	def get_config_item(self, path: str) -> IO:
		if not self.local_cdn.config_exists(path):
			cache_file_path = self.local_cdn.get_config_path(path)
			remote_path = self.remote_cdn._join_path(self.remote_cdn.config_path, path)
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

	def has_config_item(self, key: str) -> bool:
		path = f"/{partition_hash(key)}"
		return self.local_cdn.config_exists(path)


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
