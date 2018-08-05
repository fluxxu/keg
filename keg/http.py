from io import StringIO
from typing import List

import requests

from . import blizini, blte, psv
from .archive import ArchiveIndex
from .configfile import BuildConfig, CDNConfig, PatchConfig


def partition_hash(hash: str) -> str:
	return f"{hash[0:2]}/{hash[2:4]}/{hash}"


class CDNs:
	def __init__(self, values: dict) -> None:
		self._values = values
		self.name = values.get("Name", "")
		self.path = values.get("Path", "")
		self.config_path = values.get("ConfigPath", "")

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self._values}>"

	@property
	def all_servers(self) -> list:
		return self.servers + [f"http://{host}" for host in self.hosts]

	@property
	def hosts(self) -> list:
		return self._values.get("Hosts", "").split()

	@property
	def servers(self) -> list:
		return self._values.get("Servers", "").split()

	def download_build_config(self, hash: str) -> BuildConfig:
		return BuildConfig(self.download_config(hash))

	def download_cdn_config(self, hash: str) -> CDNConfig:
		return CDNConfig(self.download_config(hash))

	def download_patch_config(self, hash: str) -> PatchConfig:
		return PatchConfig(self.download_config(hash))

	def get_url(self, path, stream=False):
		assert self.all_servers
		server = self.all_servers[0]
		url = f"{server}/{self.path}{path}"
		return requests.get(url, stream=stream)

	def download_config(self, hash: str) -> dict:
		config = self.get_url(f"/config/{partition_hash(hash)}")
		return blizini.load(config.content.decode())

	def download_data_index(self, hash: str, verify: bool=False) -> ArchiveIndex:
		resp = self.get_url(f"/data/{partition_hash(hash)}.index", stream=True)
		return ArchiveIndex(resp.raw, hash, verify=verify)

	def download_data(self, hash: str, verify: bool=False) -> bytes:
		resp = self.get_url(f"/data/{partition_hash(hash)}", stream=True)
		data = blte.BLTEDecoder(resp.raw, hash, verify=verify)

		return b"".join(data.blocks)


class Versions:
	def __init__(self, values: dict) -> None:
		self._values = values
		self.build_config = values.get("BuildConfig")
		self.build_id = values.get("BuildId")
		self.cdn_config = values.get("CDNConfig")
		self.keyring = values.get("KeyRing")
		self.product_config = values.get("ProductConfig")
		self.region = values.get("Region")
		self.versions_name = values.get("VersionsName")

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self._values}>"


class HttpBackend:
	def __init__(self, remote: str) -> None:
		self.remote = remote

	def request_path(self, path: str):
		url = self.remote + path
		print(url)
		return requests.get(url)

	def get_cdns(self) -> List[CDNs]:
		return [CDNs(row) for row in self.get_psv("/cdns")]

	def get_versions(self) -> List[Versions]:
		return [Versions(row) for row in self.get_psv("/versions")]

	def get_psv(self, path: str) -> dict:
		resp = self.request_path(path)
		return psv.load(
			StringIO(resp.content.decode())
		)
