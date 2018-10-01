from typing import List

from .psv import PSVRow


class PSVResponse:
	def __init__(self, row: PSVRow) -> None:
		self._row = row

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self._row}>"


class Blobs(PSVResponse):
	def __init__(self, row: PSVRow) -> None:
		super().__init__(row)
		self.region = row.Region
		self.install_blob_md5 = row.InstallBlobMD5.lower()
		self.game_blob_md5 = row.GameBlobMD5.lower()


class CDNs(PSVResponse):
	def __init__(self, row: PSVRow) -> None:
		super().__init__(row)
		self.name = row.Name
		self.path = row.Path
		self.config_path = getattr(row, "ConfigPath", "")

	@property
	def all_servers(self) -> List[str]:
		return self.servers + [f"http://{host}" for host in self.hosts]

	@property
	def hosts(self) -> List[str]:
		return self._row.Hosts.split()

	@property
	def servers(self) -> List[str]:
		return getattr(self._row, "Servers", "").split()


class Versions(PSVResponse):
	def __init__(self, row: PSVRow) -> None:
		super().__init__(row)
		self.build_config = row.BuildConfig.lower()
		self.build_id = getattr(row, "BuildId", "")
		self.cdn_config = row.CDNConfig.lower()
		self.keyring = getattr(row, "KeyRing", "")
		self.product_config = getattr(row, "ProductConfig", "").lower()
		self.region = row.Region
		self.versions_name = getattr(row, "VersionsName", "")


class BGDL(Versions):
	pass
