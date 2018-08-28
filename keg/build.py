from typing import Union

from . import blte
from .archive import ArchiveGroup
from .cdn import BaseCDN
from .encoding import EncodingFile
from .installfile import InstallFile


class BuildManager:
	def __init__(
		self, build_config: str, cdn_config: str, cdn: BaseCDN, verify: bool=False
	) -> None:
		self.build_config_key = build_config
		self.cdn_config_key = cdn_config
		self.cdn = cdn
		self.verify = verify

		self.build_config = cdn.get_build_config(build_config, verify=verify)
		self.cdn_config = cdn.get_cdn_config(cdn_config, verify=verify)

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self.build_config_key} {self.cdn_config_key}>"

	def get_encoding(self) -> Union[EncodingFile, None]:
		encoding = self.build_config.encoding
		if not encoding.content_key:
			return None

		with self.cdn.download_data(encoding.encoding_key, verify=self.verify) as fp:
			decoded_data = blte.load(fp, encoding.encoding_key, verify=self.verify)

		return EncodingFile(decoded_data, encoding.content_key, verify=self.verify)

	def get_install(self) -> Union[InstallFile, None]:
		install = self.build_config.install

		if install.encoding_key:
			encoding_key = install.encoding_key
		elif install.content_key:
			encoding_key = self.find_encoding_key(install.content_key)
		else:
			encoding_key = ""

		if not encoding_key or not install.content_key:
			return None

		with self.cdn.download_data(encoding_key, verify=self.verify) as fp:
			return InstallFile.from_blte_file(
				fp, install.content_key, encoding_key, verify=self.verify
			)

	def find_encoding_key(self, content_key: str) -> str:
		encoding_file = self.get_encoding()
		if not encoding_file:
			return ""

		return encoding_file.find_by_content_key(content_key)

	def get_archive_group(self) -> ArchiveGroup:
		return ArchiveGroup(
			self.cdn_config.archives,
			self.cdn_config.archive_group,
			self.cdn,
			verify=self.verify
		)

	def get_root(self) -> bytes:
		encoding_file = self.get_encoding()
		assert encoding_file
		root_ekey = encoding_file.find_by_content_key(self.build_config.root)
		archive_group = self.get_archive_group()

		return archive_group.get_file_by_key(root_ekey)
