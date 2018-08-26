from typing import Union

from . import blte
from .encoding import EncodingFile
from .installfile import InstallFile


class BuildManager:
	def __init__(self, build_config_key: str, cdn, verify: bool=False) -> None:
		self.build_config_key = build_config_key
		self.cdn = cdn
		self.verify = verify
		self.build_config = cdn.get_build_config(build_config_key, verify=verify)

	def get_encoding(self) -> Union[EncodingFile, None]:
		ckey, ekey = self.build_config.encodings
		if not ekey:
			return None

		with self.cdn.download_data(ekey, verify=self.verify) as fp:
			decoded_data = blte.load(fp, ekey, verify=self.verify)

		return EncodingFile(decoded_data, ckey, verify=self.verify)

	def get_install(self) -> Union[InstallFile, None]:
		install_ckey = self.build_config.install
		if not install_ckey:
			return None

		encoding_file = self.get_encoding()
		if not encoding_file:
			return None

		install_ekey = encoding_file.find_by_content_key(install_ckey)
		with self.cdn.download_data(install_ekey, verify=self.verify) as fp:
			return InstallFile.from_blte_file(
				fp, install_ckey, install_ekey, verify=self.verify
			)
