from typing import List

from .. import psvresponse


class BaseRemote:
	def __init__(self, remote: str) -> None:
		self.remote = remote

	def get_psv(self, name: str):
		raise NotImplementedError("This method must be overridden in a subclass")

	def get_blobs(self) -> List[psvresponse.Blobs]:
		psvfile, _ = self.get_psv("blobs")
		return [psvresponse.Blobs(row) for row in psvfile]

	def get_bgdl(self) -> List[psvresponse.BGDL]:
		psvfile, _ = self.get_psv("bgdl")
		return [psvresponse.BGDL(row) for row in psvfile]

	def get_cdns(self) -> List[psvresponse.CDNs]:
		psvfile, _ = self.get_psv("cdns")
		return [psvresponse.CDNs(row) for row in psvfile]

	def get_versions(self) -> List[psvresponse.Versions]:
		psvfile, _ = self.get_psv("versions")
		return [psvresponse.Versions(row) for row in psvfile]
