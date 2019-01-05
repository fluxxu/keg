import os
from typing import Any, Dict, Iterable, List

import toml


DEFAULT_REMOTE_PREFIX = "http://us.patch.battle.net:1119/"


class KegConfig:
	def __init__(self, path: str) -> None:
		self.path = path
		self.config: Dict[str, Any] = {}

		if os.path.exists(self.path):
			self.load()

	@property
	def default_remote_prefix(self) -> str:
		return self.config["keg"].get("default-remote-prefix", DEFAULT_REMOTE_PREFIX)

	@property
	def preferred_cdns(self) -> List[str]:
		return self.config["keg"].get("preferred_cdns", [])

	@property
	def remotes(self) -> List[str]:
		return self.config.get("remotes", [])

	@property
	def fetchable_remotes(self) -> Iterable[str]:
		for remote in self.remotes:
			if self.config["remotes"][remote].get("default-fetch"):
				yield remote

	@property
	def verify(self) -> bool:
		return self.config["keg"].get("verify-integrity", True)

	def load(self):
		with open(self.path, "r") as f:
			self.config = toml.load(f)

		assert "keg" in self.config
		assert self.config["keg"].get("config_version") == 1

		assert "ngdp" in self.config
		assert self.config["ngdp"].get("hash_function") == "md5"

	def initialize(self) -> None:
		if not os.path.exists(self.path):
			self.config["keg"] = {
				"config_version": 1,
				"preferred_cdns": [],
				"default-remote-prefix": DEFAULT_REMOTE_PREFIX,
				"verify-integrity": True,
			}
			self.config["ngdp"] = {"hash_function": "md5"}
			self.save()

	def save(self) -> None:
		with open(self.path, "w") as f:
			toml.dump(self.config, f)

	def add_remote(self, remote: str, default_fetch: bool, writeable: bool) -> None:
		self.config["remotes"][remote] = {
			"default-fetch": default_fetch,
			"writeable": writeable,
		}
		self.save()

	def remove_remote(self, remote: str):
		del self.config["remotes"][remote]
		self.save()
