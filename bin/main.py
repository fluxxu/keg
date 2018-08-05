#!/usr/bin/env python
import os
from argparse import ArgumentParser

import toml


DEFAULT_REMOTE = "http://us.patch.battle.net:1119/hsb"


class App:
	def __init__(self, args):
		p = ArgumentParser()
		p.add_argument("--ngdp-dir", default=".ngdp")
		self.args = p.parse_args(args)
		self.ngdp_path = os.path.abspath(self.args.ngdp_dir)
		self.init_config()

	def init_config(self):
		self.config_path = os.path.join(self.ngdp_path, "keg.conf")

		if os.path.exists(self.config_path):
			with open(self.config_path, "r") as f:
				self.config = toml.load(f)
			assert "keg" in self.config
			assert self.config["keg"].get("config_version") == 1
		else:
			self.config = {}

	def init_repo(self):
		if not os.path.exists(self.ngdp_path):
			os.makedirs(self.ngdp_path)
			print(f"Initialized in {self.ngdp_path}")
		else:
			print(f"Reinitialized in {self.ngdp_path}")

		if not os.path.exists(self.config_path):
			self.config["keg"] = {"config_version": 1}
			self.save_config()

	def save_config(self):
		with open(self.config_path, "w") as f:
			toml.dump(self.config, f)

	def run(self):
		from keg import Keg
		from keg.encoding import EncodingFile

		self.init_repo()

		keg = Keg(DEFAULT_REMOTE)
		versions = keg.get_versions()
		cdns = keg.get_cdns()

		# pick a cdn
		assert cdns
		cdn = cdns[0]

		for version in versions:
			# BuildConfig: http://blzddist1-a.akamaihd.net/tpr/hs/config/6a/5f/6a5f9d058ac7c519d929571a64e4ef3d
			# CDNConfig: http://blzddist1-a.akamaihd.net/tpr/hs/config/17/8c/178ca9764fb469eccbbaeaf55b280336

			build_config = cdn.download_build_config(version.build_config)
			cdn_config = cdn.download_cdn_config(version.cdn_config)
			break

		content_key, encoding_key = build_config.encodings

		encoding_data = cdn.download_data(encoding_key)
		encoding_file = EncodingFile(encoding_data)

		# get the archive list

		# example key
		encoding_key = encoding_file.keys[0]

		for archive_key in cdn_config.archives:
			archive = ArchiveFile(archive_key)

		key = archive_group.resolve_encoding_key(encoding_key)
		if key:
			# key resolved in the archive file
			cdn.download_data(key)
		else:
			# loose file
			cdn.download_data(encoding_key)

		# whats left?
		# metadata:
		# - root
		# - install
		# - archive indices
		# - patch archive indices
		# - patch files
		#   - patch manifest
		#   - files referenced by patch
		# files:
		# - archives
		# - loose files

		return 0


def main():
	import sys

	sys.path.append(os.path.abspath("."))

	app = App(sys.argv[1:])
	exit(app.run())


if __name__ == "__main__":
	main()
