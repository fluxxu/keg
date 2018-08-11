#!/usr/bin/env python
import os
import sqlite3
from argparse import ArgumentParser
from typing import List, Set

import toml
from tqdm import tqdm


DEFAULT_REMOTE = "http://us.patch.battle.net:1119/hsb"


class App:
	def __init__(self, args):
		p = ArgumentParser()
		p.add_argument("--ngdp-dir", default=".ngdp")
		self.args = p.parse_args(args)
		self.ngdp_path = os.path.abspath(self.args.ngdp_dir)
		self.objects_path = os.path.join(self.ngdp_path, "objects")
		self.response_cache_dir = os.path.join(self.ngdp_path, "responses")
		self.db_path = os.path.join(self.ngdp_path, "keg.db")
		self.init_config()

		if os.path.exists(self.ngdp_path):
			self.db = sqlite3.connect(self.db_path)
		else:
			self.db = None

	@property
	def remotes(self) -> List[str]:
		return self.config["keg"].get("remotes", [])

	def _download(self, message: str, iterable: Set[str], callback):
		if not iterable:
			print(message, "Up-to-date.")
			return

		bar = tqdm(unit="files", ncols=0, leave=False, total=len(iterable))

		for key in iterable:
			bar.set_description(message + " " + key)
			callback(key)
			bar.update()
		bar.close()

		print(message, "Done.")

	def init_config(self):
		self.config_path = os.path.join(self.ngdp_path, "keg.conf")

		if os.path.exists(self.config_path):
			with open(self.config_path, "r") as f:
				self.config = toml.load(f)
			assert "keg" in self.config
			assert self.config["keg"].get("config_version") == 1

			assert "ngdp" in self.config
			assert self.config["ngdp"].get("hash_function") == "md5"
		else:
			self.config = {}

	def init_repo(self):
		if not os.path.exists(self.ngdp_path):
			os.makedirs(self.ngdp_path)
			print(f"Initialized in {self.ngdp_path}")
		else:
			print(f"Reinitialized in {self.ngdp_path}")

		if not os.path.exists(self.config_path):
			self.config["keg"] = {
				"config_version": 1,
			}
			self.config["ngdp"] = {
				"hash_function": "md5",
			}
			self.save_config()

		self.db = sqlite3.connect(self.db_path)
		self.db.execute("""
			CREATE TABLE IF NOT EXISTS responses (
				remote text,
				path text,
				timestamp int64,
				digest text
			)
		""")

		self.db.execute("""
			CREATE TABLE IF NOT EXISTS cdns (
				remote text,
				key text,
				row int,
				Name text,
				Path text,
				Hosts text,
				Servers text,
				ConfigPath text
			)
		""")

		self.db.execute("""
			CREATE TABLE IF NOT EXISTS versions (
				remote text,
				key text,
				row int,
				BuildConfig text,
				BuildID text,
				CDNConfig text,
				KeyRing text,
				ProductConfig text,
				Region text,
				VersionsName text
			)
		""")

	def save_config(self):
		with open(self.config_path, "w") as f:
			toml.dump(self.config, f)

	def add_remote(self, remote: str):
		if "remotes" not in self.config["keg"]:
			self.config["keg"]["remotes"] = []
		self.config["keg"]["remotes"].append(remote)
		self.save_config()

	def fetch(self, remote: str):
		from keg import Keg
		from keg.archive import ArchiveGroup
		from keg.encoding import EncodingFile
		from keg.cdn import CacheableCDNWrapper

		print(f"Fetching {remote}")

		keg = Keg(remote, cache_dir=self.response_cache_dir, cache_db=self.db)

		# keg fetch
		# 1. get the version
		# 2. get the cdns
		# 3. pick a cdn..? / keep the other ones as fallback
		# 4. pull the build config
		# 5. pull the cdn config
		# 6. pull the patch config
		# # 7. pull the product config (?)
		# 8. pull encoding file (which we got from build_config), if not resolved yet
		# 9. download all the archive indices
		# 10. do stuff for patch files
		# Finally... resolve all dangling data references

		# Look up all available CDNs
		cdns = keg.get_cdns()
		print("CDNs available:", ", ".join(cdn.name for cdn in cdns))

		# Pick a CDN
		assert cdns
		cdn = cdns[0]
		print(f"Using {cdn.name}")

		cdn_wrapper = CacheableCDNWrapper(cdn, base_dir=self.objects_path)

		# Find all available versions
		versions = keg.get_versions()
		print("Regions available:", ", ".join(version.region for version in versions))

		config_to_fetch = set()

		for version in versions:
			if not cdn_wrapper.has_config(version.build_config):
				config_to_fetch.add(version.build_config)
			if not cdn_wrapper.has_config(version.cdn_config):
				config_to_fetch.add(version.cdn_config)

		self._download("Fetching config...", config_to_fetch, cdn_wrapper.fetch_config)

		indices_to_fetch = set()
		for version in versions:
			cdn_config = cdn_wrapper.get_cdn_config(version.cdn_config)
			for archive_key in cdn_config.archives:
				if not cdn_wrapper.has_index(archive_key):
					indices_to_fetch.add(archive_key)

		self._download("Fetching indices...", indices_to_fetch, cdn_wrapper.fetch_index)

		archives_to_fetch = set()
		loose_files_to_fetch = set()
		patch_files_to_fetch = set()
		for version in versions:
			cdn_config = cdn_wrapper.get_cdn_config(version.cdn_config)
			# get the archive list

			for archive in cdn_config.archives:
				if not cdn_wrapper.has_data(archive):
					archives_to_fetch.add(archive)

			# Create the merged archive group
			archive_group = ArchiveGroup(
				cdn_config.archives,
				cdn_config.archive_group,
				cdn_wrapper
			)

			build_config = cdn_wrapper.get_build_config(version.build_config)
			content_key, encoding_key = build_config.encodings  # TODO verify content_key
			encoding_file = EncodingFile(
				cdn_wrapper.download_blte_data(encoding_key)
			)

			# Download loose files
			for encoding_key in encoding_file.keys:
				if not archive_group.has_file(encoding_key) and not cdn_wrapper.has_data(encoding_key):
					loose_files_to_fetch.add(encoding_key)

			# Download patch files
			patch_config = cdn_wrapper.get_patch_config(build_config.patch_config)
			for patch_entry in patch_config.patch_entries:
				for old_key, old_size, patch_key, patch_size in patch_entry.pairs:
					if not cdn_wrapper.has_patch(patch_key):
						patch_files_to_fetch.add(patch_key)

		self._download("Fetching archives...", archives_to_fetch, cdn_wrapper.download_data)
		self._download("Fetching loose files...", loose_files_to_fetch, cdn_wrapper.download_data)
		self._download("Fetching patch files...", patch_files_to_fetch, cdn_wrapper.fetch_patch)

		# whats left?
		# metadata:
		# - root
		# - install
		# - patch archive indices
		# - patch files
		#   - patch manifest
		#   - files referenced by patch

		print("Done.")

	def fetch_all(self):
		for remote in self.remotes:
			self.fetch(remote)

	def run(self):
		self.init_repo()  # keg init
		if DEFAULT_REMOTE not in self.remotes:
			self.add_remote(DEFAULT_REMOTE)  # keg remote add http://us.patch.battle.net:1119/hsb

		self.fetch_all()

		return 0


def main():
	import sys

	sys.path.append(os.path.abspath("."))

	app = App(sys.argv[1:])
	exit(app.run())


if __name__ == "__main__":
	main()
