#!/usr/bin/env python
import os
import sys
from hashlib import md5

from keg.build import BuildManager
from keg.core.keg import Keg


class MissingEntries(Exception):
	pass


def main():
	app = Keg(".ngdp")
	if not app.initialized:
		print(f"FATAL: Not a NGDP repository: {os.path.abspath('.')}")
		exit(2)

	cdn = app.local_cdn
	remote = app.clean_remote("hsb")

	target_version = sys.argv[1]
	target_build_config, target_cdn_config = app.db.find_version(
		remote=remote, version=target_version
	)
	target_build = BuildManager(target_build_config, target_cdn_config, cdn)
	install_file = target_build.get_install()
	entries = set(
		entry for entry in install_file.entries if entry[0].endswith(".unity3d")
	)

	misses = 0
	for build_config_key, cdn_config_key in app.db.get_build_configs(remote=remote):
		try:
			build = BuildManager(build_config_key, cdn_config_key, cdn)
		except FileNotFoundError:
			print(f"{build_config_key}: Missing config files", file=sys.stderr)
			# Skip the build
			continue

		try:
			root_data = build.get_root().decode()
		except FileNotFoundError:
			print(f"{build_config_key}: Missing root file", file=sys.stderr)
			continue
		except KeyError:
			print(f"{build_config_key}: Problem finding root", file=sys.stderr)

		try:
			generated = generate_for_root(root_data, entries)
		except MissingEntries as e:
			misses += 1
			print(f"{build_config_key}: {e}", file=sys.stderr)
			continue
		else:
			digest = md5(generated).hexdigest()
			print(generated.decode(), end="")
			print(f"Digest: {digest}", file=sys.stderr)
			exit(0)
	else:
		print("FATAL: Could not find suitable base to generate root", file=sys.stderr)
		exit(1)


def generate_for_root(root_data, entries):
	lines = root_data.splitlines()
	sorted_filenames = [k[0] for k in [line.split("|") for line in lines]]

	ret = []

	for fn in sorted_filenames:
		for filename, key, size in entries:
			if filename == fn:
				ret.append((filename, str(size), key))
				break

	missing_entries = len(entries) - len(ret)
	if missing_entries:
		raise MissingEntries(f"{missing_entries} missing entries")

	return "\n".join("|".join(row) for row in ret).encode() + b"\n"


if __name__ == "__main__":
	main()
