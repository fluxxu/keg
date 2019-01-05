from keg.installfile import InstallFile

from . import get_resource


def test_install_file():
	with get_resource("install_decoded", "rb") as fp:
		install_file = InstallFile(fp.read(), "b0c59af62001174f3d0857d07e8784c2", verify=True)

	assert len(install_file.tags) == 32
	assert "Amazon" in install_file.tags

	assert len(install_file.entries) == 2416
	assert install_file.entries[0] == (
		"msvcp140.dll",
		"b9abe16b723ddd90fc612d0ddb0f7ab4",
		633144,
	)

	assert len(list(install_file.filter_entries(["Windows", "enUS", "Production"]))) == 183
	assert len(list(install_file.filter_entries(["Windows", "OSX"]))) == 0
