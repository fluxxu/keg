from keg import psv
from keg.psvresponse import CDNs, Versions

from . import get_resource


def test_read_old_cdns():
	with get_resource(f"cdns/a716783d0bfb5b6ee84ac3f7c7e42b1f") as f:
		psvfile = psv.load(f)

	for cdn in [CDNs(row) for row in psvfile]:
		assert cdn.name
		assert cdn.path
		assert not cdn.config_path


def test_read_old_versions():
	with get_resource(f"versions/7a53c9036832987d60ef2336a8a714ce") as f:
		psvfile = psv.load(f)

	for version in [Versions(row) for row in psvfile]:
		assert version.region
		assert version.build_config
		assert version.cdn_config
		assert not version.build_id
		assert not version.keyring
		assert not version.product_config
		assert not version.versions_name
