from keg import espec

from . import get_resource


def test_espec_grammar():
	with get_resource("encodings.txt", "r") as f:
		data = f.read()

	specs = data.splitlines()

	for spec in specs:
		espec.GRAMMAR.parse(spec)
