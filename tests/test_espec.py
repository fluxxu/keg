from keg import espec


def test_espec_grammar():
	specs = [
		"n",
		"z",
		"b:{1768=z,66443=n}",
		"b:{164=z,16K*565=z,1656=z,140164=z}",
		"b:{256K*=e:{237DA26C65073F42,06FC152E,z}}",
		"b:{22=n,54=z,192=n,24576=n,128=n,16384=n,*=z}",
		"b:{22=n,31943=z,211232=n,27037696=n,138656=n,17747968=n,*=z}",
		"b:{16K*=z:{6,mpq}}",
	]

	for spec in specs:
		espec.GRAMMAR.parse(spec)
