deb: go-vnx-class
	CURDIR=${CURDIR} debian/gen-changelog.sh
	fakeroot debian/rules binary

debclean:
	go clean
	rm -rf debian/vnx-class debian/files debian/vnx-class.debhelper.log debian/vnx-class.substvars debian/changelog

go-vnx-class:
	go build

