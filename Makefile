help:
	@echo "make release -- submit to pypi"

dist:
	rm -rf dist
	python setup.py sdist bdist_wheel
	rm -rf s3du.egg-info build
	ls -ldh dist/s3du-*

release: release-pypi

release-pypi: dist
	twine upload dist/s3du-*

.PHONY: dist
