
clean:
	rm -rf __pycache__ build dist pyverdict.egg-info *.log \
	pyverdict/verdict_jar .pytest_cache pyverdict/__pycache__ \
	tests/__pycache__ pyverdict/metadata.json

develop:
	python setup.py develop

undevelop:
	python setup.py develop --uninstall

package:
	python setup.py sdist bdist_wheel

deploy:
	twine upload dist/*
