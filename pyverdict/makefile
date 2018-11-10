
clean:
	rm -rf __pycache__ build dist pyverdict.egg-info *.log pyverdict/verdict_jar

package:
	python setup.py sdist bdist_wheel

deploy:
	twine upload dist/*
