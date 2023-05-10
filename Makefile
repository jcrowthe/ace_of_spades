.PHONY: build clean
default: format

format:
	python -m black . --line-length 120