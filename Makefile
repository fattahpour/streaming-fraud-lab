.RECIPEPREFIX := >
.PHONY: build test run stop tail

build:
> ./scripts/build.sh

test:
> mvn -q -DskipITs=false test

run:
> ./scripts/run.sh

stop:
> ./scripts/stop.sh

tail:
> ./scripts/tail.sh

