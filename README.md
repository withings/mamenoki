# Welcome to Beanstalkclient!

This is an implementation in Rust of a Beanstalkd client based on the tokio runtime.

## Building and running

Build with :

    $ cargo doc --no-deps    # for auto docs
    $ cargo build --release  # actual build


## Tests

You can run all the tests locally or in a CI environment with:

    $ docker compose -f docker-compose-cicd.yml run all_tests

You can continuously run all the tests during the development (they will be re-run at every change) with:

    $ docker compose up


## Usage examples

You can see an usage example of this library in `example/connection_and_request.rs`.
You can run this code with

    $ docker compose -f docker-compose-cicd.yml run usage_example