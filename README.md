# Welcome to Mamenoki!

This is a Rust implementation of a Beanstalkd client based on the tokio runtime.

## Building and running

Build with :

    $ cargo doc --no-deps    # for auto docs
    $ cargo build --release  # actual build


## Tests

You can run all the tests locally or in a CI environment with:

    $ docker compose up all_tests --build

You can continuously run all the tests during the development (they will be re-run at every change) with:

    $ docker compose up dev_test_loop --build


## Usage examples

There are some usage examples in the [examples](examples) folder: they all create a connection to beanstalkd and the send different commands to it.
 * [reader.rs](examples/reader.rs) watches a beanstalk tube, reserves a job and deletes it. You can run it with `docker compose up example_reader`.
 * [writer.rs](examples/writer.rs) uses a tube and puts a job into it. You can run it with `docker compose up example_reader`.
 * [stats.rs](examples/stats.rs) requests the global beanstalkd stats and the stats for a tube. You can run it with `docker compose up example_stats`.