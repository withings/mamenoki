# Welcome to Beanstalkclient!

This is an implementation in Rust of a Beanstalkd client based on the tokio runtime.

## Building and running

Build with :

    $ cargo doc --no-deps    # for auto docs
    $ cargo build --release  # actual build


## Usage examples

You can see an usage example of this library in `example/connection_and_request.rs`.
You can run that code with

    $ docker compose up
    $ cargo run --example connection_and_request