FROM ubuntu

RUN apt update && apt -qqy install build-essential curl netcat-openbsd
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN ~/.cargo/bin/cargo install cargo-watch

WORKDIR /home/mamenoki
CMD bash -c "~/.cargo/bin/cargo watch -x check -x test"
