# DBCop

## Install

1.  Clone it.

2.  Compile and install using `cargo` and run.
    Make sure `~/.cargo/bin` is in your system path.
```
    cd dbcop
    cargo install --path .
    dbcop --help
```

## Use

There are a few `docker-compose` files in `docker` directory to create docker cluster.

The workflow goes like this,

1. Generate a bunch of histories to execute on a database.
2. Execute those histories on a database using provided `traits`. (see in `examples`).
3. Verify the executed histories for `--cc`(causal consistency), `--si`(snapshot isolation), `--ser`(serialization).
