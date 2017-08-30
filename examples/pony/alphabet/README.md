# Alphabet

This is an example application that will count the number of "votes" sent for
each letter of the alphabet and send out the previous count for that letter at the end of each update.

## Prerequisites

- ponyc
- pony-stable
- Wallaroo

See [Wallaroo Environment Setup Instructions](https://github.com/sendence/wallaroo/book/getting-started/setup.md).

## Building

Build Alphabet with

```bash
stable env ponyc
```

## Generating Data

A data generator is bundled with the application. It needs to be built:

```bash
cd data_gen
stable env ponyc
```

Then you can generate a file with a fixed number of psuedo-random votes:

```
./data_gen --message-count 1000
```

This will create a `votes.msg` file in your current working directory.

## Running Alphabet

In a separate shell, each:

0. In a shell, start up the Metrics UI if you don't already have it running:

```bash
docker start mui
```

1. Start a listener

```bash
../../../../giles/receiver/receiver --listen 127.0.0.1:7002
```

2. Start the application

```bash
./alphabet --in 127.0.0.1:7010 --out 127.0.0.1:7002 --metrics 127.0.0.1:5001 \
  --control 127.0.0.1:12500 --data 127.0.0.1:12501
```

3. Start a sender

```bash
../../../../giles/sender/sender --host 127.0.0.1:7010 \
  --file data_gen/votes.msg \ --batch-size 5 --interval 100_000_000 \
  --messages 150000000 --binary --variable-size --repeat --ponythreads=1 --no-write
```