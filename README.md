# sled-bincode
Typed wrapper around [sled](https://github.com/spacejam/sled) using [bincode](https://github.com/bincode-org/bincode) for serialization.

I've built this because the existing sled wrappers (typed-sled entity-sled) were missing some features:
- multi-tree transactions ([example](tests/tests.rs#L75-L82=))
- support for zero-copy parsing
- smallvec optimization (can make a big difference for frequent `get`s)

See [tests](tests/tests.rs) for examples.

## features
- `serde` - allows use of serde `Deserialize` and `Serialize` instead of bincode traits
