use std::marker::PhantomData;
use std::ops::RangeBounds;

mod result;
pub use result::{Error, Result};
pub use sled::transaction::{ConflictableTransactionError, TransactionError};
pub use sled::{open, Db, Error as SledError};

#[cfg(not(feature = "serde"))]
pub trait Entry<'a> {
    type Key: bincode::BorrowDecode<'a> + bincode::Encode;
    type Val: bincode::BorrowDecode<'a> + bincode::Encode;
}

#[cfg(feature = "serde")]
pub trait Entry<'a> {
    type Key: serde::Deserialize<'a> + serde::Serialize;
    type Val: serde::Deserialize<'a> + serde::Serialize;
}

type KeyOf<'a, A> = <A as Entry<'a>>::Key;
type ValOf<'a, A> = <A as Entry<'a>>::Val;

pub struct Tree<A> {
    raw: sled::Tree,
    phantom: PhantomData<A>,
}

impl<A> Tree<A> {
    pub fn open<S: AsRef<[u8]>>(db: &sled::Db, name: S) -> Result<Self> {
        let tree = Self {
            raw: db.open_tree(name)?,
            phantom: PhantomData,
        };
        Ok(tree)
    }

    #[inline]
    pub fn transaction<F, R, E>(&self, f: F) -> sled::transaction::TransactionResult<R, E>
    where
        F: Fn(TransactionalTree<A>) -> sled::transaction::ConflictableTransactionResult<R, E>,
    {
        self.raw.transaction(|t| f(TransactionalTree::new(t)))
    }

    #[inline]
    pub fn apply_batch(&self, batch: Batch<A>) -> Result<()> {
        Ok(self.raw.apply_batch(batch.raw)?)
    }

    #[inline]
    pub async fn flush_async(&self) -> Result<usize> {
        Ok(self.raw.flush_async().await?)
    }

    #[inline]
    pub fn iter(&self) -> Iter<A> {
        Iter::new(self.raw.iter())
    }

    #[inline]
    pub fn pop_max(&self) -> Result<Option<KeyValue<A>>> {
        Ok(self.raw.pop_max()?.map(|(k, v)| KeyValue::new(k, v)))
    }

    #[inline]
    pub fn pop_min(&self) -> Result<Option<KeyValue<A>>> {
        Ok(self.raw.pop_min()?.map(|(k, v)| KeyValue::new(k, v)))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    #[inline]
    pub fn clear(&self) -> Result<()> {
        Ok(self.raw.clear()?)
    }
}

impl<A: for<'a> Entry<'a>> Tree<A> {
    #[inline]
    pub fn insert(&self, key: &KeyOf<A>, value: &ValOf<A>) -> Result<Option<Value<A>>> {
        let key = encode(key)?;
        let val = encode(value)?;
        Ok(self.raw.insert(key, val)?.map(Value::new))
    }

    #[inline]
    pub fn get(&self, key: &KeyOf<A>) -> Result<Option<Value<A>>> {
        Ok(self.raw.get(encode(key)?)?.map(Value::new))
    }

    #[inline]
    pub fn remove(&self, key: &KeyOf<A>) -> Result<Option<Value<A>>> {
        Ok(self.raw.remove(encode(key)?)?.map(Value::new))
    }

    #[inline]
    pub fn range<'a, R: RangeBounds<KeyOf<'a, A>>>(&self, range: R) -> Result<Iter<A>> {
        let start = encode(range.start_bound())?;
        let end = encode(range.end_bound())?;
        Ok(Iter::new(self.raw.range(start..end)))
    }

    #[inline]
    pub fn scan_prefix(&self, prefix: &KeyOf<A>) -> Result<Iter<A>> {
        Ok(Iter::new(self.raw.scan_prefix(encode(prefix)?)))
    }
}

#[derive(Debug, Default)]
pub struct Batch<A> {
    raw: sled::Batch,
    phantom: PhantomData<A>,
}

impl<A: for<'a> Entry<'a>> Batch<A> {
    #[inline]
    pub fn insert(&mut self, key: &KeyOf<A>, val: &ValOf<A>) -> Result<()> {
        self.raw.insert(encode(key)?, encode(val)?);
        Ok(())
    }

    #[inline]
    pub fn remove(&mut self, key: &KeyOf<A>) -> Result<()> {
        self.raw.remove(encode(key)?);
        Ok(())
    }
}

pub struct Value<A> {
    raw: sled::IVec,
    phantom: PhantomData<A>,
}

impl<A> Value<A> {
    #[inline]
    fn new(raw: sled::IVec) -> Self {
        Self {
            raw,
            phantom: PhantomData,
        }
    }
}

impl<A: for<'a> Entry<'a>> Value<A> {
    #[inline]
    pub fn value(&self) -> Result<ValOf<A>> {
        decode(&self.raw)
    }
}

impl<A> bincode::Encode for Value<A>
where
    A: for<'a> Entry<'a>,
    for<'a> ValOf<'a, A>: bincode::Encode,
{
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.value()
            .map_err(|err| bincode::error::EncodeError::OtherString(err.to_string()))?
            .encode(encoder)
    }
}

#[cfg(feature = "serde")]
impl<A> serde::Serialize for Value<A>
where
    A: for<'a> Entry<'a>,
    for<'a> ValOf<'a, A>: serde::Serialize,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let val = self.value().map_err(serde::ser::Error::custom)?;
        val.serialize(serializer)
    }
}

pub struct Key<A> {
    raw: sled::IVec,
    phantom: PhantomData<A>,
}

impl<A> Key<A> {
    #[inline]
    fn new(raw: sled::IVec) -> Self {
        Self {
            raw,
            phantom: PhantomData,
        }
    }
}

impl<A: for<'a> Entry<'a>> Key<A> {
    #[inline]
    pub fn key(&self) -> Result<KeyOf<A>> {
        decode(&self.raw)
    }
}

impl<A> bincode::Encode for Key<A>
where
    A: for<'a> Entry<'a>,
    for<'a> KeyOf<'a, A>: bincode::Encode,
{
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.key()
            .map_err(|err| bincode::error::EncodeError::OtherString(err.to_string()))?
            .encode(encoder)
    }
}

#[cfg(feature = "serde")]
impl<A> serde::Serialize for Key<A>
where
    A: for<'a> Entry<'a>,
    for<'a> KeyOf<'a, A>: serde::Serialize,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let val = self.key().map_err(serde::ser::Error::custom)?;
        val.serialize(serializer)
    }
}

pub struct KeyValue<A> {
    raw_key: sled::IVec,
    raw_value: sled::IVec,
    phantom: PhantomData<A>,
}

impl<A> KeyValue<A> {
    #[inline]
    fn new(raw_key: sled::IVec, raw_value: sled::IVec) -> Self {
        Self {
            raw_key,
            raw_value,
            phantom: PhantomData,
        }
    }

    #[inline]
    pub fn into_key(self) -> Key<A> {
        Key::new(self.raw_value)
    }

    #[inline]
    pub fn into_value(self) -> Value<A> {
        Value::new(self.raw_value)
    }
}

impl<A: for<'a> Entry<'a>> KeyValue<A> {
    #[inline]
    pub fn key(&self) -> Result<KeyOf<A>> {
        decode(&self.raw_key)
    }

    #[inline]
    pub fn value(&self) -> Result<ValOf<A>> {
        decode(&self.raw_value)
    }
}

type TransactionalResult<A> = Result<A, sled::transaction::UnabortableTransactionError>;

pub struct TransactionalTree<'a, A> {
    raw: &'a sled::transaction::TransactionalTree,
    phantom: PhantomData<A>,
}

impl<'a, A> TransactionalTree<'a, A> {
    #[inline]
    fn new(raw: &'a sled::transaction::TransactionalTree) -> Self {
        Self {
            raw,
            phantom: PhantomData,
        }
    }

    #[inline]
    pub fn apply_batch(&self, batch: &Batch<A>) -> TransactionalResult<()> {
        self.raw.apply_batch(&batch.raw)
    }

    #[inline]
    pub fn flush(&self) {
        self.raw.flush()
    }

    #[inline]
    pub fn generate_id(&self) -> Result<u64> {
        Ok(self.raw.generate_id()?)
    }
}

impl<'a, A: for<'v> Entry<'v>> TransactionalTree<'a, A> {
    pub fn insert(&self, key: &KeyOf<A>, val: &ValOf<A>) -> TransactionalResult<Option<Value<A>>> {
        let key = encode(key).expect("key encoding failed");
        let val = encode(val).expect("value encoding failed");
        Ok(self.raw.insert(key, val)?.map(Value::new))
    }

    pub fn remove(&self, key: &KeyOf<A>) -> TransactionalResult<Option<Value<A>>> {
        let key = encode(key).expect("key encoding failed");
        Ok(self.raw.remove(key)?.map(Value::new))
    }

    pub fn get(&self, key: &KeyOf<A>) -> TransactionalResult<Option<Value<A>>> {
        let key = encode(key).expect("key encoding failed");
        Ok(self.raw.get(key)?.map(Value::new))
    }
}

pub trait Transactional<F, R, E> {
    fn transaction(self, fun: F) -> sled::transaction::TransactionResult<R, E>;
}

macro_rules! impl_transactable {
    ($($ty:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($ty,)* F, R, E> Transactional<F, R, E> for ($(&Tree<$ty>),*)
        where
            F: Fn($(TransactionalTree<$ty>),*) -> sled::transaction::ConflictableTransactionResult<R, E>,
        {
            #[inline]
            fn transaction(self, fun: F) -> sled::transaction::TransactionResult<R, E> {
                use sled::Transactional;
                let ($($ty,)*) = self;
                ($(&$ty.raw),*).transaction(|($($ty),*)| fun($(TransactionalTree::new($ty)),*))
            }
        }
    };
}

impl_transactable!(A, B);
impl_transactable!(A, B, C);
impl_transactable!(A, B, C, D);

pub struct Iter<A> {
    raw: sled::Iter,
    phantom: PhantomData<A>,
}

impl<A> Iter<A> {
    #[inline]
    fn new(raw: sled::Iter) -> Self {
        Self {
            raw,
            phantom: PhantomData,
        }
    }

    #[inline]
    pub fn keys(self) -> impl DoubleEndedIterator<Item = Result<Key<A>>> {
        self.raw.map(|r| {
            let (k, _) = r?;
            Ok(Key::new(k))
        })
    }

    #[inline]
    pub fn values(self) -> impl DoubleEndedIterator<Item = Result<Value<A>>> {
        self.raw.map(|r| {
            let (_, v) = r?;
            Ok(Value::new(v))
        })
    }
}

impl<A> Iterator for Iter<A> {
    type Item = Result<KeyValue<A>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.raw.next().map(|res| {
            let (k, v) = res?;
            Ok(KeyValue::new(k, v))
        })
    }
}

impl<A> DoubleEndedIterator for Iter<A> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.raw.next_back().map(|res| {
            let (k, v) = res?;
            Ok(KeyValue::new(k, v))
        })
    }
}

#[cfg(not(feature = "serde"))]
#[inline]
fn decode<'a, A: bincode::BorrowDecode<'a>>(buf: &'a [u8]) -> Result<A> {
    let (val, _) =
        bincode::decode_from_slice(buf, bincode::config::standard()).map_err(Error::DecodeError)?;
    Ok(val)
}

#[cfg(not(feature = "serde"))]
#[inline]
fn encode<A: bincode::Encode>(val: A) -> Result<Vec<u8>> {
    bincode::encode_to_vec(val, bincode::config::standard()).map_err(Error::EncodeError)
}

#[cfg(feature = "serde")]
#[inline]
fn decode<'a, A: serde::Deserialize<'a>>(buf: &'a [u8]) -> Result<A> {
    bincode::serde::decode_borrowed_from_slice(buf, bincode::config::standard()).map_err(Error::DecodeError)
}

#[cfg(feature = "serde")]
#[inline]
fn encode<A: serde::Serialize>(val: A) -> Result<Vec<u8>> {
    bincode::serde::encode_to_vec(val, bincode::config::standard()).map_err(Error::EncodeError)
}
