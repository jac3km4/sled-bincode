#![feature(generic_associated_types)]

use std::marker::PhantomData;
use std::ops::RangeBounds;

mod result;
pub use result::{Error, Result, TransactionError};
pub use {bincode, sled};

pub trait Entry {
    type Key<'a>: bincode::BorrowDecode<'a> + bincode::Encode;
    type Val<'a>: bincode::BorrowDecode<'a> + bincode::Encode;
}

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

impl<A: Entry> Tree<A> {
    #[inline]
    pub fn insert(&self, key: &A::Key<'_>, value: &A::Val<'_>) -> Result<Option<Value<A>>> {
        let key = encode(key)?;
        let val = encode(value)?;
        Ok(self.raw.insert(key, val)?.map(Value::new))
    }

    #[inline]
    pub fn get(&self, key: &A::Key<'_>) -> Result<Option<Value<A>>> {
        Ok(self.raw.get(encode(key)?)?.map(Value::new))
    }

    #[inline]
    pub fn remove(&self, key: &A::Key<'_>) -> Result<Option<Value<A>>> {
        Ok(self.raw.remove(encode(key)?)?.map(Value::new))
    }

    #[inline]
    pub fn range<'a, R: RangeBounds<A::Key<'a>>>(&self, range: R) -> Result<Iter<A>> {
        let start = encode(range.start_bound())?;
        let end = encode(range.end_bound())?;
        Ok(Iter::new(self.raw.range(start..end)))
    }

    #[inline]
    pub fn scan_prefix(&self, prefix: &A::Key<'_>) -> Result<Iter<A>> {
        Ok(Iter::new(self.raw.scan_prefix(encode(prefix)?)))
    }
}

#[derive(Debug, Default)]
pub struct Batch<A> {
    raw: sled::Batch,
    phantom: PhantomData<A>,
}

impl<A: Entry> Batch<A> {
    #[inline]
    pub fn insert(&mut self, key: &A::Key<'_>, val: &A::Val<'_>) -> Result<()> {
        self.raw.insert(encode(key)?, encode(val)?);
        Ok(())
    }

    #[inline]
    pub fn remove(&mut self, key: &A::Key<'_>) -> Result<()> {
        self.raw.remove(encode(key)?);
        Ok(())
    }
}

pub struct Value<E> {
    raw: sled::IVec,
    phantom: PhantomData<E>,
}

impl<E> Value<E> {
    #[inline]
    fn new(raw: sled::IVec) -> Self {
        Self {
            raw,
            phantom: PhantomData,
        }
    }
}

impl<E: Entry> Value<E> {
    #[inline]
    pub fn value(&self) -> Result<E::Val<'_>> {
        decode(&self.raw)
    }
}

pub struct KeyValue<E> {
    raw_key: sled::IVec,
    raw_value: sled::IVec,
    phantom: PhantomData<E>,
}

impl<E> KeyValue<E> {
    #[inline]
    fn new(raw_key: sled::IVec, raw_value: sled::IVec) -> Self {
        Self {
            raw_key,
            raw_value,
            phantom: PhantomData,
        }
    }
}

impl<E: Entry> KeyValue<E> {
    #[inline]
    pub fn key(&self) -> Result<E::Key<'_>> {
        decode(&self.raw_key)
    }

    #[inline]
    pub fn value(&self) -> Result<E::Val<'_>> {
        decode(&self.raw_value)
    }
}

type TransactionResult<A> = Result<A, sled::transaction::UnabortableTransactionError>;

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
    pub fn apply_batch(&self, batch: &Batch<A>) -> TransactionResult<()> {
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

impl<'a, A: Entry> TransactionalTree<'a, A> {
    pub fn insert(&self, key: &A::Key<'_>, val: &A::Val<'_>) -> TransactionResult<Option<Value<A>>> {
        let key = encode(key).expect("key encoding failed");
        let val = encode(val).expect("value encoding failed");
        Ok(self.raw.insert(key, val)?.map(Value::new))
    }

    pub fn remove(&self, key: &A::Key<'_>) -> TransactionResult<Option<Value<A>>> {
        let key = encode(key).expect("key encoding failed");
        Ok(self.raw.remove(key)?.map(Value::new))
    }

    pub fn get(&self, key: &A::Key<'_>) -> TransactionResult<Option<Value<A>>> {
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

#[inline]
fn decode<'a, A: bincode::BorrowDecode<'a>>(buf: &'a [u8]) -> Result<A> {
    let (val, _) =
        bincode::decode_from_slice(buf, bincode::config::standard()).map_err(Error::DecodeError)?;
    Ok(val)
}

#[inline]
fn encode<A: bincode::Encode>(val: A) -> Result<Vec<u8>> {
    bincode::encode_to_vec(val, bincode::config::standard()).map_err(Error::EncodeError)
}
