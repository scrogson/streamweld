//! Implementations of trait combinators defined in the traits module.

use crate::error::Result;
use crate::traits::{Chain, Consumer, Contramap, Filter, Map, Producer, Take};
use async_trait::async_trait;
use std::marker::PhantomData;

// Map combinator implementation
#[async_trait]
impl<P, F, U> Producer for Map<P, F>
where
    P: Producer + Send,
    F: FnMut(P::Item) -> U + Send,
    U: Send + 'static,
{
    type Item = U;

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        match self.producer.produce().await? {
            Some(item) => Ok(Some((self.f)(item))),
            None => Ok(None),
        }
    }
}

// Filter combinator implementation
#[async_trait]
impl<P, F> Producer for Filter<P, F>
where
    P: Producer + Send,
    F: FnMut(&P::Item) -> bool + Send,
{
    type Item = P::Item;

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        loop {
            match self.producer.produce().await? {
                Some(item) => {
                    if (self.predicate)(&item) {
                        return Ok(Some(item));
                    }
                    // Continue to next item if predicate fails
                }
                None => return Ok(None),
            }
        }
    }
}

// Take combinator implementation
#[async_trait]
impl<P> Producer for Take<P>
where
    P: Producer + Send,
{
    type Item = P::Item;

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        if self.remaining == 0 {
            return Ok(None);
        }

        match self.producer.produce().await? {
            Some(item) => {
                self.remaining -= 1;
                Ok(Some(item))
            }
            None => Ok(None),
        }
    }
}

// Chain combinator implementation
#[async_trait]
impl<P1, P2> Producer for Chain<P1, P2>
where
    P1: Producer + Send,
    P2: Producer<Item = P1::Item> + Send,
{
    type Item = P1::Item;

    async fn produce(&mut self) -> Result<Option<Self::Item>> {
        if let Some(ref mut first) = self.first {
            match first.produce().await? {
                Some(item) => Ok(Some(item)),
                None => {
                    // First producer exhausted, switch to second
                    self.first = None;
                    self.second.produce().await
                }
            }
        } else {
            self.second.produce().await
        }
    }
}

// Contramap combinator implementation
#[async_trait]
impl<C, F, T> Consumer for Contramap<C, F, T>
where
    C: Consumer + Send,
    F: FnMut(T) -> C::Item + Send + Sync,
    T: Send + 'static,
{
    type Item = T;

    async fn consume(&mut self, item: Self::Item) -> Result<()> {
        let mapped_item = (self.f)(item);
        self.consumer.consume(mapped_item).await
    }

    async fn finish(&mut self) -> Result<()> {
        self.consumer.finish().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impls::consumers::*;
    use crate::impls::producers::*;
    use crate::traits::ProducerExt;

    #[tokio::test]
    async fn test_map_combinator() {
        let producer = RangeProducer::new(1..6);
        let mut mapped = producer.map(|x| x * 2);

        let mut results = Vec::new();
        while let Some(item) = mapped.produce().await.unwrap() {
            results.push(item);
        }

        assert_eq!(results, vec![2, 4, 6, 8, 10]);
    }

    #[tokio::test]
    async fn test_filter_combinator() {
        let producer = RangeProducer::new(1..11);
        let mut filtered = producer.filter(|x| x % 2 == 0);

        let mut results = Vec::new();
        while let Some(item) = filtered.produce().await.unwrap() {
            results.push(item);
        }

        assert_eq!(results, vec![2, 4, 6, 8, 10]);
    }

    #[tokio::test]
    async fn test_take_combinator() {
        let producer = RangeProducer::new(1..11);
        let mut taken = producer.take(3);

        let mut results = Vec::new();
        while let Some(item) = taken.produce().await.unwrap() {
            results.push(item);
        }

        assert_eq!(results, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_chain_combinator() {
        let producer1 = RangeProducer::new(1..4);
        let producer2 = RangeProducer::new(4..7);
        let mut chained = producer1.chain(producer2);

        let mut results = Vec::new();
        while let Some(item) = chained.produce().await.unwrap() {
            results.push(item);
        }

        assert_eq!(results, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_complex_combination() {
        let producer = RangeProducer::new(1..21);
        let mut complex = producer
            .filter(|x| x % 2 == 0) // Even numbers
            .map(|x| x * 3) // Multiply by 3
            .take(3); // Take first 3

        let mut results = Vec::new();
        while let Some(item) = complex.produce().await.unwrap() {
            results.push(item);
        }

        assert_eq!(results, vec![6, 12, 18]); // 2*3, 4*3, 6*3
    }
}
