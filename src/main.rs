use async_nats::jetstream::{
    self,
    consumer::{PullConsumer, PushConsumer},
};
use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use std::{env, time::Duration};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let mut streams = Vec::with_capacity(5);
    for i in 0..5 {
        let client = async_nats::connect(&nats_url).await?;
        let jetstream = jetstream::new(client);
        let stream_name = String::from(format!("EVENTS-{}", i));
        let consumer: PullConsumer = jetstream
            .create_stream(jetstream::stream::Config {
                name: stream_name,
                subjects: vec![format!("events.{}", i)],
                ..Default::default()
            })
            .await?
            // Then, on that `Stream` use method to create Consumer and bind to it too.
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some(format!("consumer-{}", i)),
                ..Default::default()
            })
            .await?;
        // Create a stream and a consumer.
        // We can chain the methods.
        // First we create a stream and bind to it.
        streams.push((jetstream, consumer));
    }
    //let stream_name = String::from("EVENTS");

    /*

    */

    let payload = Bytes::from(vec![0; 1000]);

    let mut join_set = JoinSet::new();

    // Spawn a task for each stream to publish messages concurrently
    for (idx, (stream, consumer)) in streams.into_iter().enumerate() {
        let stream_payload = payload.clone();
        join_set.spawn(async move {
            for _ in 0..1000 {
                for i in 0..1000 {
                    let res = stream
                        .publish(format!("events.{}", idx), stream_payload.clone())
                        .await
                        .unwrap();
                    if i == 999 {
                        res.await.unwrap();
                    }
                }

                let mut messages = consumer.batch().max_messages(1000).messages().await.unwrap();
                while let Some(message) = messages.next().await {
                    let message = message.unwrap();
                    // acknowledge the message
                    message.ack().await.unwrap();
                }
            }
            idx
        });
    }

    // Wait for all publishing tasks to complete
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(stream_idx) => println!("stream {} completed", stream_idx),
            Err(e) => eprintln!("A stream task failed: {}", e),
        }
    }

    Ok(())
}
