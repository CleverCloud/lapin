use futures_executor::LocalPool;
use lapin::{
    message::DeliveryResult, options::*, publisher_confirm::Confirmation, tcp::AMQPUriTcpExt,
    types::FieldTable, BasicProperties, CloseOnDrop, Connection, ConnectionProperties,
    ConsumerDelegate, Result,
};
use log::info;
use std::{future::Future, pin::Pin};
use tcp_stream::{HandshakeError, NativeTlsConnector};

#[derive(Clone, Debug, PartialEq)]
struct Subscriber;

impl ConsumerDelegate for Subscriber {
    fn on_new_delivery(
        &self,
        delivery: DeliveryResult,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            info!("received message: {:?}", delivery);
        })
    }
}

async fn connect() -> Result<CloseOnDrop<Connection>> {
    // You need to use amqp:// scheme here to handle the TLS part manually as it's automatic when you use amqps://
    std::env::var("AMQP_ADDR")
        .unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into())
        .connect(|stream, uri, poll| {
            let tls_builder = NativeTlsConnector::builder();
            // Perform here your custom TLS setup, with tls_builder.identity or whatever else you need
            let mut res = stream.into_native_tls(
                tls_builder.build().expect("TLS configuration failed"),
                &uri.authority.host,
            );
            while let Err(error) = res {
                match error {
                    HandshakeError::Failure(io_err) => {
                        panic!("TLS connection failed: {:?}", io_err)
                    }
                    HandshakeError::WouldBlock(mid) => res = mid.handshake(),
                }
            }
            let stream = res.unwrap();
            Connection::connector(ConnectionProperties::default())(stream, uri, poll)
        })??
        .await
}

fn main() {
    std::env::set_var("RUST_LOG", "info");

    env_logger::init();

    let mut executor = LocalPool::new();

    executor.run_until(async {
        let conn = connect().await.expect("connection error");

        info!("CONNECTED");

        //send channel
        let channel_a = conn.create_channel().await.expect("create_channel");
        //receive channel
        let channel_b = conn.create_channel().await.expect("create_channel");
        info!("[{}] state: {:?}", line!(), conn.status().state());

        //create the hello queue
        let queue = channel_a
            .queue_declare(
                "hello",
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("queue_declare");
        info!("[{}] state: {:?}", line!(), conn.status().state());
        info!("[{}] declared queue: {:?}", line!(), queue);

        info!("will consume");
        channel_b
            .basic_consume(
                "hello",
                "my_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("basic_consume")
            .set_delegate(Subscriber);
        info!("[{}] state: {:?}", line!(), conn.status().state());

        info!("will publish");
        let payload = b"Hello world!";
        let confirm = channel_a
            .basic_publish(
                "",
                "hello",
                BasicPublishOptions::default(),
                payload.to_vec(),
                BasicProperties::default(),
            )
            .await
            .expect("basic_publish")
            .await
            .expect("publisher-confirms");
        assert_eq!(confirm, Confirmation::NotRequested);
        info!("[{}] state: {:?}", line!(), conn.status().state());
    })
}
