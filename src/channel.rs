use crate::{
    acknowledgement::Acknowledgements,
    auth::Credentials,
    basic_get_delivery::BasicGetDelivery,
    channel_closer::ChannelCloser,
    channel_receiver_state::DeliveryCause,
    channel_status::{ChannelState, ChannelStatus},
    connection_closer::ConnectionCloser,
    connection_status::{ConnectionState, ConnectionStep},
    consumer::Consumer,
    consumers::Consumers,
    frames::{ExpectedReply, Frames},
    internal_rpc::InternalRPCHandle,
    message::{BasicGetMessage, BasicReturnMessage, Delivery},
    protocol::{self, AMQPClass, AMQPError, AMQPHardError},
    publisher_confirm::PublisherConfirm,
    queue::Queue,
    registry::Registry,
    returned_messages::ReturnedMessages,
    socket_state::SocketStateHandle,
    topology::RestoredChannel,
    topology_internal::ChannelDefinitionInternal,
    types::*,
    BasicProperties, Configuration, Connection, ConnectionStatus, Error, ExchangeKind, Promise,
    PromiseResolver, Result,
};
use amq_protocol::frame::{AMQPContentHeader, AMQPFrame};
use executor_trait::FullExecutor;
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, fmt, sync::Arc};
use tracing::{error, info, level_enabled, trace, Level};

/// Main entry point for most AMQP operations.
///
/// It serves as a lightweight connection and can be obtained from a
///  [`Connection`] by calling [`Connection::create_channel`].
///
/// See also the RabbitMQ documentation on [channels](https://www.rabbitmq.com/channels.html).
///
/// [`Connection`]: ./struct.Connection.html
/// [`Connection::create_channel`]: ./struct.Connection.html#method.create_channel
#[derive(Clone)]
pub struct Channel {
    id: ChannelId,
    configuration: Configuration,
    status: ChannelStatus,
    connection_status: ConnectionStatus,
    global_registry: Registry,
    local_registry: Registry,
    acknowledgements: Acknowledgements,
    consumers: Consumers,
    basic_get_delivery: BasicGetDelivery,
    returned_messages: ReturnedMessages,
    waker: SocketStateHandle,
    internal_rpc: InternalRPCHandle,
    frames: Frames,
    executor: Arc<dyn FullExecutor + Send + Sync>,
    channel_closer: Option<Arc<ChannelCloser>>,
    connection_closer: Option<Arc<ConnectionCloser>>,
}

impl PartialEq for Channel {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Channel")
            .field("id", &self.id)
            .field("configuration", &self.configuration)
            .field("status", &self.status)
            .field("connection_status", &self.connection_status)
            .field("acknowledgements", &self.acknowledgements)
            .field("consumers", &self.consumers)
            .field("basic_get_delivery", &self.basic_get_delivery)
            .field("returned_messages", &self.returned_messages)
            .field("frames", &self.frames)
            .finish()
    }
}

impl Channel {
    pub(crate) fn new(
        channel_id: ChannelId,
        configuration: Configuration,
        connection_status: ConnectionStatus,
        global_registry: Registry,
        waker: SocketStateHandle,
        internal_rpc: InternalRPCHandle,
        frames: Frames,
        executor: Arc<dyn FullExecutor + Send + Sync>,
        connection_closer: Option<Arc<ConnectionCloser>>,
    ) -> Channel {
        let returned_messages = ReturnedMessages::default();
        let status = ChannelStatus::default();
        let channel_closer = if channel_id == 0 {
            None
        } else {
            Some(Arc::new(ChannelCloser::new(
                channel_id,
                status.clone(),
                internal_rpc.clone(),
            )))
        };
        Channel {
            id: channel_id,
            configuration,
            status,
            connection_status,
            global_registry,
            local_registry: Registry::default(),
            acknowledgements: Acknowledgements::new(channel_id, returned_messages.clone()),
            consumers: Consumers::default(),
            basic_get_delivery: BasicGetDelivery::default(),
            returned_messages,
            waker,
            internal_rpc,
            frames,
            executor,
            channel_closer,
            connection_closer,
        }
    }

    pub fn status(&self) -> &ChannelStatus {
        &self.status
    }

    pub(crate) fn reset(&self) {
        // FIXME
    }

    pub(crate) async fn restore(
        &self,
        ch: &ChannelDefinitionInternal,
        c: &mut RestoredChannel,
    ) -> Result<()> {
        // First, redeclare all queues
        for queue in &ch.queues {
            if queue.is_declared() {
                c.queues.push(
                    self.queue_declare(
                        queue.name.as_str(),
                        queue.options.unwrap_or_default(),
                        queue.arguments.clone().unwrap_or_default(),
                    )
                    .await?,
                );
            }
        }

        // Second, redeclare all queues bindings
        for queue in &ch.queues {
            for binding in &queue.bindings {
                self.queue_bind(
                    queue.name.as_str(),
                    binding.source.as_str(),
                    binding.routing_key.as_str(),
                    QueueBindOptions::default(),
                    binding.arguments.clone(),
                )
                .await?;
            }
        }

        // Third, redeclare all consumers
        for consumer in &ch.consumers {
            let original = consumer.original();
            if let Some(original) = original.as_ref() {
                original.reset();
            }
            c.consumers.push(
                self.do_basic_consume(
                    consumer.queue.as_str(),
                    consumer.tag.as_str(),
                    consumer.options,
                    consumer.arguments.clone(),
                    original,
                )
                .await?,
            );
        }

        // Fourth, reemit pending basic_get
        if let Some(original) = self.basic_get_delivery.recover() {
            self.do_basic_get(
                original.queue.as_str(),
                original.options,
                Some(original.resolver),
            )
            .await?;
        }

        Ok(())
    }

    fn set_closing(&self, error: Option<Error>) {
        self.set_state(ChannelState::Closing);
        if let Some(error) = error {
            self.error_publisher_confirms(error.clone());
            let _ = self.error_consumers(error.clone()); // ignore the error here, only happens with default executor if we cannot spawn a thread
        } else {
            self.consumers.start_cancel();
        }
    }

    fn set_closed(&self, error: Error) {
        self.set_state(ChannelState::Closed);
        self.error_publisher_confirms(error.clone());
        self.cancel_consumers();
        self.internal_rpc.remove_channel(self.id, error);
    }

    fn set_error(&self, error: Error) {
        self.set_state(ChannelState::Error);
        self.error_publisher_confirms(error.clone());
        self.error_consumers(error.clone());
        self.internal_rpc.remove_channel(self.id, error);
    }

    pub(crate) fn error_publisher_confirms(&self, error: Error) {
        self.acknowledgements.on_channel_error(error);
    }

    pub(crate) fn cancel_consumers(&self) {
        self.consumers.cancel();
    }

    pub(crate) fn error_consumers(&self, error: Error) {
        self.consumers.error(error);
    }

    pub(crate) fn set_state(&self, state: ChannelState) {
        self.status.set_state(state);
    }

    pub fn id(&self) -> ChannelId {
        self.id
    }

    pub(crate) fn clone_internal(&self) -> Self {
        Self {
            id: self.id,
            configuration: self.configuration.clone(),
            status: self.status.clone(),
            connection_status: self.connection_status.clone(),
            global_registry: self.global_registry.clone(),
            local_registry: self.local_registry.clone(),
            acknowledgements: self.acknowledgements.clone(),
            consumers: self.consumers.clone(),
            basic_get_delivery: self.basic_get_delivery.clone(),
            returned_messages: self.returned_messages.clone(),
            waker: self.waker.clone(),
            internal_rpc: self.internal_rpc.clone(),
            frames: self.frames.clone(),
            executor: self.executor.clone(),
            channel_closer: None,
            connection_closer: self.connection_closer.clone(),
        }
    }

    fn wake(&self) {
        trace!(channel=%self.id, "wake");
        self.waker.wake()
    }

    fn assert_channel0(&self, class_id: Identifier, method_id: Identifier) -> Result<()> {
        if self.id == 0 {
            Ok(())
        } else {
            error!(
                channel=%self.id,
                "Got a connection frame on, closing connection"
            );
            let error = AMQPError::new(
                AMQPHardError::COMMANDINVALID.into(),
                format!("connection frame received on channel {}", self.id).into(),
            );
            self.internal_rpc.close_connection(
                error.get_id(),
                error.get_message().to_string(),
                class_id,
                method_id,
            );
            Err(Error::ProtocolError(error))
        }
    }

    pub async fn close(&self, reply_code: ReplyCode, reply_text: &str) -> Result<()> {
        self.do_channel_close(reply_code, reply_text, 0, 0).await
    }

    pub async fn basic_consume(
        &self,
        queue: &str,
        consumer_tag: &str,
        options: BasicConsumeOptions,
        arguments: FieldTable,
    ) -> Result<Consumer> {
        self.do_basic_consume(queue, consumer_tag, options, arguments, None)
            .await
    }

    pub async fn basic_get(
        &self,
        queue: &str,
        options: BasicGetOptions,
    ) -> Result<Option<BasicGetMessage>> {
        self.do_basic_get(queue, options, None).await
    }

    pub async fn exchange_declare(
        &self,
        exchange: &str,
        kind: ExchangeKind,
        options: ExchangeDeclareOptions,
        arguments: FieldTable,
    ) -> Result<()> {
        self.do_exchange_declare(exchange, kind.kind(), options, arguments, kind.clone())
            .await
    }

    pub async fn wait_for_confirms(&self) -> Result<Vec<BasicReturnMessage>> {
        if self
            .acknowledgements
            .get_last_pending()
            .await
            .transpose()?
            .is_some()
        {
            trace!("Waiting for pending confirms");
        } else {
            trace!("No confirms to wait for");
        }
        Ok(self.returned_messages.drain())
    }

    #[cfg(test)]
    pub(crate) fn register_queue(
        &self,
        name: ShortString,
        options: QueueDeclareOptions,
        arguments: FieldTable,
    ) {
        self.local_registry.register_queue(name, options, arguments);
    }

    #[cfg(test)]
    pub(crate) fn register_consumer(&self, tag: ShortString, consumer: Consumer) {
        self.consumers.register(tag, consumer);
    }

    pub(crate) fn send_method_frame(
        &self,
        method: AMQPClass,
        resolver: PromiseResolver<()>,
        expected_reply: Option<ExpectedReply>,
    ) {
        self.send_frame(AMQPFrame::Method(self.id, method), resolver, expected_reply);
    }

    pub(crate) fn send_frame(
        &self,
        frame: AMQPFrame,
        resolver: PromiseResolver<()>,
        expected_reply: Option<ExpectedReply>,
    ) {
        trace!(channel=%self.id, "send_frame");
        self.frames.push(self.id, frame, resolver, expected_reply);
        self.wake();
    }

    async fn send_method_frame_with_body(
        &self,
        method: AMQPClass,
        payload: &[u8],
        properties: BasicProperties,
        publisher_confirms_result: Option<PublisherConfirm>,
    ) -> Result<PublisherConfirm> {
        let class_id = method.get_amqp_class_id();
        let header = AMQPContentHeader {
            class_id,
            body_size: payload.len() as PayloadSize,
            properties,
        };
        let frame_max = self.configuration.frame_max();
        let mut frames = vec![
            AMQPFrame::Method(self.id, method),
            AMQPFrame::Header(self.id, class_id, Box::new(header)),
        ];

        // a content body frame 8 bytes of overhead
        frames.extend(
            payload
                .chunks(frame_max as usize - 8)
                .map(|chunk| AMQPFrame::Body(self.id, chunk.into())),
        );

        trace!(channel=%self.id, "send_frames");
        let promise = self.frames.push_frames(frames);
        self.wake();
        promise.await?;
        Ok(publisher_confirms_result
            .unwrap_or_else(|| PublisherConfirm::not_requested(self.returned_messages.clone())))
    }

    fn handle_invalid_contents(
        &self,
        error: String,
        class_id: Identifier,
        method_id: Identifier,
    ) -> Result<()> {
        error!(%error);
        let error = AMQPError::new(AMQPHardError::UNEXPECTEDFRAME.into(), error.into());
        self.internal_rpc.close_connection(
            error.get_id(),
            error.get_message().to_string(),
            class_id,
            method_id,
        );
        Err(Error::ProtocolError(error))
    }

    pub(crate) fn handle_content_header_frame(
        &self,
        class_id: Identifier,
        size: PayloadSize,
        properties: BasicProperties,
    ) -> Result<()> {
        self.status.set_content_length(
            self.id,
            class_id,
            size,
            |delivery_cause, confirm_mode| match delivery_cause {
                DeliveryCause::Consume(consumer_tag) => {
                    self.consumers
                        .handle_content_header_frame(consumer_tag, size, properties);
                }
                DeliveryCause::Get => {
                    self.basic_get_delivery
                        .handle_content_header_frame(size, properties);
                }
                DeliveryCause::Return => {
                    self.returned_messages.handle_content_header_frame(
                        size,
                        properties,
                        confirm_mode,
                    );
                }
            },
            |msg| {
                error!(%msg);
                let error = AMQPError::new(AMQPHardError::FRAMEERROR.into(), msg.into());
                self.internal_rpc.close_connection(
                    error.get_id(),
                    error.get_message().to_string(),
                    class_id,
                    0,
                );
                let error = Error::ProtocolError(error);
                self.set_error(error.clone());
                Err(error)
            },
            |msg| self.handle_invalid_contents(msg, class_id, 0),
        )
    }

    pub(crate) fn handle_body_frame(&self, payload: Vec<u8>) -> Result<()> {
        self.status.receive(
            self.id,
            payload.len() as PayloadSize,
            |delivery_cause, remaining_size, confirm_mode| match delivery_cause {
                DeliveryCause::Consume(consumer_tag) => {
                    self.consumers
                        .handle_body_frame(consumer_tag, remaining_size, payload);
                }
                DeliveryCause::Get => {
                    self.basic_get_delivery
                        .handle_body_frame(remaining_size, payload);
                }
                DeliveryCause::Return => {
                    self.returned_messages
                        .handle_body_frame(remaining_size, payload, confirm_mode);
                }
            },
            |msg| self.handle_invalid_contents(msg, 0, 0),
        )
    }

    pub(crate) fn topology(&self) -> ChannelDefinitionInternal {
        ChannelDefinitionInternal {
            channel: Some(self.clone()),
            queues: self.local_registry.queues_topology(true),
            consumers: self.consumers.topology(),
        }
    }

    fn before_basic_publish(&self) -> Option<PublisherConfirm> {
        if self.status.confirm() {
            Some(self.acknowledgements.register_pending())
        } else {
            None
        }
    }

    fn before_basic_cancel(&self, consumer_tag: &str) {
        self.consumers.start_cancel_one(consumer_tag);
    }

    fn acknowledgement_error(
        &self,
        error: AMQPError,
        class_id: Identifier,
        method_id: Identifier,
    ) -> Result<()> {
        error!("Got a bad acknowledgement from server, closing channel");
        let channel = self.clone();
        let err = error.clone();
        self.internal_rpc.register_internal_future(async move {
            channel
                .do_channel_close(
                    error.get_id(),
                    error.get_message().as_str(),
                    class_id,
                    method_id,
                )
                .await
        });
        Err(Error::ProtocolError(err))
    }

    fn on_connection_start_ok_sent(
        &self,
        resolver: PromiseResolver<Connection>,
        connection: Connection,
        credentials: Credentials,
    ) {
        self.connection_status
            .set_connection_step(ConnectionStep::StartOk(resolver, connection, credentials));
    }

    fn on_connection_open_sent(&self, resolver: PromiseResolver<Connection>) {
        self.connection_status
            .set_connection_step(ConnectionStep::Open(resolver));
    }

    fn on_connection_close_ok_sent(&self, error: Error) {
        if let Error::ProtocolError(_) = error {
            self.internal_rpc.set_connection_error(error);
        } else {
            self.internal_rpc.set_connection_closed(error);
        }
    }

    fn next_expected_close_ok_reply(&self) -> Option<Reply> {
        self.frames
            .next_expected_close_ok_reply(self.id, Error::InvalidChannelState(ChannelState::Closed))
    }

    fn before_channel_close(&self) {
        self.set_closing(None);
    }

    fn on_channel_close_ok_sent(&self, error: Error) {
        self.set_closed(error);
    }

    fn on_basic_recover_async_sent(&self) {
        self.consumers.drop_prefetched_messages();
    }

    fn on_basic_ack_sent(&self, multiple: bool, delivery_tag: DeliveryTag) {
        if multiple && delivery_tag == 0 {
            self.consumers.drop_prefetched_messages();
        }
    }

    fn on_basic_nack_sent(&self, multiple: bool, delivery_tag: DeliveryTag) {
        if multiple && delivery_tag == 0 {
            self.consumers.drop_prefetched_messages();
        }
    }

    fn tune_connection_configuration(
        &self,
        channel_max: ChannelId,
        frame_max: FrameSize,
        heartbeat: Heartbeat,
    ) {
        // If we disable the heartbeat (0) but the server don't, follow it and enable it too
        // If both us and the server want heartbeat enabled, pick the lowest value.
        if self.configuration.heartbeat() == 0
            || (heartbeat != 0 && heartbeat < self.configuration.heartbeat())
        {
            self.configuration.set_heartbeat(heartbeat);
        }

        if channel_max != 0 {
            // 0 means we want to take the server's value
            // If both us and the server specified a channel_max, pick the lowest value.
            if self.configuration.channel_max() == 0
                || channel_max < self.configuration.channel_max()
            {
                self.configuration.set_channel_max(channel_max);
            }
        }
        if self.configuration.channel_max() == 0 {
            self.configuration.set_channel_max(ChannelId::max_value());
        }

        if frame_max != 0 {
            // 0 means we want to take the server's value
            // If both us and the server specified a frame_max, pick the lowest value.
            if self.configuration.frame_max() == 0 || frame_max < self.configuration.frame_max() {
                self.configuration.set_frame_max(frame_max);
            }
        }
        if self.configuration.frame_max() == 0 {
            self.configuration.set_frame_max(FrameSize::max_value());
        }
    }

    fn on_connection_start_received(&self, method: protocol::connection::Start) -> Result<()> {
        trace!(?method, "Server sent connection::Start");
        let state = self.connection_status.state();
        if let (
            ConnectionState::Connecting,
            Some(ConnectionStep::ProtocolHeader(
                resolver,
                connection,
                credentials,
                mechanism,
                mut options,
            )),
        ) = (state.clone(), self.connection_status.connection_step())
        {
            let mechanism_str = mechanism.to_string();
            let locale = options.locale.clone();

            if !method
                .mechanisms
                .to_string()
                .split_whitespace()
                .any(|m| m == mechanism_str)
            {
                error!(%mechanism, "unsupported mechanism");
            }
            if !method
                .locales
                .to_string()
                .split_whitespace()
                .any(|l| l == locale)
            {
                error!(%locale, "unsupported locale");
            }

            if !options.client_properties.contains_key("product")
                || !options.client_properties.contains_key("version")
            {
                options.client_properties.insert(
                    "product".into(),
                    AMQPValue::LongString(env!("CARGO_PKG_NAME").into()),
                );
                options.client_properties.insert(
                    "version".into(),
                    AMQPValue::LongString(env!("CARGO_PKG_VERSION").into()),
                );
            }

            options
                .client_properties
                .insert("platform".into(), AMQPValue::LongString("rust".into()));

            let mut capabilities = FieldTable::default();
            capabilities.insert("publisher_confirms".into(), true.into());
            capabilities.insert("exchange_exchange_bindings".into(), true.into());
            capabilities.insert("basic.nack".into(), true.into());
            capabilities.insert("consumer_cancel_notify".into(), true.into());
            capabilities.insert("connection.blocked".into(), true.into());
            capabilities.insert("consumer_priorities".into(), true.into());
            capabilities.insert("authentication_failure_close".into(), true.into());
            capabilities.insert("per_consumer_qos".into(), true.into());
            capabilities.insert("direct_reply_to".into(), true.into());

            options
                .client_properties
                .insert("capabilities".into(), AMQPValue::FieldTable(capabilities));

            let channel = self.clone();
            self.internal_rpc.register_internal_future(async move {
                channel
                    .connection_start_ok(
                        options.client_properties,
                        &mechanism_str,
                        &credentials.sasl_auth_string(mechanism),
                        &locale,
                        resolver,
                        connection,
                        credentials,
                    )
                    .await
            });
            Ok(())
        } else {
            error!(?state, "Invalid state");
            let error = Error::InvalidConnectionState(state);
            self.internal_rpc.set_connection_error(error.clone());
            Err(error)
        }
    }

    fn on_connection_secure_received(&self, method: protocol::connection::Secure) -> Result<()> {
        trace!(?method, "Server sent connection::Secure");

        let state = self.connection_status.state();
        if let (ConnectionState::Connecting, Some(ConnectionStep::StartOk(.., credentials))) =
            (state.clone(), self.connection_status.connection_step())
        {
            let channel = self.clone();
            self.internal_rpc.register_internal_future(async move {
                channel
                    .connection_secure_ok(&credentials.rabbit_cr_demo_answer())
                    .await
            });
            Ok(())
        } else {
            error!(?state, "Invalid state");
            let error = Error::InvalidConnectionState(state);
            self.internal_rpc.set_connection_error(error.clone());
            Err(error)
        }
    }

    fn on_connection_tune_received(&self, method: protocol::connection::Tune) -> Result<()> {
        trace!(?method, "Server sent Connection::Tune");

        let state = self.connection_status.state();
        if let (
            ConnectionState::Connecting,
            Some(ConnectionStep::StartOk(resolver, connection, _)),
        ) = (state.clone(), self.connection_status.connection_step())
        {
            self.tune_connection_configuration(
                method.channel_max,
                method.frame_max,
                method.heartbeat,
            );

            let channel = self.clone();
            let configuration = self.configuration.clone();
            let vhost = self.connection_status.vhost();
            self.internal_rpc.register_internal_future(async move {
                channel
                    .connection_tune_ok(
                        configuration.channel_max(),
                        configuration.frame_max(),
                        configuration.heartbeat(),
                    )
                    .await?;
                channel.connection_open(&vhost, connection, resolver).await
            });
            Ok(())
        } else {
            error!(?state, "Invalid state");
            let error = Error::InvalidConnectionState(state);
            self.internal_rpc.set_connection_error(error.clone());
            Err(error)
        }
    }

    fn on_connection_open_ok_received(
        &self,
        _: protocol::connection::OpenOk,
        connection: Connection,
    ) -> Result<()> {
        let state = self.connection_status.state();
        if let (ConnectionState::Connecting, Some(ConnectionStep::Open(resolver))) =
            (state.clone(), self.connection_status.connection_step())
        {
            self.connection_status.set_state(ConnectionState::Connected);
            resolver.swear(Ok(connection));
            Ok(())
        } else {
            error!(?state, "Invalid state");
            let error = Error::InvalidConnectionState(state);
            self.internal_rpc.set_connection_error(error.clone());
            Err(error)
        }
    }

    fn on_connection_close_received(&self, method: protocol::connection::Close) -> Result<()> {
        let error = AMQPError::try_from(method.clone())
            .map(|error| {
                error!(
                    channel=%self.id,
                    ?method,
                    ?error,
                    "Connection closed",
                );
                Error::ProtocolError(error)
            })
            .unwrap_or_else(|error| {
                error!(%error);
                info!(channel=%self.id, ?method, "Connection closed");
                Error::InvalidConnectionState(ConnectionState::Closed)
            });
        self.internal_rpc.set_connection_closing();
        self.frames.drop_pending(error.clone());
        if let Some(resolver) = self.connection_status.connection_resolver() {
            resolver.swear(Err(error.clone()));
        }
        self.internal_rpc.send_connection_close_ok(error);
        Ok(())
    }

    fn on_connection_blocked_received(&self, _method: protocol::connection::Blocked) -> Result<()> {
        self.connection_status.block();
        Ok(())
    }

    fn on_connection_unblocked_received(
        &self,
        _method: protocol::connection::Unblocked,
    ) -> Result<()> {
        self.connection_status.unblock();
        self.wake();
        Ok(())
    }

    fn on_connection_close_ok_received(&self) -> Result<()> {
        self.internal_rpc
            .set_connection_closed(Error::InvalidConnectionState(ConnectionState::Closed));
        Ok(())
    }

    fn on_channel_open_ok_received(
        &self,
        _method: protocol::channel::OpenOk,
        resolver: PromiseResolver<Channel>,
        channel: Channel,
    ) -> Result<()> {
        self.set_state(ChannelState::Connected);
        resolver.swear(Ok(channel));
        Ok(())
    }

    fn on_channel_flow_received(&self, method: protocol::channel::Flow) -> Result<()> {
        self.status.set_send_flow(method.active);
        let channel = self.clone();
        self.internal_rpc.register_internal_future(async move {
            channel
                .channel_flow_ok(ChannelFlowOkOptions {
                    active: method.active,
                })
                .await
        });
        Ok(())
    }

    fn on_channel_flow_ok_received(
        &self,
        method: protocol::channel::FlowOk,
        resolver: PromiseResolver<Boolean>,
    ) -> Result<()> {
        // Nothing to do here, the server just confirmed that we paused/resumed the receiving flow
        resolver.swear(Ok(method.active));
        Ok(())
    }

    fn on_channel_close_received(&self, method: protocol::channel::Close) -> Result<()> {
        let error = AMQPError::try_from(method.clone()).map(|error| {
            error!(
                channel=%self.id, ?method, ?error,
                "Channel closed"
            );
            Error::ProtocolError(error)
        });
        self.set_closing(error.clone().ok());
        let error = error.unwrap_or_else(|error| {
            error!(%error);
            info!(channel=%self.id, ?method, "Channel closed");
            Error::InvalidChannelState(ChannelState::Closing)
        });
        let channel = self.clone();
        self.internal_rpc
            .register_internal_future(async move { channel.channel_close_ok(error).await });
        Ok(())
    }

    fn on_channel_close_ok_received(&self) -> Result<()> {
        self.set_closed(Error::InvalidChannelState(ChannelState::Closed));
        Ok(())
    }

    fn on_exchange_bind_ok_received(
        &self,
        destination: ShortString,
        source: ShortString,
        routing_key: ShortString,
        arguments: FieldTable,
    ) -> Result<()> {
        self.global_registry
            .register_exchange_binding(destination, source, routing_key, arguments);
        Ok(())
    }

    fn on_exchange_unbind_ok_received(
        &self,
        destination: ShortString,
        source: ShortString,
        routing_key: ShortString,
        arguments: FieldTable,
    ) -> Result<()> {
        self.global_registry.deregister_exchange_binding(
            destination.as_str(),
            source.as_str(),
            routing_key.as_str(),
            &arguments,
        );
        Ok(())
    }

    fn on_exchange_declare_ok_received(
        &self,
        resolver: PromiseResolver<()>,
        exchange: ShortString,
        kind: ExchangeKind,
        options: ExchangeDeclareOptions,
        arguments: FieldTable,
    ) -> Result<()> {
        self.global_registry
            .register_exchange(exchange, kind, options, arguments);
        resolver.swear(Ok(()));
        Ok(())
    }

    fn on_exchange_delete_ok_received(&self, exchange: ShortString) -> Result<()> {
        self.global_registry.deregister_exchange(exchange.as_str());
        Ok(())
    }

    fn on_queue_delete_ok_received(
        &self,
        method: protocol::queue::DeleteOk,
        resolver: PromiseResolver<MessageCount>,
        queue: ShortString,
    ) -> Result<()> {
        self.local_registry.deregister_queue(queue.as_str());
        self.global_registry.deregister_queue(queue.as_str());
        resolver.swear(Ok(method.message_count));
        Ok(())
    }

    fn on_queue_purge_ok_received(
        &self,
        method: protocol::queue::PurgeOk,
        resolver: PromiseResolver<MessageCount>,
    ) -> Result<()> {
        resolver.swear(Ok(method.message_count));
        Ok(())
    }

    fn on_queue_declare_ok_received(
        &self,
        method: protocol::queue::DeclareOk,
        resolver: PromiseResolver<Queue>,
        options: QueueDeclareOptions,
        arguments: FieldTable,
    ) -> Result<()> {
        if options.exclusive {
            self.local_registry
                .register_queue(method.queue.clone(), options, arguments.clone());
        }
        self.global_registry
            .register_queue(method.queue.clone(), options, arguments);
        resolver.swear(Ok(Queue::new(
            method.queue,
            method.message_count,
            method.consumer_count,
        )));
        Ok(())
    }

    fn on_queue_bind_ok_received(
        &self,
        queue: ShortString,
        exchange: ShortString,
        routing_key: ShortString,
        arguments: FieldTable,
    ) -> Result<()> {
        self.local_registry.register_queue_binding(
            queue.clone(),
            exchange.clone(),
            routing_key.clone(),
            arguments.clone(),
        );
        self.global_registry
            .register_queue_binding(queue, exchange, routing_key, arguments);
        Ok(())
    }

    fn on_queue_unbind_ok_received(
        &self,
        queue: ShortString,
        exchange: ShortString,
        routing_key: ShortString,
        arguments: FieldTable,
    ) -> Result<()> {
        self.local_registry.deregister_queue_binding(
            queue.as_str(),
            exchange.as_str(),
            routing_key.as_str(),
            &arguments,
        );
        self.global_registry.deregister_queue_binding(
            queue.as_str(),
            exchange.as_str(),
            routing_key.as_str(),
            &arguments,
        );
        Ok(())
    }

    fn on_basic_get_ok_received(
        &self,
        method: protocol::basic::GetOk,
        resolver: PromiseResolver<Option<BasicGetMessage>>,
        queue: ShortString,
        options: BasicGetOptions,
    ) -> Result<()> {
        let class_id = method.get_amqp_class_id();
        self.basic_get_delivery.start_new_delivery(
            queue,
            options,
            BasicGetMessage::new(
                self.id,
                method.delivery_tag,
                method.exchange,
                method.routing_key,
                method.redelivered,
                method.message_count,
                self.internal_rpc.clone(),
            ),
            resolver,
        );
        self.status.set_will_receive(class_id, DeliveryCause::Get);
        Ok(())
    }

    fn on_basic_get_empty_received(&self, method: protocol::basic::GetEmpty) -> Result<()> {
        match self.frames.next_expected_reply(self.id) {
            Some(Reply::BasicGetOk(resolver, ..)) => {
                resolver.swear(Ok(None));
                Ok(())
            }
            _ => self.handle_invalid_contents(
                format!("unexpected basic get empty received on channel {}", self.id),
                method.get_amqp_class_id(),
                method.get_amqp_method_id(),
            ),
        }
    }

    fn on_basic_consume_ok_received(
        &self,
        method: protocol::basic::ConsumeOk,
        resolver: PromiseResolver<Consumer>,
        channel_closer: Option<Arc<ChannelCloser>>,
        queue: ShortString,
        options: BasicConsumeOptions,
        arguments: FieldTable,
        original: Option<Consumer>,
    ) -> Result<()> {
        let consumer = original.unwrap_or_else(|| {
            Consumer::new(
                method.consumer_tag.clone(),
                self.executor.clone(),
                channel_closer,
                queue,
                options,
                arguments,
            )
        });
        let external_consumer = consumer.external(self.id, self.internal_rpc.clone());
        self.consumers.register(method.consumer_tag, consumer);
        resolver.swear(Ok(external_consumer));
        Ok(())
    }

    fn on_basic_deliver_received(&self, method: protocol::basic::Deliver) -> Result<()> {
        let class_id = method.get_amqp_class_id();
        let consumer_tag = method.consumer_tag.clone();
        self.consumers.start_delivery(&consumer_tag, |error| {
            Delivery::new(
                self.id,
                method.delivery_tag,
                method.exchange,
                method.routing_key,
                method.redelivered,
                Some(self.internal_rpc.clone()),
                Some(error),
            )
        });
        self.status
            .set_will_receive(class_id, DeliveryCause::Consume(consumer_tag));
        Ok(())
    }

    fn on_basic_cancel_received(&self, method: protocol::basic::Cancel) -> Result<()> {
        self.consumers.deregister(method.consumer_tag.as_str());
        if !method.nowait {
            let channel = self.clone();
            self.internal_rpc.register_internal_future(async move {
                channel.basic_cancel_ok(method.consumer_tag.as_str()).await
            });
        }
        Ok(())
    }

    fn on_basic_cancel_ok_received(&self, method: protocol::basic::CancelOk) -> Result<()> {
        self.consumers.deregister(method.consumer_tag.as_str());
        Ok(())
    }

    fn on_basic_ack_received(&self, method: protocol::basic::Ack) -> Result<()> {
        if self.status.confirm() {
            if method.multiple {
                if method.delivery_tag > 0 {
                    self.acknowledgements
                        .ack_all_before(method.delivery_tag)
                        .or_else(|err| {
                            self.acknowledgement_error(
                                err,
                                method.get_amqp_class_id(),
                                method.get_amqp_method_id(),
                            )
                        })?;
                } else {
                    self.acknowledgements.ack_all_pending();
                }
            } else {
                self.acknowledgements
                    .ack(method.delivery_tag)
                    .or_else(|err| {
                        self.acknowledgement_error(
                            err,
                            method.get_amqp_class_id(),
                            method.get_amqp_method_id(),
                        )
                    })?;
            }
        }
        Ok(())
    }

    fn on_basic_nack_received(&self, method: protocol::basic::Nack) -> Result<()> {
        if self.status.confirm() {
            if method.multiple {
                if method.delivery_tag > 0 {
                    self.acknowledgements
                        .nack_all_before(method.delivery_tag)
                        .or_else(|err| {
                            self.acknowledgement_error(
                                err,
                                method.get_amqp_class_id(),
                                method.get_amqp_method_id(),
                            )
                        })?;
                } else {
                    self.acknowledgements.nack_all_pending();
                }
            } else {
                self.acknowledgements
                    .nack(method.delivery_tag)
                    .or_else(|err| {
                        self.acknowledgement_error(
                            err,
                            method.get_amqp_class_id(),
                            method.get_amqp_method_id(),
                        )
                    })?;
            }
        }
        Ok(())
    }

    fn on_basic_return_received(&self, method: protocol::basic::Return) -> Result<()> {
        let class_id = method.get_amqp_class_id();
        self.returned_messages
            .start_new_delivery(BasicReturnMessage::new(
                method.exchange,
                method.routing_key,
                method.reply_code,
                method.reply_text,
            ));
        self.status
            .set_will_receive(class_id, DeliveryCause::Return);
        Ok(())
    }

    fn on_basic_recover_ok_received(&self) -> Result<()> {
        self.consumers.drop_prefetched_messages();
        Ok(())
    }

    fn on_confirm_select_ok_received(&self) -> Result<()> {
        self.status.set_confirm();
        Ok(())
    }

    fn on_access_request_ok_received(&self, _: protocol::access::RequestOk) -> Result<()> {
        Ok(())
    }
}

#[cfg(feature = "codegen")]
include!(concat!(env!("OUT_DIR"), "/channel.rs"));
#[cfg(not(feature = "codegen"))]
include!("generated.rs");
