"""Kafka service for MonkDB Data Platform."""

import structlog
import json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime

from api.config import KafkaConfig

logger = structlog.get_logger(__name__)


class KafkaService:
    """Kafka service for message production and consumption."""
    
    def __init__(self, config: KafkaConfig):
        """Initialize Kafka service."""
        self.config = config
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumers: Dict[str, AIOKafkaConsumer] = {}
        self._connected = False
    
    async def connect(self) -> None:
        """Connect to Kafka."""
        try:
            # Initialize producer
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                client_id=self.config.client_id,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            await self.producer.start()
            self._connected = True
            
            logger.info(
                "Connected to Kafka",
                bootstrap_servers=self.config.bootstrap_servers,
                client_id=self.config.client_id
            )
            
        except KafkaError as e:
            logger.error("Failed to connect to Kafka", error=str(e))
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from Kafka."""
        try:
            # Stop all consumers
            for consumer_id, consumer in self.consumers.items():
                await consumer.stop()
                logger.info("Stopped Kafka consumer", consumer_id=consumer_id)
            
            self.consumers.clear()
            
            # Stop producer
            if self.producer:
                await self.producer.stop()
                logger.info("Stopped Kafka producer")
            
            self._connected = False
            logger.info("Disconnected from Kafka")
            
        except Exception as e:
            logger.error("Error during Kafka disconnect", error=str(e))
    
    async def ping(self) -> bool:
        """Ping Kafka to check connectivity."""
        if not self._connected or not self.producer:
            return False
        
        try:
            # Try to get metadata as a connectivity check
            metadata = await self.producer.client.fetch_metadata()
            return len(metadata.brokers) > 0
        except Exception as e:
            logger.error("Kafka ping failed", error=str(e))
            return False
    
    async def produce_message(
        self,
        topic: str,
        message: Dict[str, Any],
        key: Optional[str] = None,
        partition: Optional[int] = None
    ) -> None:
        """Produce a message to a Kafka topic."""
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")
        
        try:
            # Add timestamp to message
            message_with_timestamp = {
                **message,
                "timestamp": datetime.utcnow().isoformat(),
                "producer_id": self.config.client_id
            }
            
            # Send message
            await self.producer.send(
                topic=topic,
                value=message_with_timestamp,
                key=key,
                partition=partition
            )
            
            logger.debug(
                "Message produced",
                topic=topic,
                key=key,
                partition=partition,
                message_size=len(json.dumps(message_with_timestamp))
            )
            
        except Exception as e:
            logger.error(
                "Failed to produce message",
                topic=topic,
                key=key,
                error=str(e)
            )
            raise
    
    async def produce_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
        keys: Optional[List[str]] = None
    ) -> None:
        """Produce a batch of messages to a Kafka topic."""
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")
        
        try:
            batch = self.producer.create_batch()
            
            for i, message in enumerate(messages):
                key = keys[i] if keys and i < len(keys) else None
                
                # Add timestamp to message
                message_with_timestamp = {
                    **message,
                    "timestamp": datetime.utcnow().isoformat(),
                    "producer_id": self.config.client_id,
                    "batch_index": i
                }
                
                batch.append(
                    key=key.encode('utf-8') if key else None,
                    value=json.dumps(message_with_timestamp).encode('utf-8'),
                    timestamp=None
                )
            
            # Send batch
            await self.producer.send_batch(batch, topic)
            
            logger.info(
                "Batch produced",
                topic=topic,
                message_count=len(messages)
            )
            
        except Exception as e:
            logger.error(
                "Failed to produce batch",
                topic=topic,
                message_count=len(messages),
                error=str(e)
            )
            raise
    
    async def create_consumer(
        self,
        topics: List[str],
        consumer_id: str,
        group_id: Optional[str] = None
    ) -> AIOKafkaConsumer:
        """Create a Kafka consumer."""
        try:
            consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=self.config.bootstrap_servers,
                client_id=f"{self.config.client_id}-{consumer_id}",
                group_id=group_id or self.config.consumer_group_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                auto_commit_interval_ms=self.config.auto_commit_interval_ms,
                session_timeout_ms=self.config.session_timeout_ms,
                max_poll_records=self.config.max_poll_records,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                key_deserializer=lambda k: k.decode('utf-8') if k else None
            )
            
            await consumer.start()
            self.consumers[consumer_id] = consumer
            
            logger.info(
                "Kafka consumer created",
                consumer_id=consumer_id,
                topics=topics,
                group_id=group_id or self.config.consumer_group_id
            )
            
            return consumer
            
        except Exception as e:
            logger.error(
                "Failed to create Kafka consumer",
                consumer_id=consumer_id,
                topics=topics,
                error=str(e)
            )
            raise
    
    async def consume_messages(
        self,
        consumer_id: str,
        message_handler: Callable[[Dict[str, Any]], None],
        max_messages: Optional[int] = None
    ) -> None:
        """Consume messages from Kafka topics."""
        if consumer_id not in self.consumers:
            raise ValueError(f"Consumer {consumer_id} not found")
        
        consumer = self.consumers[consumer_id]
        message_count = 0
        
        try:
            async for message in consumer:
                try:
                    # Process message
                    result = message_handler(message.value)
                    if result is not None:
                        await result
                    
                    message_count += 1
                    
                    logger.debug(
                        "Message consumed",
                        consumer_id=consumer_id,
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        key=message.key
                    )
                    
                    # Check if we've reached the maximum number of messages
                    if max_messages and message_count >= max_messages:
                        break
                        
                except Exception as e:
                    logger.error(
                        "Error processing message",
                        consumer_id=consumer_id,
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        error=str(e)
                    )
                    # Continue processing other messages
                    continue
            
            logger.info(
                "Message consumption completed",
                consumer_id=consumer_id,
                messages_processed=message_count
            )
            
        except Exception as e:
            logger.error(
                "Error during message consumption",
                consumer_id=consumer_id,
                error=str(e)
            )
            raise
    
    async def stop_consumer(self, consumer_id: str) -> None:
        """Stop a specific consumer."""
        if consumer_id in self.consumers:
            consumer = self.consumers[consumer_id]
            await consumer.stop()
            del self.consumers[consumer_id]
            
            logger.info("Kafka consumer stopped", consumer_id=consumer_id)
    
    async def get_topic_metadata(self, topic: str) -> Dict[str, Any]:
        """Get metadata for a specific topic."""
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")
        
        try:
            metadata = await self.producer.client.fetch_metadata([topic])
            
            topic_metadata = metadata.topics.get(topic)
            if not topic_metadata:
                raise ValueError(f"Topic {topic} not found")
            
            return {
                "topic": topic,
                "partitions": len(topic_metadata.partitions),
                "partition_info": [
                    {
                        "partition": partition_id,
                        "leader": partition_info.leader,
                        "replicas": partition_info.replicas,
                        "isr": partition_info.isr
                    }
                    for partition_id, partition_info in topic_metadata.partitions.items()
                ]
            }
            
        except Exception as e:
            logger.error("Failed to get topic metadata", topic=topic, error=str(e))
            raise
    
    async def list_topics(self) -> List[str]:
        """List all available topics."""
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")
        
        try:
            metadata = await self.producer.client.fetch_metadata()
            topics = list(metadata.topics.keys())
            
            logger.debug("Listed Kafka topics", topic_count=len(topics))
            
            return topics
            
        except Exception as e:
            logger.error("Failed to list topics", error=str(e))
            raise