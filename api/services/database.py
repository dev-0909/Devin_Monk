"""Database service for MonkDB Data Platform."""

import structlog
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from typing import Dict, Any, List, Optional, Union
from bson import ObjectId

from api.config import MonkDBConfig

logger = structlog.get_logger(__name__)


class DatabaseService:
    """MongoDB database service."""
    
    def __init__(self, config: MonkDBConfig):
        """Initialize database service."""
        self.config = config
        self.client: Optional[AsyncIOMotorClient] = None
        self.database: Optional[AsyncIOMotorDatabase] = None
        self._connected = False
    
    async def connect(self) -> None:
        """Connect to MongoDB."""
        try:
            self.client = AsyncIOMotorClient(
                self.config.uri,
                maxPoolSize=self.config.connection_pool_size,
                maxIdleTimeMS=self.config.max_idle_time_ms,
                serverSelectionTimeoutMS=self.config.server_selection_timeout_ms
            )
            
            # Test connection
            await self.client.admin.command('ping')
            
            self.database = self.client[self.config.database]
            self._connected = True
            
            logger.info(
                "Connected to MongoDB",
                host=self.config.host,
                port=self.config.port,
                database=self.config.database
            )
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error("Failed to connect to MongoDB", error=str(e))
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from MongoDB."""
        if self.client:
            self.client.close()
            self._connected = False
            logger.info("Disconnected from MongoDB")
    
    async def ping(self) -> bool:
        """Ping MongoDB to check connectivity."""
        if not self._connected or not self.client:
            return False
        
        try:
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.error("MongoDB ping failed", error=str(e))
            return False
    
    def get_collection(self, collection_name: str) -> AsyncIOMotorCollection:
        """Get a collection from the database."""
        if not self.database:
            raise RuntimeError("Database not connected")
        return self.database[collection_name]
    
    async def insert_data(self, collection_name: str, data: Dict[str, Any]) -> Any:
        """Insert a document into a collection."""
        try:
            collection = self.get_collection(collection_name)
            result = await collection.insert_one(data)
            
            logger.debug(
                "Document inserted",
                collection=collection_name,
                document_id=str(result.inserted_id)
            )
            
            return result
        
        except Exception as e:
            logger.error(
                "Failed to insert document",
                collection=collection_name,
                error=str(e)
            )
            raise
    
    async def insert_many_data(self, collection_name: str, data: List[Dict[str, Any]]) -> Any:
        """Insert multiple documents into a collection."""
        try:
            collection = self.get_collection(collection_name)
            result = await collection.insert_many(data)
            
            logger.debug(
                "Documents inserted",
                collection=collection_name,
                count=len(result.inserted_ids)
            )
            
            return result
        
        except Exception as e:
            logger.error(
                "Failed to insert documents",
                collection=collection_name,
                error=str(e)
            )
            raise
    
    async def find_data(
        self,
        collection_name: str,
        query: Dict[str, Any],
        skip: int = 0,
        limit: Optional[int] = None,
        sort: Optional[List[tuple]] = None
    ) -> List[Dict[str, Any]]:
        """Find documents in a collection."""
        try:
            collection = self.get_collection(collection_name)
            cursor = collection.find(query)
            
            if skip > 0:
                cursor = cursor.skip(skip)
            
            if limit:
                cursor = cursor.limit(limit)
            
            if sort:
                cursor = cursor.sort(sort)
            
            results = await cursor.to_list(length=None)
            
            # Convert ObjectId to string for JSON serialization
            for result in results:
                if '_id' in result:
                    result['_id'] = str(result['_id'])
            
            logger.debug(
                "Documents found",
                collection=collection_name,
                count=len(results),
                skip=skip,
                limit=limit
            )
            
            return results
        
        except Exception as e:
            logger.error(
                "Failed to find documents",
                collection=collection_name,
                error=str(e)
            )
            raise
    
    async def find_one_data(
        self,
        collection_name: str,
        query: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Find a single document in a collection."""
        try:
            collection = self.get_collection(collection_name)
            result = await collection.find_one(query)
            
            if result and '_id' in result:
                result['_id'] = str(result['_id'])
            
            logger.debug(
                "Document found",
                collection=collection_name,
                found=result is not None
            )
            
            return result
        
        except Exception as e:
            logger.error(
                "Failed to find document",
                collection=collection_name,
                error=str(e)
            )
            raise
    
    async def update_data(
        self,
        collection_name: str,
        query: Dict[str, Any],
        update: Dict[str, Any]
    ) -> Any:
        """Update documents in a collection."""
        try:
            collection = self.get_collection(collection_name)
            
            # Convert string ID to ObjectId if needed
            if '_id' in query and isinstance(query['_id'], str):
                query['_id'] = ObjectId(query['_id'])
            
            result = await collection.update_many(query, update)
            
            logger.debug(
                "Documents updated",
                collection=collection_name,
                matched=result.matched_count,
                modified=result.modified_count
            )
            
            return result
        
        except Exception as e:
            logger.error(
                "Failed to update documents",
                collection=collection_name,
                error=str(e)
            )
            raise
    
    async def delete_data(
        self,
        collection_name: str,
        query: Dict[str, Any]
    ) -> Any:
        """Delete documents from a collection."""
        try:
            collection = self.get_collection(collection_name)
            
            # Convert string ID to ObjectId if needed
            if '_id' in query and isinstance(query['_id'], str):
                query['_id'] = ObjectId(query['_id'])
            
            result = await collection.delete_many(query)
            
            logger.debug(
                "Documents deleted",
                collection=collection_name,
                deleted=result.deleted_count
            )
            
            return result
        
        except Exception as e:
            logger.error(
                "Failed to delete documents",
                collection=collection_name,
                error=str(e)
            )
            raise
    
    async def count_data(
        self,
        collection_name: str,
        query: Dict[str, Any]
    ) -> int:
        """Count documents in a collection."""
        try:
            collection = self.get_collection(collection_name)
            count = await collection.count_documents(query)
            
            logger.debug(
                "Documents counted",
                collection=collection_name,
                count=count
            )
            
            return count
        
        except Exception as e:
            logger.error(
                "Failed to count documents",
                collection=collection_name,
                error=str(e)
            )
            raise
    
    async def create_index(
        self,
        collection_name: str,
        keys: Union[str, List[tuple]],
        **kwargs
    ) -> str:
        """Create an index on a collection."""
        try:
            collection = self.get_collection(collection_name)
            index_name = await collection.create_index(keys, **kwargs)
            
            logger.info(
                "Index created",
                collection=collection_name,
                index_name=index_name
            )
            
            return index_name
        
        except Exception as e:
            logger.error(
                "Failed to create index",
                collection=collection_name,
                error=str(e)
            )
            raise
    
    async def aggregate_data(
        self,
        collection_name: str,
        pipeline: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Perform aggregation on a collection."""
        try:
            collection = self.get_collection(collection_name)
            cursor = collection.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
            # Convert ObjectId to string for JSON serialization
            for result in results:
                if '_id' in result and isinstance(result['_id'], ObjectId):
                    result['_id'] = str(result['_id'])
            
            logger.debug(
                "Aggregation completed",
                collection=collection_name,
                pipeline_stages=len(pipeline),
                results_count=len(results)
            )
            
            return results
        
        except Exception as e:
            logger.error(
                "Failed to perform aggregation",
                collection=collection_name,
                error=str(e)
            )
            raise