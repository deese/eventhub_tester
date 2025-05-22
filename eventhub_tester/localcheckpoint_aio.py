import os
import json
import logging
import traceback
from typing import Iterable, Dict, Any, Union, Optional
from datetime import datetime
from azure.eventhub.aio import CheckpointStore
from azure.eventhub.exceptions import EventHubError

import aiofiles

logger = logging.getLogger(__name__)
DEBUG = False

class LocalFileCheckpointStore(CheckpointStore):
    """Local file checkpoint store for Azure Event Hub.
    This class implements the CheckpointStore interface to manage
    checkpointing and ownership information in a local file."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.checkpoints = self._load_checkpoints()
        self.ownerships = self._load_ownership()

    def _load_checkpoints(self):
        """Load checkpoints from the local file."""

        try:
            if os.path.exists(self.file_path):
                with open(self.file_path, 'r', encoding="utf-8") as f:
                    return json.load(f).get('checkpoints', [])
        except Exception as e:
            logger.exception(e)
        return []

    def _load_ownership(self):
        """Load ownership from the local file."""
        try:
            if os.path.exists(self.file_path):
                return json.load(open(self.file_path, 'r',  encoding="utf-8")).get('ownership', [])
        except Exception as e:
            logger.exception(e)
        return  []

    async def _save(self):
        """Save both checkpoints and ownership to the local file."""
        data = {
            'checkpoints': self.checkpoints,
            'ownership': self.ownerships
        }
        async with aiofiles.open(self.file_path, 'w') as f:
            await f.write(json.dumps(data))


    async def __aenter__(self) -> "LocalFileCheckpointStore":
        """Open the checkpoint store."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Close the checkpoint store."""
        self._save()

    def get_ownership(self, ownership):
        """ Check if ownership exists ."""
        for _c in self.ownerships:
            if _c is None:
                return None
            if all(_c[v] == ownership[v] for v in ['fully_qualified_namespace',  'eventhub_name', 'consumer_group', 'partition_id']):
                return _c
        return None


    async def claim_ownership(self, ownership_list: Iterable[Dict[str, Any]], **kwargs: Any) -> Iterable[Dict[str, Any]]:
        """Claim ownership of a partition."""
        if len(self.ownerships) == 0:
            self.ownerships = ownership_list
            for i in self.ownerships:
                i['last_modified_time'] = datetime.now().timestamp()
            return self.ownerships

        for _owner in ownership_list:
            _oship = self.get_ownership(_owner)
            if _oship:
                _oship = _owner
                _oship['last_modified_time'] = datetime.now().timestamp()
            else:
                _owner['last_modified_time'] = datetime.now().timestamp()
                self.ownerships.append(_owner)
        await self._save()
        return self.ownerships

    def get_checkpoint(self, checkpoint):
        """ Check if checkpoint exists ."""
        for _c in self.checkpoints:
            if _c is None:
                return None
            if all(_c[v] == checkpoint[v] for v in ['fully_qualified_namespace',  'eventhub_name', 'consumer_group', 'partition_id' ]):
                return _c
        return None

    async def update_checkpoint(self, checkpoint: Dict[str, Optional[Union[str, int]]], **kwargs: Any) -> None:
        """Update the checkpoint for a given partition."""
        try:
            event_hub = self.get_checkpoint(checkpoint)
            if event_hub:
                event_hub['sequence_number'] = checkpoint['sequence_number']
                event_hub['offset'] = checkpoint['offset']
            else:
                self.checkpoints.append(checkpoint)

            await self._save()
        except Exception as e:
            logger.exception(e)

    async def list_checkpoints(self, fully_qualified_namespace: str, eventhub_name: str, consumer_group: str, **kwargs: Any) -> Iterable[Dict[str, Any]]:
        """List all checkpoints for a given Event Hub and consumer group."""
        try:
            tmp = { 'fully_qualified_namespace': fully_qualified_namespace, 'eventhub_name': eventhub_name, 'consumer_group': consumer_group}
            return [ _c for _c in self.checkpoints if all(_c[v] == tmp[v] for v in ['fully_qualified_namespace', 'eventhub_name', 'consumer_group'])]
        except Exception as e:
            logger.exception(e)

    async def list_ownership(
        self, fully_qualified_namespace: str, eventhub_name: str, consumer_group: str, **kwargs: Any
    ) -> Iterable[Dict[str, Any]]:
        """List all ownership information."""
        ## Reviewed.
        try:
            return self.ownerships
        except:
            logger.exception(e)

    async def close (self) -> None:
        """  Close the checkpoint store."""
        return await self.__aexit__()
