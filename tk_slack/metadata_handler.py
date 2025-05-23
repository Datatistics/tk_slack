# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/API/06_metadata_handler.ipynb.

# %% auto 0
__all__ = ['MessageMetadataHandler']

# %% ../nbs/API/06_metadata_handler.ipynb 3
from fastcore.basics import patch_to
from fastcore.test import *
import json
from typing import Dict, Any, Optional, List

# %% ../nbs/API/06_metadata_handler.ipynb 5
class MessageMetadataHandler:
    """
    Handles the creation and attachment of metadata to Slack messages.
    
    This class implements Slack's message metadata feature which allows
    attaching invisible structured data to messages for event-driven processing.
    
    Reference: https://api.slack.com/metadata
    """
    pass

# %% ../nbs/API/06_metadata_handler.ipynb 6
@patch_to(MessageMetadataHandler,cls_method=True)
def create_metadata(
        self,
        event_type: str,
        view_info: Optional[Dict[str, Any]] = None,
        response_config: Optional[Dict[str, Any]] = None,
        custom_data: Optional[Dict[str, Any]] = None
        ) -> Dict[str, Any]:
    """Create structured metadata for a Slack message.
    
    Args:
        event_type: Type of event (e.g., 'lead_alert', 'data_notification')
        view_info: Optional information about the Snowflake view that generated this message
        response_config: Optional configuration for how to respond to user actions
        custom_data: Optional additional custom data to include
        
    Returns:
        Properly formatted metadata dictionary
    """
    # Create the event payload
    event_payload = {}
    
    # Add view information if provided
    if view_info: event_payload.update(view_info)
        
    # Add response configuration if provided
    if response_config:
        event_payload.update({
            "response_type": response_config.get("response_type", "ephemeral"),
            "response_message": response_config.get("response_message", "Your response has been received!"),
            "replace_original": response_config.get("replace_original", False)
        })
        
    # Add any custom data provided
    if custom_data:
        # Add with a custom_data prefix to avoid conflicts
        event_payload.update({
            f"custom_{k}": v for k, v in custom_data.items()
        })
        
    # Create the metadata object
    metadata = {
        "event_type": event_type,
        "event_payload": event_payload
    }
    
    return metadata
    

# %% ../nbs/API/06_metadata_handler.ipynb 7
@patch_to(MessageMetadataHandler,cls_method=True)
def add_metadata_to_message(
    self,
    message: Dict[str, Any],
    event_type: str,
    view_info: Optional[Dict[str, Any]] = None,
    response_config: Optional[Dict[str, Any]] = None,
    custom_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
    """Add metadata to a Slack message payload.
    
    Args:
        message: Slack message payload
        event_type: Type of event
        view_info: Optional view information
        response_config: Optional response configuration
        custom_data: Optional additional custom data
        
    Returns:
        Message with metadata attached
    """
    # Create metadata
    metadata = MessageMetadataHandler.create_metadata(
        event_type, view_info, response_config, custom_data
    )
    
    # Add to message
    message_with_metadata = message.copy()
    message_with_metadata["metadata"] = metadata
    
    return message_with_metadata


# %% ../nbs/API/06_metadata_handler.ipynb 8
@patch_to(MessageMetadataHandler,cls_method=True)
def extract_metadata_from_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
    """Extract metadata from a Slack message.
    
    Args:
        message: Slack message with metadata
        
    Returns:
        Extracted metadata or empty dict if none
    """
    if not message or "metadata" not in message:
        return {}
        
    return message["metadata"]

# %% ../nbs/API/06_metadata_handler.ipynb 9
@patch_to(MessageMetadataHandler,cls_method=True)
def get_event_payload(self, message: Dict[str, Any]) -> Dict[str, Any]:
    """Get the event payload from a message's metadata.
    
    Args:
        message: Slack message with metadata
        
    Returns:
        Event payload or empty dict if none
    """
    metadata = MessageMetadataHandler.extract_metadata_from_message(message)
    if not metadata or "event_payload" not in metadata:
        return {}
        
    return metadata["event_payload"]
