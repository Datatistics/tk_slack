"""Fill in a module description here"""

# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/API/05_message_templates.ipynb.

# %% auto 0
__all__ = ['MessageTemplate']

# %% ../nbs/API/05_message_templates.ipynb 3
from fastcore.basics import patch_to
from fastcore.test import *
from .core import DebugLogger, SlackMessenger, ColumnUtils, SlackFormatter
from typing import List, Tuple, Dict, Any, Callable, Optional
from .block_builder import BlockBuilder
from .template_engine import TemplateEngine
import pandas as pd
import json

# %% ../nbs/API/05_message_templates.ipynb 4
class MessageTemplate:
    """Refactored templates using modular components."""
    pass

# %% ../nbs/API/05_message_templates.ipynb 5
@patch_to(MessageTemplate,cls_method=True)
def _send_messages_and_log(
        self,
        messages: List[Tuple[str, List[Dict[str, Any]], Dict[str, Any]]],
        view: str,
        view_group: str,
        channel_id: str,
        send_to_slack_func: Callable,
        log_alert_history_func: Callable
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Send multiple messages and log results.
        
        Args:
            messages: List of (message_text, blocks, row_data) tuples
            view: View name
            view_group: View group name
            channel_id: Slack channel ID
            send_to_slack_func: Function to send messages to Slack
            log_alert_history_func: Function to log alert history
            
        Returns:
            Tuple of (success_flag, error_details)
        """
        all_success = True
        all_errors = []
        
        for idx, (message_text, payload_blocks, row_data) in enumerate(messages):
            DebugLogger.log(f'Sending message for {view} - Item {idx}')
            DebugLogger.log(f'Payload Blocks: {json.dumps(payload_blocks, default=str)}')
            
            # Send to Slack with error handling
            success, error_details = SlackMessenger._send_alert_to_slack(
                send_to_slack_func, 
                channel_id, 
                message_text, 
                payload_blocks, 
                f"{view}_item_{idx}"
            )
            
            if not success:
                all_success = False
                all_errors.append({f"item_{idx}": error_details})
            
            # Format row data for logging
            formatted_data = SlackMessenger._format_data_for_logging(row_data)
            
            # Log alert history
            SlackMessenger._log_alert(
                log_alert_history_func,
                view=view,
                view_group=view_group,
                channel_id=channel_id,
                success=success,
                error_details=error_details,
                formatted_data=formatted_data,
                message_text=message_text
            )
        
        return all_success, all_errors if not all_success else None

# %% ../nbs/API/05_message_templates.ipynb 6
@patch_to(MessageTemplate,cls_method=True)
def _send_messages_and_log_with_metadata(
        self,
        messages: List[Tuple[Dict[str, Any], pd.DataFrame]],
        view: str,
        view_group: str,
        channel_id: str,
        send_to_slack_func: Callable,
        log_alert_history_func: Callable
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Send multiple messages with metadata and log results.
        
        Args:
            messages: List of (message_payload, row_data) tuples
            view: View name
            view_group: View group name
            channel_id: Slack channel ID
            send_to_slack_func: Function to send messages to Slack
            log_alert_history_func: Function to log alert history
            
        Returns:
            Tuple of (success_flag, error_details)
        """
        all_success = True
        all_errors = []
        
        for idx, (message_payload, row_data) in enumerate(messages):
            DebugLogger.log(f'Sending message for {view} - Item {idx}')
            
            # Extract text and blocks from payload
            message_text = message_payload.get("text", "")
            
            # Send to Slack with error handling
            try:
                # We need to use the Slack Web API client directly to support metadata
                success, error_details = send_to_slack_func(
                    message_payload,
                    f"{view}_item_{idx}"
                )
                
                if not success:
                    all_success = False
                    all_errors.append({f"item_{idx}": error_details})
                
                # Format row data for logging
                formatted_data = SlackMessenger._format_data_for_logging(row_data)
                
                # Log alert history
                SlackMessenger._log_alert(
                    log_alert_history_func,
                    view=view,
                    view_group=view_group,
                    channel_id=channel_id,
                    success=success,
                    error_details=error_details,
                    formatted_data=formatted_data,
                    message_text=message_text
                )
            except Exception as e:
                all_success = False
                error_msg = f"Error sending message: {str(e)}"
                all_errors.append({f"item_{idx}": error_msg})
                DebugLogger.log(error_msg)
        
        return all_success, all_errors if not all_errors else None

# %% ../nbs/API/05_message_templates.ipynb 7
@patch_to(MessageTemplate,cls_method=True)
def template_f1(
        self,
        df: pd.DataFrame,
        view: str,
        view_group: str,
        message_text: str,
        channel_id: str,
        view_config: Dict[str, Any],
        send_to_slack_func: Callable = None,
        log_alert_history_func: Callable = None,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Slack Message Format 1: Single message with row sections and details on the right.
        
        Args:
            df: DataFrame with alert data
            view: View name
            view_group: Group name for the view
            message_text: Main message text
            channel_id: Slack channel ID
            view_config: Configuration for the view
            send_to_slack_func: Function to send messages to Slack
            log_alert_history_func: Function to log alert history
            
        Returns:
            Tuple of (success_flag, error_details)
        """
        DebugLogger.log(f'Processing template_f1 for view: {view}')
        
        # Create title from view name
        title = view.lower().replace(view_group, '').replace('_', ' ').title()
        
        # Initialize blocks with header
        payload_blocks = [BlockBuilder.create_header_block(title)]
        
        # Get columns for details
        df_columns = list(df.columns)
        detail_columns = (
            view_config.get('detail_columns') 
            if 'detail_columns' in view_config 
            else ColumnUtils.get_detail_columns(df_columns)
        )
        
        DebugLogger.log(f'df_columns: {df_columns}')
        DebugLogger.log(f'detail_columns: {detail_columns}')
        
        # Process each row into a section
        for idx, row in df.iterrows():
            detail_text = SlackFormatter.right_hand_details(row, detail_columns, df)
            section_text = SlackFormatter.format_section_name(row, df_columns)
            payload_blocks.append(BlockBuilder.process_section_row(section_text, detail_text))
        
        DebugLogger.log(f'Payload Blocks: {json.dumps(payload_blocks, default=str)}')
        print(f'   Sending Alert for {view}')
        
        # Send to Slack with error handling
        success, error_details = SlackMessenger._send_alert_to_slack(
            send_to_slack_func, 
            channel_id, 
            message_text, 
            payload_blocks, 
            view
        )
        
        # Format data for logging
        formatted_data = SlackMessenger._format_data_for_logging(df)
        
        if log_alert_history_func:
            # Log alert history
            SlackMessenger._log_alert(
                log_alert_history_func,
                view=view,
                view_group=view_group,
                channel_id=channel_id,
                success=success,
                error_details=error_details,
                formatted_data=formatted_data,
                message_text=message_text
            )
        
        return success, error_details

# %% ../nbs/API/05_message_templates.ipynb 8
@patch_to(MessageTemplate,cls_method=True)
def template_f2(
    cls,
    df: pd.DataFrame,
    view: str,
    view_group: str,
    message_text: str,
    channel_id: str,
    view_config: Dict[str, Any] = None,
    send_to_slack_func: Callable = None,
    log_alert_history_func: Callable = None,
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """Slack Message Format 2: Individual interactive messages for each row.
    
    Args:
        df: DataFrame with alert data
        view: View name
        view_group: Group name for the view
        message_text: Main message text
        channel_id: Slack channel ID
        view_config: Configuration for the view
        send_to_slack_func: Function to send messages to Slack
        log_alert_history_func: Function to log alert history
        
    Returns:
        Tuple of (success_flag, error_details)
    """
    DebugLogger.log(f'Processing template_f2 for view: {view}')
    
    # Import MessageMetadataHandler here to avoid circular imports
    from tk_slack.metadata_handler import MessageMetadataHandler
    
    # Normalize column names for case-insensitive access
    df_columns = list(df.columns)
    col_map = ColumnUtils.normalize_columns(df_columns)
    
    # Prepare messages for each row
    messages = []
    
    # Process each row
    for idx, row in df.iterrows():
        # Get row-specific config or fallback to view_config
        config = TemplateEngine._parse_row_config(row, view_config, col_map)
        config['view'] = view
        config['view_group'] = view_group

        # Extract response configuration from config
        response_config = {
            "response_type": config.get("response_type", "ephemeral"),
            "response_message": config.get("response_message", "Thank you for your response!"),
            "replace_original": config.get("replace_original", False)
            }
        
        # Build message blocks for this row
        payload_blocks = TemplateEngine.build_individual_message_blocks(row, df_columns, col_map, config)
        
        # Message text can be customized per row or use the default
        row_message_col = col_map.get('MESSAGE_TEXT')
        row_message = row[row_message_col] if row_message_col and pd.notna(row[row_message_col]) else message_text
        
        # Create message payload
        message_payload = {
            "channel": channel_id,
            "text": row_message,
            "blocks": payload_blocks
        }
        
        # Add metadata to message
        custom_data = {"row_index": idx}
        message_with_metadata = MessageMetadataHandler.add_metadata_to_message(
            message_payload,
            event_type=f"{view}_notification",
            view_info=config,
            response_config=response_config,
            custom_data=custom_data
        )
        
        # Add to message list
        messages.append((message_with_metadata, pd.DataFrame([row])))
    
    # Send messages and log results
    return cls._send_messages_and_log_with_metadata(
        messages, view, view_group, channel_id, send_to_slack_func, log_alert_history_func
    )
