import os
import json
import requests
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv
import pandas as pd

from tk_slack.interaction_builder import ActionHandler, SnowflakeConnector
from tk_slack.message_templates import MessageTemplate

# Load environment variables
load_dotenv()

# Initialize the Slack app
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# Initialize Snowflake connector
snowflake = SnowflakeConnector()

# Set up the action handler - this registers a universal handler for all interactive elements
handler = ActionHandler.setup_slack_action_handler(app, snowflake)

# Example event handlers
@app.event("app_mention")
def handle_app_mention_events(body, logger):
    logger.info(body)

@app.event("message")
def handle_message_events(body, logger):
    logger.info(body)

# Custom send_to_slack function that supports metadata
def send_to_slack(message_payload, message_id):
    """Send a message to Slack with metadata support.
    
    Args:
        message_payload: Complete message payload with metadata
        message_id: ID for logging/tracking
        
    Returns:
        Tuple of (success, error_details)
    """
    try:
        # Use the Slack Web API to send the message
        from slack_sdk import WebClient
        
        # Create a client with the bot token
        client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
        
        # Send the message
        response = client.chat_postMessage(**message_payload)
        
        return True, None
    except Exception as e:
        return False, str(e)

def log_history(*args, **kwargs):
    """Log history of messages sent."""
    # This is a placeholder for logging functionality
    # In a real implementation, you would log to a database or file
    print(f"Logging message history: {args}, {kwargs}")

# Example of how to send a message with the template system
def send_data_alert(df, view, view_group, message_text, channel_id, config=None):
    """Send a data alert using the template system.
    
    Args:
        df: DataFrame with alert data
        view: View name
        view_group: View group name
        message_text: Message text
        channel_id: Channel ID to send to
        config: Optional configuration
    """
    # Default config if none provided
    if not config:
        config = {
            "response_type": "ephemeral",
            "response_message": "Thank you for your response! We'll process your selection: {value}",
            "replace_original": False
        }
    
    # Send using template
    MessageTemplate.template_f2(
        df=df,
        view=view,
        view_group=view_group,
        message_text=message_text,
        channel_id=channel_id,
        view_config=config,
        send_to_slack_func=send_to_slack,
        log_alert_history_func=log_history
    )
    
    print(f"Sent alert for '{view}' to channel {channel_id}")

# Example usage
def generate_example_alert():
    """Generate an example alert to demonstrate the system."""
    # Sample data
    data = {
        'name': ['Project X Kickoff', 'Client Y Renewal', 'Feature Z Implementation'],
        'text': [
            'Initial kickoff meeting for Project X',
            'Renewal discussion with Client Y',
            'Planning for Feature Z implementation'
        ],
        'status': ['Pending', 'Scheduled', 'In Progress'],
        'priority': ['High', 'Medium', 'High'],
        'due_date': ['2025-06-01', '2025-05-25', '2025-06-15'],
        'option_name': [
            ['Schedule Meeting', 'Postpone', 'Cancel'],
            ['Accept', 'Negotiate', 'Decline'],
            ['Start Work', 'Assign to Me', 'Request More Info']
        ],
        'option_value': [
            ['schedule', 'postpone', 'cancel'],
            ['accept', 'negotiate', 'decline'],
            ['start', 'assign_self', 'request_info']
        ]
    }
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Configuration
    config = {
        'response_type': 'ephemeral',
        'response_message': 'Thanks {user}! You selected "{text}" ({value})',
        'replace_original': False
    }
    
    # Send the alert to the specified channel
    send_data_alert(
        df=df,
        view='example_view',
        view_group='examples',
        message_text='Example Data Alert',
        channel_id=os.environ.get("SLACK_TEST_CHANNEL", "general"),
        config=config
    )

# Start the app when run directly
if __name__ == "__main__":
    # Generate an example alert if requested
    if os.environ.get("GENERATE_EXAMPLE", "false").lower() == "true":
        print("Generating example alert...")
        generate_example_alert()
        print("Example alert generated.")
    
    # Start the app
    handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
    handler.start()