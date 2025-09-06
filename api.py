from flask import Flask, request, jsonify
from datetime import datetime
import csv
import json
import os
from typing import List, Dict, Any

app = Flask(__name__)

# In-memory storage for events (you can replace this with a database later)
events_data = []

# File to persist data
DATA_FILE = "events_data.json"

def load_data():
    """Load events from file on startup"""
    global events_data
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                events_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            events_data = []

def save_data():
    """Save events to file"""
    with open(DATA_FILE, 'w') as f:
        json.dump(events_data, f, indent=2)

def parse_csv_line(line: str) -> Dict[str, Any]:
    """Parse a single line of CSV data"""
    parts = line.strip().split()
    if len(parts) >= 3:
        return {
            "user_id": parts[0],
            "event_type": parts[1],
            "event_timestamp": parts[2],
            "added_at": datetime.now().isoformat()
        }
    return None

# Load existing data on startup
load_data()

@app.route('/')
def home():
    """API documentation"""
    return jsonify({
        "message": "User Events API",
        "endpoints": {
            "GET /events": "Get all events",
            "GET /events/<user_id>": "Get events for specific user",
            "POST /events": "Add new event(s)",
            "GET /users": "Get all unique users",
            "GET /event-types": "Get all event types",
            "DELETE /events": "Clear all events (admin only)"
        },
        "total_events": len(events_data)
    })

@app.route('/events', methods=['GET'])
def get_all_events():
    """Get all events with optional filtering"""
    user_id = request.args.get('user_id')
    event_type = request.args.get('event_type')
    
    filtered_events = events_data
    
    if user_id:
        filtered_events = [e for e in filtered_events if e['user_id'] == user_id]
    
    if event_type:
        filtered_events = [e for e in filtered_events if e['event_type'] == event_type]
    
    return jsonify({
        "events": filtered_events,
        "count": len(filtered_events),
        "total_events": len(events_data)
    })

@app.route('/events/<user_id>', methods=['GET'])
def get_user_events(user_id):
    """Get all events for a specific user"""
    user_events = [e for e in events_data if e['user_id'] == user_id]
    return jsonify({
        "user_id": user_id,
        "events": user_events,
        "count": len(user_events)
    })

@app.route('/events', methods=['POST'])
def add_events():
    """Add new event(s) to the system"""
    try:
        content_type = request.content_type
        
        if content_type == 'application/json':
            # Handle JSON input
            data = request.get_json()
            
            if isinstance(data, list):
                # Multiple events
                new_events = []
                for event in data:
                    if all(key in event for key in ['user_id', 'event_type', 'event_timestamp']):
                        event['added_at'] = datetime.now().isoformat()
                        new_events.append(event)
                events_data.extend(new_events)
            else:
                # Single event
                if all(key in data for key in ['user_id', 'event_type', 'event_timestamp']):
                    data['added_at'] = datetime.now().isoformat()
                    events_data.append(data)
                    new_events = [data]
                else:
                    return jsonify({"error": "Missing required fields"}), 400
        
        elif content_type == 'text/plain':
            # Handle CSV-like text input
            text_data = request.get_data(as_text=True)
            lines = text_data.strip().split('\n')
            
            new_events = []
            for line in lines:
                if line.strip():
                    event = parse_csv_line(line)
                    if event:
                        events_data.append(event)
                        new_events.append(event)
        
        else:
            return jsonify({"error": "Unsupported content type"}), 400
        
        # Save to file
        save_data()
        
        return jsonify({
            "message": f"Added {len(new_events)} event(s)",
            "events_added": new_events,
            "total_events": len(events_data)
        }), 201
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/users', methods=['GET'])
def get_users():
    """Get all unique users"""
    users = list(set(event['user_id'] for event in events_data))
    user_stats = {}
    
    for user in users:
        user_events = [e for e in events_data if e['user_id'] == user]
        user_stats[user] = {
            "event_count": len(user_events),
            "event_types": list(set(e['event_type'] for e in user_events))
        }
    
    return jsonify({
        "users": users,
        "count": len(users),
        "user_stats": user_stats
    })

@app.route('/event-types', methods=['GET'])
def get_event_types():
    """Get all unique event types"""
    event_types = list(set(event['event_type'] for event in events_data))
    
    # Get count for each event type
    type_counts = {}
    for event_type in event_types:
        type_counts[event_type] = len([e for e in events_data if e['event_type'] == event_type])
    
    return jsonify({
        "event_types": event_types,
        "count": len(event_types),
        "type_counts": type_counts
    })

@app.route('/events', methods=['DELETE'])
def clear_events():
    """Clear all events (admin function)"""
    global events_data
    events_data = []
    save_data()
    return jsonify({"message": "All events cleared", "total_events": 0})

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get overall statistics"""
    if not events_data:
        return jsonify({
            "total_events": 0,
            "unique_users": 0,
            "event_types": []
        })
    
    unique_users = len(set(event['user_id'] for event in events_data))
    event_types = list(set(event['event_type'] for event in events_data))
    
    # Most active user
    user_counts = {}
    for event in events_data:
        user_id = event['user_id']
        user_counts[user_id] = user_counts.get(user_id, 0) + 1
    
    most_active_user = max(user_counts.items(), key=lambda x: x[1]) if user_counts else None
    
    return jsonify({
        "total_events": len(events_data),
        "unique_users": unique_users,
        "event_types": event_types,
        "most_active_user": {
            "user_id": most_active_user[0],
            "event_count": most_active_user[1]
        } if most_active_user else None
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)