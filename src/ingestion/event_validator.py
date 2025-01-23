# src/ingestion/event_validator.py
import json
import jsonschema
from typing import Dict, Optional


class EventValidator:
    def __init__(self, schema_path: str):
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)

        # Pre-compile schema for better performance
        self.validator = jsonschema.validators.validator_for(self.schema)(self.schema)

    def validate_event(self, event: Dict) -> Optional[str]:
        """
        Validates an event against the schema.
        Returns None if valid, error message if invalid.
        """
        try:
            self.validator.validate(event)
            return None
        except jsonschema.exceptions.ValidationError as e:
            return f"Validation error: {str(e)}"
        except Exception as e:
            return f"Unexpected error during validation: {str(e)}"

    def is_valid(self, event: Dict) -> bool:
        """
        Returns True if event is valid, False otherwise.
        """
        return self.validate_event(event) is None

    def get_schema_requirements(self) -> Dict:
        """
        Returns a dict of required fields and their types
        """
        return {
            "required_fields": self.schema.get("required", []),
            "properties": self.schema.get("properties", {})
        }

    def validate_batch(self, events: list[Dict]) -> tuple[list[Dict], list[tuple[Dict, str]]]:
        """
        Validates a batch of events.
        Returns tuple of (valid_events, invalid_events_with_errors)
        """
        valid_events = []
        invalid_events = []

        for event in events:
            error = self.validate_event(event)
            if error is None:
                valid_events.append(event)
            else:
                invalid_events.append((event, error))

        return valid_events, invalid_events
