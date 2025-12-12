from dataclasses import dataclass, field
import json
from typing import List
from .master_record import MasterRecord

@dataclass
class ValidationError(MasterRecord):
    errors: List[str] = field(default_factory=list)

    def to_json(self) -> str:
        # Get the base dictionary from the parent class logic
        # (We could also use dataclasses.asdict, but this keeps custom logic if you add it later)
        data = {
            "id": self.id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "ssn": self.ssn,
            "hourly_rate": self.hourly_rate,
            "client_id": self.client_id,
            "source_file": self.source_file,
            "attributes": self.attributes,
            "errors": self.errors
        }
        return json.dumps(data)

    @staticmethod
    def from_json(data: str) -> 'ValidationError':
        obj = json.loads(data)
        return ValidationError(
            id=obj.get("id"),
            first_name=obj.get("first_name"),
            last_name=obj.get("last_name"),
            ssn=obj.get("ssn"),
            hourly_rate=obj.get("hourly_rate"),
            client_id=obj["client_id"],
            source_file=obj["source_file"],
            attributes=obj.get("attributes", {}),
            errors=obj.get("errors", [])
        )