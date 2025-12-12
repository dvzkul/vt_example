from dataclasses import dataclass,field
import json

@dataclass
class MasterRecord:
    id: str
    first_name: str
    last_name: str
    ssn: str
    hourly_rate: float
    client_id: str
    source_file: str
    attributes: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps({
            "id": self.id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "ssn": self.ssn,
            "hourly_rate": self.hourly_rate,
            "client_id": self.client_id,
            "source_file": self.source_file,
            "attributes": self.attributes
        })

    @staticmethod
    def from_json(data: str) -> 'MasterRecord':
        obj = json.loads(data)
        return MasterRecord(
            id=obj["id"],
            first_name=obj["first_name"],
            last_name=obj["last_name"],
            ssn=obj["ssn"],
            hourly_rate=obj["hourly_rate"],
            client_id=obj["client_id"],
            source_file=obj["source_file"],
            attributes=obj.get("attributes", {})
    )
        