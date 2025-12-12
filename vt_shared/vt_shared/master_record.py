from dataclasses import dataclass,field
import json

@dataclass
class MasterRecord:
    id: str
    first_name: str
    last_name: str
    ssn: str
    hourly_rate: float
    attributes: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps({
            "id": self.id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "ssn": self.ssn,
            "hourly_rate": self.hourly_rate,
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
            attributes=obj.get("attributes", {})
    )
        