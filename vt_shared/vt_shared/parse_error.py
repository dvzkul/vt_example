from dataclasses import dataclass, field
import json
from typing import Any, Dict, Optional

@dataclass
class ParseError:
    error: str
    source: str
    file_name: str
    row_data: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps({
            "error": self.error,
            "source": self.source,
            "file_name": self.file_name,
            "row_data": self.row_data
        })

    @staticmethod
    def from_json(data: str) -> 'ParseError':
        obj = json.loads(data)
        return ParseError(
            error=obj.get("error", "Unknown Error"),
            source=obj.get("source", "Unknown Source"),
            file_name=obj.get("file_name", "Unknown File"),
            row_data=obj.get("row_data", {})
        )