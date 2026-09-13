from dataclasses import dataclass
from datetime import datetime


@dataclass
class KnowledgeArticle:

    id: str

    title: str

    content: str

    tags: list[str]

    source: str

    is_active: bool

    created_at: datetime

    updated_at: datetime
