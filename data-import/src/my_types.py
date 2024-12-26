from typing import TypedDict


class Row(TypedDict):
    video_id: str
    author: str
    text: str
    likes: int
