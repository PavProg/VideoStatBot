import json
import datetime
from typing import Optional, Annotated
from src.db.database import Base
from sqlalchemy import ForeignKey, Integer, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship

intpk = Annotated[int, mapped_column(primary_key=True)]

class VideosOrm(Base):
    __tablename__ = 'videos'

    id: Mapped[intpk]
    video_id: Mapped[str] = mapped_column(unique=True)
    creator_id: Mapped[str] = mapped_column(unique=True)
    video_creator_at: Mapped[Optional[datetime.datetime]]
    views_count: Mapped[Optional[int]] = mapped_column(Integer)
    likes_count: Mapped[Optional[int]] = mapped_column(Integer)
    comments_count: Mapped[Optional[int]] = mapped_column(Integer)
    reports_count: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)

    snapshots: Mapped[list["SnapshotsOrm"]] = relationship()

class SnapshotsOrm(Base):
    __tablename__ = 'snapshots'

    id: Mapped[intpk]
    snapshot_id: Mapped[str] = mapped_column(unique=True)
    video_id: Mapped[str] = mapped_column(ForeignKey('videos.video_id'))
    views_count: Mapped[Optional[int]] = mapped_column(Integer)
    likes_count: Mapped[Optional[int]] = mapped_column(Integer)
    comments_count: Mapped[Optional[int]] = mapped_column(Integer)
    reports_count: Mapped[Optional[int]] = mapped_column(Integer)
    delta_views_count: Mapped[Optional[int]] = mapped_column(Integer)
    delta_likes_count: Mapped[Optional[int]] = mapped_column(Integer)
    delta_reports_count: Mapped[Optional[int]] = mapped_column(Integer)
    delta_comments_count: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)

    videos: Mapped["VideosOrm"] = relationship()


# with open('videos.json', 'r', encoding='utf-8') as f:
#     videos = json.load(f)

