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
    creator_id: Mapped[str] = mapped_column()
    video_created_at: Mapped[Optional[datetime.datetime]]
    views_count: Mapped[Optional[int]] = mapped_column(Integer)
    likes_count: Mapped[Optional[int]] = mapped_column(Integer)
    comments_count: Mapped[Optional[int]] = mapped_column(Integer)
    reports_count: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    snapshots: Mapped[list["SnapshotsOrm"]] = relationship(
        back_populates="videos",
        order_by="desc(SnapshotsOrm.created_at)",
    )

    def __repr__(self):
        return f"<Video(video_id='{self.video_id}', creator='{self.creator_id}')>"

    def to_dict(self) -> dict:
        """Преобразование в словарь для LLM"""
        return {
            "video_id": self.video_id,
            "creator_id": self.creator_id,
            "video_created_at": self.video_created_at.isoformat() if self.video_created_at else None,
            "views_count": self.views_count or 0,
            "likes_count": self.likes_count or 0,
            "comments_count": self.comments_count or 0,
            "reports_count": self.reports_count or 0,
            "snapshots": [s.to_dict() for s in self.snapshots]
        }

class SnapshotsOrm(Base):
    __tablename__ = 'snapshots'

    id: Mapped[intpk]
    snapshot_id: Mapped[str] = mapped_column(unique=True)
    video_id: Mapped[str] = mapped_column(ForeignKey('videos.video_id', ondelete="CASCADE"), nullable=False)
    views_count: Mapped[Optional[int]] = mapped_column(Integer)
    likes_count: Mapped[Optional[int]] = mapped_column(Integer)
    comments_count: Mapped[Optional[int]] = mapped_column(Integer)
    reports_count: Mapped[Optional[int]] = mapped_column(Integer)
    delta_views_count: Mapped[Optional[int]] = mapped_column(Integer)
    delta_likes_count: Mapped[Optional[int]] = mapped_column(Integer)
    delta_reports_count: Mapped[Optional[int]] = mapped_column(Integer)
    delta_comments_count: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    videos: Mapped["VideosOrm"] = relationship(
        back_populates="snapshots",
    )

    def __repr__(self):
        return f"<Snapshot(snapshot_id='{self.snapshot_id}', video='{self.video_id}')>"

    def to_dict(self) -> dict:
        """Преобразование в словарь для LLM"""
        return {
            "snapshot_id": self.snapshot_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "views_count": self.views_count or 0,
            "likes_count": self.likes_count or 0,
            "comments_count": self.comments_count or 0,
            "reports_count": self.reports_count or 0,
            "delta_views_count": self.delta_views_count or 0,
            "delta_likes_count": self.delta_likes_count or 0,
            "delta_comments_count": self.delta_comments_count or 0,
            "delta_reports_count": self.delta_reports_count or 0
        }