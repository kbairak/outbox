import datetime
import reprlib
from typing import Optional

from sqlalchemy import JSON, DateTime, Index, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class OutboxTable(Base):
    __tablename__ = "outbox_table"

    id: Mapped[int] = mapped_column(primary_key=True)
    routing_key: Mapped[str] = mapped_column(Text)
    body: Mapped[bytes] = mapped_column()
    tracking_ids: Mapped[list[str]] = mapped_column(JSON)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    expiration: Mapped[Optional[datetime.timedelta]] = mapped_column()
    send_after: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True))
    sent_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        # Partial index for pending messages
        Index(
            "outbox_pending_idx",
            "send_after",
            "created_at",
            postgresql_where=(sent_at.is_(None)),
        ),
        # Partial index for cleanup
        Index(
            "outbox_cleanup_idx",
            "sent_at",
            postgresql_where=(sent_at.is_not(None)),
        ),
    )

    def __repr__(self) -> str:
        routing_key = self.routing_key
        body = reprlib.repr(self.body)
        tracking_ids = self.tracking_ids
        created_at = self.created_at
        args = [f"{routing_key=}", f"{body=}", f"{tracking_ids=}", f"{created_at=}"]
        if self.send_after != self.created_at:
            send_after = self.send_after
            args.append(f"{send_after=}")
        if self.expiration:
            expiration = self.expiration.total_seconds
            args.append(f"{expiration=}")
        return f"OutboxTable({', '.join(args)})"
