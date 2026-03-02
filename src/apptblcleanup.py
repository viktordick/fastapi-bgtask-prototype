import time
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    Table,
    any_,
    delete,
    literal,
    select,
    text,
)
from sqlmodel import Field, Session, SQLModel


def Col(colname, **kw):
    """
    Wrapper for field type with aliased column name on the DB
    """
    return Field(**kw, sa_column_kwargs={"name": colname})


metadata_obj = MetaData()


class AppTblCleanup(SQLModel, table=True):
    id: Optional[int] = Col("apptblcleanup_id", default=None, primary_key=True)
    tablename: str = Col("apptblcleanup_tablename")
    range: Optional[str] = Col("apptblcleanup_range")
    filter: Optional[str] = Col("apptblcleanup_filter")

    @dataclass
    class RunResult:
        count: int  # number of deleted records
        elapsed: float  # time in seconds it took
        limit_reached: bool  # if the limit was reached

    def run(self, session: Session) -> RunResult:
        """
        Execute cleanup for the selected table.
        """
        start = time.monotonic()
        LIMIT = 100_000
        table = Table(
            self.tablename,
            metadata_obj,
            Column(f"{self.tablename}_id", Integer, primary_key=True, key="id"),
            Column(f"{self.tablename}_modtime", DateTime(timezone=True), key="modtime"),
            extend_existing=True,
        )
        ids = (
            session.execute(
                select(table.c.id)
                .where(table.c.modtime < text("now() - (:ival)::interval"))
                .where(text(self.filter or "true"))
                .params(ival=self.range)
                .order_by(table.c.id)
                .limit(LIMIT)
            )
            .scalars()
            .all()
        )
        session.execute(delete(table).where(table.c.id == any_(literal(ids))))
        elapsed = time.monotonic() - start
        return AppTblCleanup.RunResult(
            count=len(ids),
            limit_reached=(len(ids) == LIMIT),
            elapsed=elapsed,
        )
