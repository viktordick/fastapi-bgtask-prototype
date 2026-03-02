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


@dataclass
class AppTblCleanupRunResult:
    count: int
    limit_reached: bool


class AppTblCleanup(SQLModel, table=True):
    id: Optional[int] = Col("apptblcleanup_id", default=None, primary_key=True)
    tablename: str = Col("apptblcleanup_tablename")
    range: Optional[str] = Col("apptblcleanup_range")
    filter: Optional[str] = Col("apptblcleanup_filter")

    def run(self, session: Session) -> AppTblCleanupRunResult:
        """
        Execute cleanup for the selected table.
        Returns if the limit was reached
        """
        LIMIT = 100_000
        table = Table(
            self.tablename,
            metadata_obj,
            Column(f"{self.tablename}_id", Integer, primary_key=True, key="id"),
            Column(f"{self.tablename}_modtime", DateTime(timezone=True), key="modtime"),
            extend_existing=True,
        )
        ids = (
            session.exec(
                select(table.c.id)
                .where(table.c.modtime < text("now() - (:ival)::interval"))
                .where(text(self.filter))
                .params(ival=self.range)
                .order_by(table.c.id)
                .limit(LIMIT)
            )
            .scalars()
            .all()
        )
        session.exec(delete(table).where(table.c.id == any_(ids)))
        return AppTblCleanupRunResult(
            count=len(ids),
            limit_reached=(len(ids) == LIMIT),
        )
