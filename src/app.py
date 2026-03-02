from fastapi import Depends, FastAPI
from sqlmodel import SQLModel, select
from yaml import CLoader, load

from .apptblcleanup import AppTblCleanup
from .auth import User
from .dbsession import DBSession, DBSessionMiddleware

with open("config.yml") as f:
    config = load(f, Loader=CLoader)

user = User(config["pwhash"])

app = FastAPI(dependencies=[Depends(user.verify)])
app.add_middleware(DBSessionMiddleware, connstr=config["connstr"])


@app.post("/bgtask/init")
async def init(session: DBSession):
    """
    Initialize required table
    """
    SQLModel.metadata.create_all(session.get_bind())


@app.post("/bgtask/tablecleanup")
async def tablecleanup(session: DBSession) -> dict[str, AppTblCleanup.RunResult]:
    """
    Clean up data according to given rules in apptblcleanup.
    Commits after each processed batch. Returns a mapping from table name to
    number of entries deleted.
    """
    rows = list(session.exec(select(AppTblCleanup)))
    stats = {
        row.tablename: AppTblCleanup.RunResult(
            count=0,
            elapsed=0.0,
            limit_reached=False,
        )
        for row in rows
    }
    while rows:
        new_rows = []
        for row in rows:
            res = row.run(session)
            tgt = stats[row.tablename]
            tgt.count += res.count
            tgt.elapsed += res.elapsed
            tgt.limit_reached = tgt.limit_reached or res.limit_reached
            if res.limit_reached:
                new_rows.append(row)
            session.commit()
        rows = new_rows
    return stats
