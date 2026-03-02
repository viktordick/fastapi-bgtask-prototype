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
async def tablecleanup(session: DBSession) -> dict[str, int]:
    """
    Clean up data according to given rules in apptblcleanup.
    Commits after each processed batch. Returns a mapping from table name to
    number of entries deleted.
    """
    rows = session.exec(select(AppTblCleanup))
    stats = {}
    while rows:
        new_rows = []
        for row in rows:
            res = row.run(session)
            stats[row.tablename] = stats.get(row.tablename, 0) + res.count
            if res.limit_reached:
                new_rows.append(row)
            session.commit()
        rows = new_rows
    return stats
