from fastapi import Depends, FastAPI
from yaml import CLoader, load

from .auth import User
from .dbsession import DBSession, DBSessionMiddleware

with open("config.yml") as f:
    config = load(f, Loader=CLoader)

user = User(config["pwhash"])

app = FastAPI(dependencies=[Depends(user.verify)])
app.add_middleware(DBSessionMiddleware, connstr=config["connstr"])


@app.post("/bgtask/cron_tablecleanup")
async def tablecleanup(session: DBSession):
    """
    TODO
    """
    return {"success": True}
