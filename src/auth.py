from typing import Annotated

import argon2
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

BasicAuth = Annotated[HTTPBasicCredentials, Depends(HTTPBasic())]


class User:
    pwhash: str
    hasher: argon2.PasswordHasher

    def __init__(self, pwhash):
        self.pwhash = pwhash
        self.hasher = argon2.PasswordHasher()

    def check(self, password) -> bool:
        try:
            self.hasher.verify(self.pwhash, password)
            return True
        except argon2.exceptions.VerifyMismatchError:
            return False

    def verify(self, auth: BasicAuth) -> None:
        """
        To be used as dependency
        """
        if not self.check(auth.password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Basic"},
            )
