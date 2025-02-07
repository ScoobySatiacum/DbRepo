import logging

import pytest

from db_repo.dbrepository import DbRepository

from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(filename="", level=logging.INFO, format='%(asctime)s %(message)s')

db_path = "/srv/weather_data_api/weather_data.sqlite"

def test_init():
    my_file = Path(db_path)
    if (my_file.exists()):
        Path.unlink(db_path)
        
    sut = DbRepository(db_path)

    assert sut

def test_create_connection_success():
    db_path = "/srv/weather_data_api/weather_data.sqlite"
    sut = DbRepository(db_path)

    result = sut.create_connection()

    assert result

def test_execute_query():
    db_path = "/srv/weather_data_api/weather_data.sqlite"
    sut = DbRepository(db_path)

    result, test = sut.execute_query("create table test (col0 int)")

    assert result == True