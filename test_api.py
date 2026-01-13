import pytest
from fastapi.testclient import TestClient

from api import app

client = TestClient(app)

def test_app_starts():
    response = client.get("/docs")
    assert response.status_code == 200



# CHARACTERS

def test_get_characters():
    response = client.get("/characters")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)

    if data:
        character = data[0]

        assert "id" in character
        assert "name" in character
        assert "planet" in character
        assert isinstance(character["planet"], dict)


def test_get_character_by_id():
    response = client.get("/characters/1")

    if response.status_code == 404:
        pytest.skip("Character with id=1 not found")

    assert response.status_code == 200
    data = response.json()

    assert "name" in data
    assert "planet" in data
    assert isinstance(data["planet"], dict)


def test_get_character_not_found():
    response = client.get("/characters/999999")
    assert response.status_code == 404



# PLANETS

def test_get_planets():
    response = client.get("/planets")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)

    if data:
        planet = data[0]
        assert "name" in planet
        assert "population" in planet


def test_get_planet_by_id():
    response = client.get("/planets/id/1")

    if response.status_code == 404:
        pytest.skip("Planet with id=1 not found")

    assert response.status_code == 200
    data = response.json()

    assert "name" in data
    assert "climate" in data


def test_get_planet_by_name():
    response = client.get("/planets/name/Bespin")

    if response.status_code == 404:
        pytest.skip("Planet 'Bespin' not found")

    assert response.status_code == 200
    data = response.json()

    assert data["name"].lower() == "bespin"



# PLANET + CHARACTERS

def test_get_planet_with_characters():
    response = client.get("/planets/name/Bespin/characters")

    if response.status_code == 404:
        pytest.skip("Planet 'Bespin' not found")

    assert response.status_code == 200
    data = response.json()

    assert "characters" in data
    assert isinstance(data["characters"], list)

    if data["characters"]:
        character = data["characters"][0]
        assert "name" in character
        assert "gender" in character
