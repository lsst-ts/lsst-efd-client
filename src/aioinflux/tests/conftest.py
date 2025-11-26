import asyncio

import pytest
import pytest_asyncio


from aioinflux.aioinflux import InfluxDBClient
import testing_utils as utils
import subprocess
import time
import requests

CONTAINER_NAME = "influxdb_test_for_pytest"
INFLUX_PORT = 8086
INFLUX_IMAGE = "influxdb:1.8"


@pytest.fixture(scope="session", autouse=True)
def influxdb_service():
    """
    Starts and stops the InfluxDB Docker container for the entire test session.
    """
    print(f"\n--- Starting InfluxDB container ({CONTAINER_NAME}) ---")

    # --- SETUP (Before Tests) ---
    try:
        # 1. Start the container in detached mode (-d)
        subprocess.run(
            [
                "docker", "run", "-d",
                "--rm",  # Automatically remove the container when it exits
                f"--name={CONTAINER_NAME}",
                "-p", f"{INFLUX_PORT}:{INFLUX_PORT}",
                INFLUX_IMAGE
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # 2. Wait for the database to be ready
        max_wait_time = 30
        start_time = time.time()
        ready = False

        print(f"Waiting for InfluxDB to become available on port {INFLUX_PORT}...")
        while time.time() - start_time < max_wait_time:
            try:
                # InfluxDB uses /ping endpoint, which returns 204 when ready
                response = requests.get(f"http://127.0.0.1:{INFLUX_PORT}/ping")
                if response.status_code == 204:
                    ready = True
                    print("InfluxDB is ready!")
                    break
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(1)

        if not ready:
            raise RuntimeError("InfluxDB failed to start within the timeout.")

        # --- YIELD (Run Tests) ---
        yield

    finally:
        # --- TEARDOWN (After Tests) ---
        print(f"\n--- Stopping and cleaning up container ({CONTAINER_NAME}) ---")

        # Stop the container
        # Use try/except just in case the container failed to start
        try:
            subprocess.run(
                ["docker", "stop", CONTAINER_NAME],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        except subprocess.CalledProcessError as e:
            print("Error stopping container (might already be stopped): "
                  f"{e.stderr.decode()}")


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="module")
async def client():
    async with InfluxDBClient(db="client_test", mode="async") as client:
        await client.create_database()
        yield client
        await client.drop_database()


@pytest.fixture(scope="module")
def df_client():
    if utils.pd is None:
        return
    with InfluxDBClient(
        db="df_client_test", mode="blocking", output="dataframe"
    ) as client:
        client.create_database()
        yield client
        client.drop_database()


@pytest_asyncio.fixture(scope="module")
async def iter_client():
    async with InfluxDBClient(db="iter_client_test", mode="async") as client:
        await client.create_database()
        await client.write([p for p in utils.cpu_load_generator(100)])
        yield client
        await client.drop_database()
