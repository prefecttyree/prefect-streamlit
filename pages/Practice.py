import httpx
import prefect
from prefect import task, flow, get_run_logger, runtime

@task(name="fetch_weather_data")
def fetch_weather_data(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast"
    temps = httpx.get(base_url,
                      params=dict(latitude=52.52, longitude=13.405, hourly="temperature_2m"))
    forecast = float(temps.json()["hourly"]["temperature_2m"][0])
    print(forecast)
    return forecast



@task(name="save_weather", )
def save_weather(temp: float):
    with open("weather.csv", "w+") as w:
        w.write(str(temp))
    return "Successfully wrote temp"

@flow(name="log_it")
def log_it():
    logger = get_run_logger()
    logger.info("INFO level log message.")
    logger.debug("DEBUG level log message.")
    logger.error("ERROR level log message.")
    logger.critical("CRITICAL level log message.")

@task()
def my_task(y):
    print("I am a task", runtime.task_run.name)
    print("Flow run parameters:", runtime.flow_run.parameters)

@flow(log_prints=True)
def my_flow():
    print("my Name is", runtime.flow_run.name)
    print("I belong to deployment", runtime.deployment.name)
    my_task(2)

@flow(name="pipeline",log_prints=True)
def pipeline(lat: float = 38.9, lon: float = -77.0):
    temp = fetch_weather_data(lat, lon)
    result = save_weather(temp)
    log_it()
    my_flow()
    return result




if __name__ == "__main__":
    #pipeline.serve(name="fetch-weather-data" )
    pipeline()
    