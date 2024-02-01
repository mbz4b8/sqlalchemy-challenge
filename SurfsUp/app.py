# Import the dependencies.
import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from datetime import datetime

from flask import Flask, jsonify



#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return (
        f"Welcome to the SurfsUp API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/2016-08-18<br/>"
        f"/api/v1.0/2016-08-18/2017-08-18<br/>"
    )


"""TODO: Convert the query results from your precipitation analysis (i.e. retrieve 
only the last 12 months of data) to a dictionary using date as the key and prcp as the value.
Return a JSON list of temperature observations for the previous year. """

@app.route("/api/v1.0/precipitation")
def precipitation():

    # Starting from the most recent data point in the database. 
    recent_date_row = session.query(func.max(measurement.date)).first()
    recent_date = recent_date_row[0]  

    # Calculate the date one year from the last date in data set.
    end_date = dt.datetime.strptime(recent_date, '%Y-%m-%d')
    start_date = end_date - dt.timedelta(days=366)

    # Perform a query to retrieve the data and precipitation scores
    results = session.query(measurement.date, measurement.prcp).\
        filter(measurement.date >= start_date).\
        order_by(measurement.date).all()
    
    #Convert list of tuples into normal list
    all_precipitation = list(np.ravel(results))

    return jsonify(all_precipitation)

    

"""TODO: Return a JSON list of stations from the dataset. """

@app.route("/api/v1.0/stations")
def stations():

    #Query all stations
    results_station = session.query(measurement.station).distinct().all()

    #Convert tuples into normal list
    all_stations = [result[0] for result in results_station]

    return jsonify(all_stations)

"""TODO: Query the dates and temperature observations of the most-active station for the previous year of data.
Return a JSON list of temperature observations for the previous year """

@app.route("/api/v1.0/tobs")
def tobs():
    # Using the most active station id from the previous query
    most_active_station_id = 'USC00519281'
    # Using the most active station id
    # Query the last 12 months of temperature observation data for this station 
    # Starting from the most recent data point in the database. 
    recent_date_row = session.query(func.max(measurement.date)).filter(measurement.station == most_active_station_id).scalar()

    # Calculate the date one year from the last date in data set.
    end_date = dt.datetime.strptime(recent_date_row, '%Y-%m-%d')
    start_date = end_date - dt.timedelta(days=366)

    # Perform a query to retrieve the data and precipitation scores
    results = session.query(measurement.date, measurement.tobs).\
    filter(measurement.date >= start_date).\
    filter(measurement.station == most_active_station_id).\
    order_by(measurement.date).all()

    #Convert list of tuples into normal list
    mostactive_tobs = list(np.ravel(results))

    return jsonify(mostactive_tobs)


#"""TODO: Return a JSON list of the minimum temperature, the average temperature, 
#and the maximum temperature for a specified start or start-end range. For a specified start, 
#calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date. For a 
#specified start date and end date, calculate TMIN, TAVG, and TMAX for the 
#dates from the start date to the end date, inclusive.. """

#Define your Flask route for temperature statistics
@app.route("/api/v1.0/<start>")
def start_date(start): 
    try:
        # Parse the start date from the URL and convert it to a datetime object
        start_date = dt.datetime.strptime(start, '%Y-%m-%d')

        # Retrieve the minimum date from the database
        min_date_in_db_str = session.query(func.min(measurement.date)).scalar()

        # Convert the minimum date string to a datetime object
        min_date_in_db = dt.datetime.strptime(min_date_in_db_str, '%Y-%m-%d')

        # Retrieve the maximum date from the database
        max_date_in_db_str = session.query(func.max(measurement.date)).scalar()

        # Convert the maximum date string to a datetime object
        max_date_in_db = dt.datetime.strptime(max_date_in_db_str, '%Y-%m-%d')

        # Check if the start date falls between the minimum and maximum dates
        if start_date < min_date_in_db or start_date > max_date_in_db:
            return jsonify({"error": "Start date is not within the valid range of the data (2010-01-01 to 2017-08-23)."}), 400
        
        # Define query to calculate TMIN, TAVG, and TMAX based on the provided date range
        results_start = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
                          filter(measurement.date >= start_date).all()

        # Create a dictionary to hold the results
        temperature_starts = {
            "start_date": start_date.strftime('%Y-%m-%d'),
            "TMIN": results_start[0][0],
            "TAVG": results_start[0][1],
            "TMAX": results_start[0][2]
        }

        return jsonify(temperature_starts)
    
    except ValueError:
        # Handle invalid date format in the URL
        return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD format."}), 400
    except Exception as e:
        # Handle other exceptions
        return jsonify({"error": str(e)}), 500

 #Define your Flask route for temperature statistics
@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start, end):
    try:
        # Parse the start date and end date from the URL and convert them to datetime objects
        start_date = dt.datetime.strptime(start, '%Y-%m-%d')
        end_date = dt.datetime.strptime(end, '%Y-%m-%d')

        # Check if the end date is after the start date
        if end_date <= start_date:
            return jsonify({"error": "End date must be after the start date."}), 400

        # Retrieve the minimum and maximum dates from the database
        min_date_in_db_str = session.query(func.min(measurement.date)).scalar()
        min_date_in_db = dt.datetime.strptime(min_date_in_db_str, '%Y-%m-%d')

        max_date_in_db_str = session.query(func.max(measurement.date)).scalar()
        max_date_in_db = dt.datetime.strptime(max_date_in_db_str, '%Y-%m-%d')

        # Check if the start date and end date fall within the valid range of the data
        if start_date < min_date_in_db or start_date > max_date_in_db:
            return jsonify({"error": "Start date is not within the valid range of the data (2010-01-01 to 2017-08-23)."}), 400
        
        if end_date < min_date_in_db or end_date > max_date_in_db:
            return jsonify({"error": "End date is not within the valid range of the data (2010-01-01 to 2017-08-23)."}), 400

        # Define query to calculate TMIN, TAVG, and TMAX based on the provided date range
        results = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
                  filter(measurement.date >= start_date, measurement.date <= end_date).all()

        # Create a dictionary to hold the results
        temperature_stats = {
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d'),
            "TMIN": results[0][0],
            "TAVG": results[0][1],
            "TMAX": results[0][2]
        }

        return jsonify(temperature_stats)
   
    except ValueError:
        # Handle invalid date format in the URL
        return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD format."}), 400
    except Exception as e:
        # Handle other exceptions
        return jsonify({"error": str(e)}), 500
    
if __name__ == "__main__":
    app.run(debug=True)