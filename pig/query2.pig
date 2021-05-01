
-- Counties in CA where accidents happened where visibility < 5

us_accidents = LOAD '/home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/pig/US_Accidents_Dec20.csv' USING PigStorage(',') AS (ID:CHARARRAY, Source:CHARARRAY, TMC:DOUBLE, Severity:INT, Start_Time:CHARARRAY, End_Time:CHARARRAY, Start_Lat:DOUBLE, Start_Lng:DOUBLE, End_Lat:DOUBLE, End_Lng:DOUBLE, Distance:DOUBLE, Description:CHARARRAY, Number:DOUBLE, Street:CHARARRAY, Side:CHARARRAY, City:CHARARRAY, County:CHARARRAY, State:CHARARRAY, Zipcode:CHARARRAY, Country:CHARARRAY, Timezone:CHARARRAY, Airport_Code:CHARARRAY, Weather_Timestamp:CHARARRAY, Temperature:DOUBLE, Wind_Chill:DOUBLE, Humidity:DOUBLE, Pressure:DOUBLE, Visibility:DOUBLE, Wind_Direction:CHARARRAY, Wind_Speed:DOUBLE, Precipitation:DOUBLE, Weather_Condition:CHARARRAY, Amenity:BOOLEAN, Bump:CHARARRAY, Crossing:BOOLEAN, Give_Way:BOOLEAN, Junction:BOOLEAN, No_Exit:BOOLEAN, Railway:BOOLEAN, Roundabout:BOOLEAN, Station:BOOLEAN, Stop:BOOLEAN, Traffic_Calming:BOOLEAN, Traffic_Signal:BOOLEAN, Turning_Loop:BOOLEAN, Sunrise_Sunset:CHARARRAY, Civil_Twilight:CHARARRAY, Nautical_Twilight:CHARARRAY, Astronomical_Twilight:CHARARRAY);

columns = FOREACH us_accidents GENERATE ID, State, Visibility;

county = FOREACH us_accidents GENERATE ID, County;

state = FILTER columns BY State == 'CA' AND NOT Visibility is null;

visibility = FILTER state BY Visibility < 5;

inner_join = JOIN county BY ID, visibility BY ID;

records = FOREACH inner_join GENERATE State, County, Visibility;

county_visibility = DISTINCT records;

STORE county_visibility INTO '/home/ebd/Documents/HadoopProjects/USTrafficAccidentsProject/pig/query2_output.txt' using PigStorage(';');
