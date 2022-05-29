register /usr/lib/pig/piggybank.jar;
capitalbike = LOAD '$Input' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') 
    AS (Duration:int,
        Start_date:datetime, 
        End_date:datetime, 
        Start_station_number:chararray, 
        Start_station:chararray, 
        End_station_number:chararray, 
        End_station:chararray, 
        Bike_number:chararray, 
        Member_type:chararray
    );
/* Agrupar per Bike_number */
bike = GROUP  capitalbike BY (Bike_number); 
bike_duration_SUM = FOREACH bike GENERATE group, SUM(capitalbike.Duration) as SUM;
/* Guadar el resultat */
STORE bike_duration_SUM INTO '$Output' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');
