/* Aquest script de pig permet processar les dates dels datasets
https://s3.amazonaws.com/capitalbikeshare-data/index.html
*/

/* Permet descarregar csv de format molts diversos */
register /usr/lib/pig/piggybank.jar;

/* Important llevar la capçalera per les futures transformacions */
/* Per poder operar les dates s'han d'incorporar com chararray,
   mesenvant es tranformaran DateTime */
capitalbike = LOAD '$INPUT_BIKES'
   USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') 
    AS (Duration:int,
        Start_date:chararray, 
        End_date:chararray, 
        Start_station_number:chararray, 
        Start_station:chararray, 
        End_station_number:chararray, 
        End_station:chararray, 
        Bike_number:chararray, 
        Member_type:chararray
    );
--dump capitalbike;

/* Perquè la tranformacions de les dates no donin problemes
   ens hem d'assegurar que tenen el format correcte,
   per això s'aplica una explesió regular */
capitalbikeregexdate = filter capitalbike by
    (Start_date MATCHES '^([0-9]{4})-([0-1][0-9])-([0-3][0-9])\\s([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9])$')
    and (End_date MATCHES '^([0-9]{4})-([0-1][0-9])-([0-3][0-9])\\s([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9])$');
--dump capitalbikeregexdate;

/* Convertir els strings amb el tipus DataTime amb la funció ToDate */
capitalbikedate = foreach capitalbikeregexdate generate
        Duration,
        ToDate(Start_date,'yyyy-MM-dd HH:mm:ss') AS Start_date_t,
        ToDate(End_date,'yyyy-MM-dd HH:mm:ss') AS End_date_t,
        Start_station_number, 
        Start_station, 
        End_station_number, 
        End_station, 
        Bike_number, 
        Member_type;
--dump capitalbikeregexdate;

/* Amb les funcions GetWeekYear i GetWeek agafam els valors
   corresponents a l'any i la setmana de l'any */
capitalbikedateweek_01 = foreach capitalbikedate generate
        Duration,
        GetWeekYear(Start_date_t) AS Start_date_wy,
        GetWeek(Start_date_t) AS Start_date_w,
        GetWeekYear(End_date_t) AS End_date_wy,
        GetWeek(End_date_t) AS End_date_w,
        Start_station_number, 
        Start_station, 
        End_station_number, 
        End_station, 
        Bike_number, 
        Member_type;
--dump capitalbikedateweek_01;

/* Agrupar per Bike_number,Start_date_wy,Start_date_w */
bikeweek = GROUP  capitalbikedateweek_01 BY (Bike_number,Start_date_wy,Start_date_w);

/* Obtenim el temps que s'ha utilitzat una bicicleta per setmana */
bikeweek_duration_SUM = FOREACH bikeweek GENERATE group, SUM(capitalbikedateweek_01.Duration) as SUM, COUNT(capitalbikedateweek_01.Bike_number) as num_trajectes;

/* Guadar el resultat */
STORE bikeweek_duration_SUM INTO '$OUTPUT_BIKES' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');

/*
https://www.javatpoint.com/pig
https://www.cloudduggu.com/pig/datetime-built-in-functions/
https://pig.apache.org/docs/latest/func.html#datetime-functions
*/

