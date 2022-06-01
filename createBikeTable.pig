/* 
Aquest script de pig permet processar les dates dels datasets
https://s3.amazonaws.com/capitalbikeshare-data/index.html
*/

/* Permet descarregar csv de format molts diversos */
register /usr/lib/pig/piggybank.jar;

/* Important llevar la capçalera per les futures transformacions */
/* Per poder operar les dates s'han d'incorporar com chararray, mes envant es tranformaran DateTime */
bike_rental = LOAD '$INPUT_BIKES'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') 
    AS (
        Duration:int,
        Start_date:chararray, 
        End_date:chararray, 
        Start_station_number:chararray, 
        Start_station:chararray, 
        End_station_number:chararray, 
        End_station:chararray, 
        Bike_number:chararray, 
        Member_type:chararray
    );


/* Perquè la tranformacions de les dates no donin problemes ens hem d'assegurar que tenen el format correcte,
per això s'aplica una explesió regular */
bike_rental_regex_date = filter bike_rental by
    (Start_date MATCHES '^([0-9]{4})-([0-1][0-9])-([0-3][0-9])\\s([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9])$')
    and (End_date MATCHES '^([0-9]{4})-([0-1][0-9])-([0-3][0-9])\\s([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9])$');


/* Convertir els strings amb el tipus DataTime amb la funció ToDate */
/* Amb les funcions GetWeekYear i GetWeek agafam els valors corresponents a l'any i la setmana de l'any */
bike_rental_week = foreach bike_rental_regex_date generate
    Duration,
    GetWeekYear(ToDate(Start_date,'yyyy-MM-dd HH:mm:ss')) AS Start_date_wy,
    GetWeek(ToDate(Start_date,'yyyy-MM-dd HH:mm:ss')) AS Start_date_w,
    GetWeekYear(ToDate(End_date,'yyyy-MM-dd HH:mm:ss')) AS End_date_wy,
    GetWeek(ToDate(End_date,'yyyy-MM-dd HH:mm:ss')) AS End_date_w,
    Start_station_number, 
    Start_station, 
    End_station_number, 
    End_station, 
    Bike_number, 
    Member_type;


/* Agrupar per Bike_number, Start_date_wy, Start_date_w */
bike_week = GROUP bike_rental_week BY (Bike_number, Start_date_wy, Start_date_w);

/* Obtenim el temps que s'ha utilitzat una bicicleta per setmana */
/* Obtenim el nombre de trajectes que ha realitzat una bicicleta per setmana */
bike_week_duration = FOREACH bike_week GENERATE 
    group.Bike_number as Bike_number, 
    group.Start_date_wy as Start_date_wy, 
    group.Start_date_w as Start_date_w, 
    SUM(bike_rental_week.Duration) as SUM, 
    COUNT(bike_rental_week.Bike_number) as num_trajectes;

/* Guadar el resultat */
STORE bike_week_duration INTO '$OUTPUT_BIKES' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');


/* ---------------------------------------------------------------- */


/* Agrupar per Bike_number, Start_date_wy, Start_date_w */
station_week = GROUP bike_rental_week BY (Bike_number, Start_date_wy, Start_date_w, Start_station_number, End_station_number);

/* Obtenim els usos de les estacions setmana */
/* Obtenim el nombre de bicicleta de les estacions per setmana */
station_week_use = FOREACH station_week GENERATE 
   group.Bike_number as Bike_number, 
   group.Start_date_wy as Start_date_wy, 
   group.Start_date_w as Start_date_w, 
   SUM(bike_rental_week.Start_station_number) as Start_station_number, 
   SUM(bike_rental_week.Start_station) as Start_station, 
   SUM(bike_rental_week.End_station_number) as End_station_number, 
   SUM(bike_rental_week.End_station) as End_station, 
   COUNT(bike_rental_week.Bike_number) as num_trajectes;

/* Guadar el resultat */
STORE station_week_use INTO '$OUTPUT_STATIONS' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');

/*
https://www.javatpoint.com/pig
https://www.cloudduggu.com/pig/datetime-built-in-functions/
https://pig.apache.org/docs/latest/func.html#datetime-functions
*/
