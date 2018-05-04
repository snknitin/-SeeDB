create table split1 (age real, workclass text, fnlwgt real, education text, education_num real, marital_status text, occupation text, relationship text, race text, sex text, capital_gain real, capital_loss real, hours_per_week real, native_country text, economic_indicator text);
create table split2 (age real, workclass text, fnlwgt real, education text, education_num real, marital_status text, occupation text, relationship text, race text, sex text, capital_gain real, capital_loss real, hours_per_week real, native_country text, economic_indicator text);
create table split3 (age real, workclass text, fnlwgt real, education text, education_num real, marital_status text, occupation text, relationship text, race text, sex text, capital_gain real, capital_loss real, hours_per_week real, native_country text, economic_indicator text);
create table split4 (age real, workclass text, fnlwgt real, education text, education_num real, marital_status text, occupation text, relationship text, race text, sex text, capital_gain real, capital_loss real, hours_per_week real, native_country text, economic_indicator text);
create table split5 (age real, workclass text, fnlwgt real, education text, education_num real, marital_status text, occupation text, relationship text, race text, sex text, capital_gain real, capital_loss real, hours_per_week real, native_country text, economic_indicator text);
\copy split1 from ../Data/adult.split_0.csv delimiter ',' csv;
\copy split2 from ../Data/adult.split_1.csv delimiter ',' csv;
\copy split3 from ../Data/adult.split_2.csv delimiter ',' csv;
\copy split4 from ../Data/adult.split_3.csv delimiter ',' csv;
\copy split5 from ../Data/adult.split_4.csv delimiter ',' csv;

create view census as select * from split1 union select * from split2 union select * from split3 union select * from split4 union select * from split5;
