create table census (age real, workclass text, fnlwgt real, education text, education_num real, marital_status text, occupation text, relationship text, race text, sex text, capital_gain real, capital_loss real, hours_per_week real, native_country text, economic_indicator text);
\copy census from adult.data.txt delimiter ',' csv;
\copy census from adult.test.txt delimiter ',' csv header;