create view married as
select * from census where marital_status in (' Married-AF-spouse', ' Married-civ-spouse', ' Married-spouse-absent',' Separated');

create view unmarried as
select * from census where marital_status in (' Never-married', ' Widowed',' Divorced');