-- Data Cleaning
select *
From layoffs;

CREATE TABLE layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- Remove Duplicates 
WITH duplicate_cte AS
(
select *,
row_number () over ( 
partition by company,location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE ROW_NUM > 1;

Select *
from layoffs_staging
where company = 'casper';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2;

INSERT INTO layoffs_staging2
select *,
row_number() over ( 
partition by company,location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
from layoffs_staging;


select * 
from layoffs_staging2
where row_num > 1;

delete 
from layoffs_staging2
where row_num > 1;

select * 
from layoffs_staging2
where row_num > 1;

SET SQL_SAFE_UPDATES = 0;

select * 
from layoffs_staging2;

-- Standardizing data

SELECT DISTINCT (trim(company))
FROM layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select company, trim(company)
from layoffs_staging2;

select * 
from layoffs_staging2;

select distinct industry 
from layoffs_staging2
order by 1;
select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

select * 
from layoffs_staging2
where industry = 'crypto';

select * 
from layoffs_staging2
where country like 'United States%'
order by 1 ;
	
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2 
set country = trim(trailing '.' from country)
where country like 'United States';

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` DATE;

select *
from layoffs_staging2;

-- WORKING WITH NULL AND BLANK VALUES
select *
from layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company like 'Bally%' ;


Update layoffs_staging2 
set industry = null
where industry ='';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company = t2.company
where (t1.industry is null or t1.industry = '')    
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')    
and t2.industry is not null;


select * 
from layoffs_staging2;



select *
from layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;

delete  
from layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;

Alter table layoffs_staging2
drop column row_num;

-- Exploratory Data Analysis

select * 
from layoffs_staging2;

select MAX(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select * 
from layoffs_staging2
where percentage_laid_off = 1;

select * 
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off DESC;

select company, SUM(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


select industry, SUM(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country, SUM(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;


select `date`, SUM(total_laid_off)
from layoffs_staging2
group by `date`
order by 1 desc;


select year(`date`), SUM(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage, SUM(total_laid_off)
from layoffs_staging2
group by stage
order by 1 desc;


select stage, SUM(total_laid_off)
from layoffs_staging2
group by stage
order by 1 desc;


select substring(`date` , 1, 7) AS `Month`, sum(total_laid_off)
from layoffs_staging2
WHERE substring(`date` , 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

WITH ROLLING_TOTAL AS 
( 
select substring(`date` , 1, 7) AS `Month`, sum(total_laid_off) AS TOTAL_OFF
from layoffs_staging2
WHERE substring(`date` , 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off
,SUM(TOTAL_OFF) OVER(ORDER BY `MONTH`) AS rolling_total
FROM ROLLING_TOTAL;

select company, SUM(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select company,YEAR(`date`), SUM(total_laid_off)
from layoffs_staging2
group by company, YEAR(`date`)
order by 3 desc;

SELECT 
    company, 
    YEAR(`date`) AS layoff_year, 
    SUM(total_laid_off) AS total_laid_off_count
FROM 
    layoffs_staging2
GROUP BY 
    company, 
    layoff_year
ORDER BY 
    total_laid_off_count DESC;
 SELECT 
    company, 
    YEAR(`date`) AS layoff_year, 
    SUM(total_laid_off) AS total_laid_off_count,
    ROW_NUMBER() OVER (PARTITION BY YEAR(`date`) ORDER BY SUM(total_laid_off) DESC) AS layoff_rank
FROM 
    layoffs_staging2
WHERE 
    YEAR(`date`) IS NOT NULL
GROUP BY 
    company, 
    layoff_year
ORDER BY 
    layoff_year, 
    total_laid_off_count DESC;
    
 SELECT *
FROM (
    SELECT 
        company, 
        YEAR(`date`) AS layoff_year, 
        SUM(total_laid_off) AS total_laid_off_count,
        ROW_NUMBER() OVER (PARTITION BY YEAR(`date`) ORDER BY SUM(total_laid_off) DESC) AS layoff_rank
    FROM 
        layoffs_staging2
    WHERE 
        YEAR(`date`) IS NOT NULL
    GROUP BY 
        company, 
        layoff_year
) AS ranked_data
WHERE layoff_rank <= 5
ORDER BY 
    layoff_year, 
    total_laid_off_count DESC;


   
    
    
    
    
    
    
    
    







