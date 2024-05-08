-- Layoff Data Exploration Project in SQL
--
-- Irem Arici, May 2024

-- DATA CLEANING

DROP TABLE layoffs_staging;
DROP TABLE layoffs_staging2;

SELECT * FROM layoffs;
CREATE TABLE layoffs_staging LIKE layoffs;
SELECT * FROM layoffs_staging;
INSERT layoffs_staging SELECT * FROM layoffs; -- New table

SELECT *, ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
	FROM layoffs_staging; -- filter row num

WITH duplicate_cte AS
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT * FROM duplicate_cte WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2 WHERE row_num >1;

INSERT INTO layoffs_staging2
	SELECT *, ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, 
    stage, country, funds_raised_millions) AS row_num FROM layoffs_staging;

DELETE FROM layoffs_staging2 WHERE row_num >1;

-- Standardizing data

SELECT company, TRIM(company) FROM layoffs_staging2; 
UPDATE layoffs_staging2 SET company = TRIM(company);

-- Crypto is tripled in dataset, so we will combine them.
SELECT DISTINCT industry FROM layoffs_staging2 ORDER BY 1; 
SELECT * FROM layoffs_staging2 WHERE industry LIKE "Crypto%";
UPDATE layoffs_staging2 SET industry = "Crypto" WHERE industry LIKE "Crypto%";

-- Cryptos are combined under one name
SELECT DISTINCT industry FROM layoffs_staging2; 

-- United States & United States. are doubles and we will fix it
SELECT DISTINCT country FROM layoffs_staging2 ORDER BY 1; 
SELECT * FROM layoffs_staging2 WHERE country LIKE "United States%";
SELECT DISTINCT country, TRIM(TRAILING "." FROM country) FROM layoffs_staging2 ORDER BY 1; 
UPDATE layoffs_staging2 SET country = TRIM(TRAILING "." FROM country) 
	WHERE country LIKE "United States%";

 -- standardizing date column & updating the table
SELECT `date`, STR_TO_DATE(`date`, "%m/%d/%Y") FROM layoffs_staging2; 
UPDATE layoffs_staging2 SET `date` = STR_TO_DATE(`date`, "%m/%d/%Y") WHERE `date`!= "None";

 -- deleting None values from the column first
UPDATE layoffs_staging2 SET `date` = NULL WHERE `date`= "None";
ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;

-- companies where laid off columns are null/none
SELECT * FROM layoffs_staging2 WHERE total_laid_off = "None" 
	AND percentage_laid_off = "None";

UPDATE layoffs_staging2 SET industry = NULL 
	WHERE industry = "" OR industry = "None";

-- companies where industry column is null/none or blank
SELECT * FROM layoffs_staging2 WHERE industry IS NULL OR industry = "";

-- Need to populate industry column if availabe to include as an input
-- so will use join
SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = "") 
AND (t2.industry IS NOT NULL OR t2.industry != "");

UPDATE layoffs_staging2 t1 
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = "") 
AND (t2.industry IS NOT NULL OR t2.industry != "");

-- Lastly, convert "None" string to null values 
UPDATE layoffs_staging2 SET total_laid_off = NULL WHERE total_laid_off = "None";
UPDATE layoffs_staging2 SET percentage_laid_off = NULL WHERE percentage_laid_off = "None";
UPDATE layoffs_staging2 SET funds_raised_millions = NULL WHERE funds_raised_millions = "None";

-- Deleting records where bth total & percentage laid off is null
SELECT COUNT(*) FROM layoffs_staging2 
	WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
DELETE FROM layoffs_staging2 
	WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Dropping row_num column since we do not need it anymore
ALTER TABLE layoffs_staging2 DROP COLUMN row_num;

-- Data is cleaned: 
SELECT * FROM layoffs_staging2;

-- DATA EXPLORATION --

-- companies that are completely closed
SELECT * FROM layoffs_staging2 WHERE percentage_laid_off = 1
	ORDER BY total_laid_off DESC; 

-- total laid off sums by company
SELECT company, SUM(total_laid_off) FROM layoffs_staging2 
	GROUP BY company
    ORDER BY 2 DESC;
    
-- total laid off sums by industry
SELECT industry, SUM(total_laid_off) FROM layoffs_staging2 
	GROUP BY industry
    ORDER BY 2 DESC;

-- total laid off sums by country
SELECT country, SUM(total_laid_off) FROM layoffs_staging2 
	GROUP BY country
    ORDER BY 2 DESC;

-- min & max dates of layoffs
SELECT MIN(`date`), MAX(`date`) FROM layoffs_staging2;
	-- from '2020-03-11' to '2023-03-06'

-- total laid off sums by year
SELECT YEAR(`date`), SUM(total_laid_off) FROM layoffs_staging2 
	GROUP BY YEAR(`date`)
    ORDER BY 1 DESC;
		-- 125677 people laid off in 2023, '2022', 160661 people in 2022 and 500 have "null" value 

-- total laid off sums by the stage the company is in
SELECT stage, SUM(total_laid_off) FROM layoffs_staging2 
	GROUP BY stage
    ORDER BY 2 DESC;
    -- most is Post-IPO, the next being unknown, acquired and so on

-- percentage laid off by company
SELECT company, AVG(percentage_laid_off) FROM layoffs_staging2 
	GROUP BY company
    ORDER BY 2 DESC;

-- total lay offs by month & year
SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off) FROM layoffs_staging2
	WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
	GROUP BY `Month`
    ORDER BY 1 ASC;

-- rolling total with total layoffs & months
WITH Rolling_Total AS
	(SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off) AS total_off FROM layoffs_staging2
		WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
		GROUP BY `Month`
		ORDER BY 1 ASC)
	SELECT `Month`,  total_off,
		SUM(total_off) OVER(ORDER BY `Month`) AS rolling_total 
		FROM Rolling_Total;

-- total layoffs by companies by year 
SELECT company, YEAR(`date`), SUM(total_laid_off) FROM layoffs_staging2
	GROUP BY company, YEAR(`date`)
	ORDER BY 3 DESC;

-- companies ranked by total layoffs by years
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off) FROM layoffs_staging2
	GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS `Rank` FROM Company_Year
	WHERE years IS NOT NULL
    )
SELECT * FROM Company_Year_Rank WHERE `Rank` <= 5;









