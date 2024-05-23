-- SQL Project - Data Cleaning

SELECT * 
FROM world_layoffs.layoffs;

-- Création d'un staging table;les modifications seront appliquées sur un staging table ; 
-- Table original avec les données brutes reste inchangé. 

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

USE world_layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


-- Etapes de data cleaning :
-- 1.Vérifier les doublons et les supprimer
-- 2.Normaliser les données et corriger les erreurs
-- 3.Examiner les valeurs nulles 
-- 4.Supprimer les colonnes et les lignes non nécessaires

-- 1.Vérifier les doublons et les supprimer

# Verifier les doublons

SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;



SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- verifier pour 'Oda'
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;
-- Il semble que toutes ces entrées sont légitimes et ne devraient pas être supprimées.
-- Nous devons vraiment examiner chaque ligne pour être précis

-- les vrais doublons
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Supprimer les lignes où le numéro de ligne est supérieur à 1 ou 2 ou plus

WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- Une solution, est de créer une nouvelle colonne et d'ajouter les numéros de ligne dans celle-ci.
-- Ensuite, supprimer les lignes où les numéros de ligne sont supérieurs à 2, puis supprimer cette colonne;


ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;


SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- nous pouvons supprimer les lignes où le "row_num" est supérieur à 2
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- 2.Normaliser les données et corriger les erreurs

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- Industry contient des nulls et des valeur null
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- airbnb c'est travel, mais ce n'est pas mentione
-- meme chose pour les autre valeurs
-- alors on ecris des query pour remplacer les null par l'industry correct


-- remplacer les valeurs vide avec des null

UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- si possible on remplace les null par l'indsutry correspendante

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Bally c'est la seul qui n'a pas une valeur mentione 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- remplacer tous les valeur qui on Crypto en une valeur
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');


SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------


SELECT *
FROM world_layoffs.layoffs_staging2;

-- standariser les deux valeurs  "United States" et "United States."

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- c'est ok
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


-- regler la date:
SELECT *
FROM world_layoffs.layoffs_staging2;

-- utiliser str
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- changer le type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2;


-- 3.Examiner les valeurs nulles



-- Les valeurs nulles dans total_laid_off, percentage_laid_off et funds_raised_millions semblent normales. Je ne pense pas que je veux les changer.
-- J'aime les laisser nulles car cela facilite les calculs pendant la phase EDA.

-- Donc, il n'y a rien que je veux changer avec les valeurs nulles.

-- 4.Supprimer les colonnes et les lignes non nécessaires

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- supprimer les data inutiles
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;


































