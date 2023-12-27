Create table CovidDeaths (
	iso_code varchar,
	continent varchar,
	location varchar,
	date timestamp,
	population integer,
	total_cases integer,
	new_cases integer,
	new_cases_smoothed double precision,
	total_deaths integer,
	new_deaths integer,
	new_deaths_smoothed double precision,
	total_cases_per_million double precision,
	new_cases_per_million double precision,
	new_cases_smoothed_per_million double precision,
	total_deaths_per_million double precision,
	new_deaths_per_million double precision,
	new_deaths_smoothed_per_million double precision,
	reproduction_rate double precision,
	icu_patients integer,
	icu_patients_per_million double precision,
	hosp_patients integer,
	hosp_patients_per_million double precision,
	weekly_icu_admissions integer,
	weekly_icu_admissions_per_million double precision,
	weekly_hosp_admission integer,
	weekly_hosp_admissions_per_million double precision,
	total_tests integer
);

select * from coviddeaths order by 3,4;

copy coviddeaths from '/Applications/PostgreSQL 16/datasets/Covid_Project/CovidDeaths.csv' csv header;

alter table coviddeaths rename to covid_deaths;


create table covid_vaccinations (
	iso_code varchar,
	continent varchar,
	location varchar,
	date date,
	total_tests bigint,
	new_tests integer,
	total_test_per_thousand double precision,
	new_tests_per_thousand double precision,
	new_tests_smoothed double precision,
	new_tests_smoothed_per_thousand double precision,
	positive_rate double precision,
	tests_per_case double precision,
	tests_units varchar,
	total_vaccinations integer,
	people_vaccinated integer,
	people_fully_vaccinated integer,
	total_boosters integer,
	new_vaccinations integer,
	new_vaccinations_smoothed double precision,
	total_vaccinations_per_hundred double precision,
	people_vaccinated_per_hundred double precision,
	people_fully_vaccinated_per_hundred double precision,
	total_boosters_per_hundred double precision,
	new_vaccinations_smoothed_per_million double precision,
	new_people_vaccinated_smoothed double precision,
	new_people_vaccinated_smoothed_per_hundred double precision,
	stringency_index double precision,
	population_density double precision,
	median_age double precision,
	aged_65_older double precision,
	aged_70_older double precision,
	gdp_per_capita double precision,
	extreme_poverty double precision,
	cardiovasc_death_rate double precision,
	diabetes_prevalence double precision,
	female_smokers double precision,
	male_smokers double precision,
	handwashing_facilities double precision,
	hospital_beds_per_thousand double precision,
	life_expectancy double precision,
	human_development_index double precision,
	excess_mortality_cumulative_absolute double precision,
	excess_mortality_cumulative double precision,
	excess_mortality double precision,
	excess_mortality_cumulative_per_million double precision
);

select * from covid_vaccinations;
alter table covid_vaccinations alter column total_vaccinations type bigint;
alter table covid_vaccinations alter column people_vaccinated type bigint;
alter table covid_vaccinations alter column people_fully_vaccinated type bigint;
alter table covid_vaccinations alter column total_boosters type bigint;
copy covid_vaccinations from '/Applications/PostgreSQL 16/datasets/Covid_Project/CovidVaccinations.csv' csv header;


select location, date, total_cases, new_cases, total_deaths, population from covid_deaths order by 1,2;


-- Looking at Total Cases vs Total Deaths
-- Shows probablilty of dying if you contract covid in your country

select
	location,
	date,
	total_cases,
	total_deaths,
	(cast(total_deaths as decimal) / total_cases)*100 as death_percentage
from covid_deaths
where location = 'United Kingdom'
order by 1,2;


-- Looking at Total Cases vs Population
-- Shows percentage of population that contracted COVID

select
	location,
	date,
	population,
	total_cases,
	(cast(total_cases as decimal) / population) * 100 as percentage_pop_with_covid
from covid_deaths
where location = 'United Kingdom'
order by 1,2;


-- Looking at Countries with Highest Infection Rate compared to Population

select
	location,
	population,
	MAX(total_cases) as highest_infection_count,
	MAX(cast(total_cases as decimal) / population) * 100 as percentage_pop_with_covid
from covid_deaths
group by location, population
order by Percentage_pop_with_covid desc;


-- Looking at Countries with Highest Death Count per Population

select location, MAX(total_deaths) as total_death_count
from covid_deaths
where continent is not null and total_deaths is not null
group by location
order by Total_Death_Count desc;


-- Looking at the Continents with the highest death count per population

select continent, MAX(total_deaths) as total_death_count
from covid_deaths
where continent is not null and total_deaths is not null
group by continent
order by Total_Death_Count desc;


-- Looking at Global Numbers
-- Shows the Death Percentage by date from start of data set

select
	date,
	SUM(new_cases) as total_cases,
	SUM(new_deaths) as total_deaths,
	(SUM(cast(new_deaths as decimal)) / NULLIF(SUM(new_cases), 0)) * 100 as death_percentage
from covid_deaths
where continent is not null
group by date
order by 1,2;


-- Shows the overall Death Percentage across entire time period of data set

select
	SUM(new_cases) as total_cases,
	SUM(new_deaths) as total_deaths,
	(SUM(cast(new_deaths as decimal)) / NULLIF(SUM(new_cases), 0)) * 100 as death_percentage
from covid_deaths
where continent is not null
order by 1,2;


-- Looking at Total Population vs Vaccinations in the World

select
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	SUM(vac.new_vaccinations) over (Partition by dea.location order by dea.location, dea.date) as rolling_people_vaccinated
from covid_deaths as dea
join covid_vaccinations as vac
on dea.location = vac.location and dea.date = vac.date
where dea.continent is not null
order by 2,3;


-- Using CTE to perform calculations on partitionn by in previous query

With PopvsVac (continent, location, date, population, new_vaccinations, rolling_people_vaccinated)
as
(
	select
		dea.continent,
		dea.location,
		dea.date,
		dea.population,
		vac.new_vaccinations,
		SUM(vac.new_vaccinations) over(partition by dea.location order by dea.location, dea.date) as rolling_people_vaccinated
	from covid_deaths as dea
	join covid_vaccinations as vac
	on dea.location = vac.location and dea.date = vac.date
	where dea.continent is not null
)
select *, (rolling_people_vaccinated / population) * 100 as rolling_percentage_pop_vaccinated
from PopvsVac;


-- Using a Temp Table to perform calculations on partition by in previous query

Drop table if exists percent_population_vaccinated;
create table percent_population_vaccinated (
	continent varchar(255),
	location varchar(255),
	date timestamp,
	population numeric,
	new_vaccinations numeric,
	rolling_people_vaccinated numeric
);

insert into percent_population_vaccinated
select
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	SUM(vac.new_vaccinations) over (Partition by dea.location order by dea.location, dea.date) as rolling_people_vaccinated
from covid_deaths as dea
join covid_vaccinations as vac
on dea.date = vac.date and dea.location = vac.location
where dea.continent is not null;

select *, (rolling_people_vaccinated / population) * 100 as rolling_percentage_pop_vaccinated
from percent_population_vaccinated;


-- Creating a View to store data

create view view_percent_population_vaccinated as
select
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	SUM(vac.new_vaccinations) over (partition by dea.location order by dea.location, dea.date) as rolling_people_vaccinated
from covid_deaths as dea
join covid_vaccinations as vac
on dea.location = vac.location and dea.date = vac.date
where dea.continent is not null;


